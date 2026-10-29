/**
 * MeshCentral Multi-Device File Deployer Plugin
 * Author: sokol549
 * License: Apache-2.0
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

module.exports = function (parent) {
    var obj = {};
    obj.parent = parent;
    obj.meshServer = parent.meshServer || parent.parent;

    // Storage paths
    var dataPath = obj.meshServer.datapath || path.join(__dirname, '../../meshcentral-data');
    var pluginStorageDir = path.join(dataPath, 'multideploy');
    var filesStorageDir = path.join(pluginStorageDir, 'files');
    var historyFile = path.join(pluginStorageDir, 'history.json');

    // In-memory state
    var activeJobs = {};
    var jobHistory = [];
    var routeRegistrationAttempts = 0;

    // Ensure storage directories exist
    try {
        if (!fs.existsSync(pluginStorageDir)) fs.mkdirSync(pluginStorageDir, { recursive: true });
        if (!fs.existsSync(filesStorageDir)) fs.mkdirSync(filesStorageDir, { recursive: true });
        if (fs.existsSync(historyFile)) {
            jobHistory = JSON.parse(fs.readFileSync(historyFile, 'utf8'));
        }
    } catch (e) {
        console.log('[MultiDeploy] Init error:', e.message);
    }

    function saveHistory() {
        try {
            var toSave = jobHistory.slice(-100);
            fs.writeFileSync(historyFile, JSON.stringify(toSave, null, 2), 'utf8');
        } catch (e) {
            console.log('[MultiDeploy] Failed to save history:', e.message);
        }
    }

    // Server-side startup hook
    obj.server_startup = function () {
        console.log('[MultiDeploy] Multi-Device File Deployer Plugin loaded.');
        registerRoutesWhenReady(30);
        // MeshCentral 1.3.x reloads the server module but otherwise keeps the
        // browser plugin bundle cached until a full server restart. Refresh it
        // here so a plugin-only reload also delivers the current client code.
        if (obj.meshServer && typeof obj.meshServer.updateMeshCore === 'function') {
            obj.meshServer.updateMeshCore();
        }
    };

    // Express HTTP hook
    obj.hook_setupHttpHandlers = function (webserver) {
        // MeshCentral passes its WebServer object to this hook, not the Express app.
        registerRoutes(webserver && (webserver.app || webserver));
    };

    function getExpressApp() {
        if (!obj.meshServer) return null;
        return obj.meshServer.app || (obj.meshServer.webserver && obj.meshServer.webserver.app) || null;
    }

    function registerRoutesWhenReady(attemptsLeft) {
        try {
            if (registerRoutes(getExpressApp())) return;
        } catch (e) {
            console.log('[MultiDeploy] HTTP route registration failed:', e.message);
        }

        if (attemptsLeft > 0) {
            setTimeout(function () { registerRoutesWhenReady(attemptsLeft - 1); }, 1000);
        }
    }

    // Some MeshCentral releases load plugins before firing server hooks. Start an
    // independent registration check so the API does not depend on hook timing.
    function ensureRoutesRegistered() {
        try {
            if (registerRoutes(getExpressApp())) return;
        } catch (e) {
            console.log('[MultiDeploy] HTTP route registration failed:', e.message);
        }

        routeRegistrationAttempts++;
        if (routeRegistrationAttempts < 60) {
            setTimeout(ensureRoutesRegistered, 1000);
        } else {
            console.log('[MultiDeploy] HTTP routes were not registered after 60 seconds.');
        }
    }

    setTimeout(ensureRoutesRegistered, 1000);

    // Watchdog timer to catch timeouts (runs every 10 seconds)
    setInterval(function () {
        var now = Date.now();
        var updated = false;
        for (var jobId in activeJobs) {
            var job = activeJobs[jobId];
            if (job.status !== 'in_progress') continue;

            var allDone = true;
            for (var nodeId in job.nodes) {
                var node = job.nodes[nodeId];
                if (node.status === 'queued' || node.status === 'downloading') {
                    if (now - (node.lastActivity || job.startTime) > 10 * 60 * 1000) {
                        node.status = 'failed';
                        node.error = 'Превышено время ожидания (таймаут 10 минут)';
                        updated = true;
                    } else {
                        allDone = false;
                    }
                }
            }

            if (allDone) {
                finalizeJob(job);
                updated = true;
            }
        }
        if (updated) saveHistory();
    }, 10000);

    function finalizeJob(job) {
        job.endTime = Date.now();
        var successCount = 0;
        var failCount = 0;
        for (var n in job.nodes) {
            if (job.nodes[n].status === 'completed') successCount++;
            else failCount++;
        }
        job.successCount = successCount;
        job.failCount = failCount;
        job.status = failCount === 0 ? 'completed' : (successCount === 0 ? 'failed' : 'partial');

        jobHistory.unshift(JSON.parse(JSON.stringify(job)));
        saveHistory();
    }

    // Helper: get agent connection
    function getAgent(nodeId) {
        var wsagents = (obj.meshServer.webserver && obj.meshServer.webserver.wsagents) 
            ? obj.meshServer.webserver.wsagents 
            : obj.meshServer.wsagents;
        return wsagents ? wsagents[nodeId] : null;
    }

    // Helper: get base server URL
    function getServerUrl(req) {
        if (obj.meshServer.certificates && obj.meshServer.certificates.CommonName) {
            var proto = 'https://';
            var host = obj.meshServer.certificates.CommonName;
            var port = obj.meshServer.port || 443;
            if (port === 443) return proto + host;
            return proto + host + ':' + port;
        }
        if (req) {
            var host = req.headers.host;
            var proto = (req.connection && req.connection.encrypted) ? 'https://' : 'http://';
            return proto + host;
        }
        return 'http://127.0.0.1';
    }

    // Register all API & transfer routes
    function registerRoutes(app) {
        if (!app || typeof app.get !== 'function' || typeof app.post !== 'function') {
            return;
        }
        if (app._multideploy_registered) return true;
        app._multideploy_registered = true;
        console.log('[MultiDeploy] HTTP API routes registered.');

        // 1. Get nodes & meshes list from server database
        app.get('/plugin/multideploy/api/nodes', function (req, res) {
            try {
                var wsagents = (obj.meshServer.webserver && obj.meshServer.webserver.wsagents)
                    ? obj.meshServer.webserver.wsagents
                    : obj.meshServer.wsagents;

                function sendResult(rawNodes, rawMeshes) {
                    var nodes = (rawNodes || []).map(function (doc) {
                        var isOnline = !!(wsagents && wsagents[doc._id]);
                        return {
                            _id: doc._id,
                            name: doc.name || doc._id,
                            meshid: doc.meshid,
                            os: doc.os || 'Unknown',
                            osdesc: doc.osdesc || '',
                            ip: doc.ip || (wsagents && wsagents[doc._id] ? wsagents[doc._id].remoteAddress : ''),
                            online: isOnline
                        };
                    });
                    var meshes = (rawMeshes || []).map(function (doc) {
                        return {
                            _id: doc._id,
                            name: doc.name || doc._id,
                            desc: doc.desc || ''
                        };
                    });

                    // Also supplement from active wsagents if node not in list
                    if (wsagents) {
                        var nodeMap = {};
                        nodes.forEach(function (n) { nodeMap[n._id] = true; });
                        for (var nid in wsagents) {
                            if (!nodeMap[nid]) {
                                var ag = wsagents[nid];
                                nodes.push({
                                    _id: nid,
                                    name: (ag.node && ag.node.name) || nid,
                                    meshid: (ag.node && ag.node.meshid) || '',
                                    os: (ag.node && ag.node.os) || 'Unknown',
                                    osdesc: '',
                                    ip: ag.remoteAddress || '',
                                    online: true
                                });
                            }
                        }
                    }

                    res.json({ meshes: meshes, nodes: nodes });
                }

                if (obj.meshServer.db && typeof obj.meshServer.db.GetAllType === 'function') {
                    obj.meshServer.db.GetAllType('node', function (nodesError, rawNodes) {
                        obj.meshServer.db.GetAllType('mesh', function (meshesError, rawMeshes) {
                            if (nodesError || meshesError) {
                                console.log('[MultiDeploy] /api/nodes database error:', nodesError || meshesError);
                            }
                            sendResult(nodesError ? [] : rawNodes, meshesError ? [] : rawMeshes);
                        });
                    });
                } else {
                    console.log('[MultiDeploy] /api/nodes: MeshCentral database API GetAllType is unavailable.');
                    sendResult([], []);
                }
            } catch (e) {
                console.log('[MultiDeploy] /api/nodes error:', e.message);
                res.json({ meshes: [], nodes: [] });
            }
        });

        // 2. Upload file to server
        app.post('/plugin/multideploy/api/upload', function (req, res) {
            var body = '';
            req.on('data', function (chunk) { body += chunk; });
            req.on('end', function () {
                try {
                    var data = JSON.parse(body);
                    var fileId = data.fileId || ('file_' + Date.now() + '_' + crypto.randomBytes(4).toString('hex'));
                    var filePath = path.join(filesStorageDir, fileId);

                    var buffer = Buffer.from(data.chunkData, 'base64');
                    if (data.chunkIndex === 0) {
                        fs.writeFileSync(filePath, buffer);
                    } else {
                        fs.appendFileSync(filePath, buffer);
                    }

                    var isLast = (data.chunkIndex + 1 >= data.totalChunks);
                    var sha256 = '';
                    var totalSize = 0;

                    if (isLast) {
                        var fileBytes = fs.readFileSync(filePath);
                        totalSize = fileBytes.length;
                        sha256 = crypto.createHash('sha256').update(fileBytes).digest('hex');
                    }

                    res.json({
                        ok: true,
                        fileId: fileId,
                        chunkIndex: data.chunkIndex,
                        totalChunks: data.totalChunks,
                        completed: isLast,
                        size: totalSize,
                        sha256: sha256
                    });
                } catch (e) {
                    res.status(500).json({ error: 'Upload failed: ' + e.message });
                }
            });
        });

        // 3. Start a deployment job
        app.post('/plugin/multideploy/api/deploy', function (req, res) {
            var body = '';
            req.on('data', function (chunk) { body += chunk; });
            req.on('end', function () {
                try {
                    var params = JSON.parse(body);
                    var fileId = params.fileId;
                    var fileName = params.fileName;
                    var filePath = path.join(filesStorageDir, fileId);

                    if (!fs.existsSync(filePath)) {
                        return res.status(400).json({ error: 'Файл не найден на сервере' });
                    }

                    var fileStat = fs.statSync(filePath);
                    var fileSize = fileStat.size;
                    var targetNodeIds = params.targets || [];

                    if (targetNodeIds.length === 0) {
                        return res.status(400).json({ error: 'Не выбраны целевые компьютеры' });
                    }

                    var jobId = 'job_' + Date.now() + '_' + crypto.randomBytes(4).toString('hex');
                    var serverUrl = getServerUrl(req);

                    var job = {
                        jobId: jobId,
                        fileId: fileId,
                        fileName: fileName,
                        fileSize: fileSize,
                        destPath: params.destPath || '',
                        runAfter: !!params.runAfter,
                        runArgs: params.runArgs || '',
                        overwrite: params.overwrite !== false,
                        startTime: Date.now(),
                        endTime: null,
                        status: 'in_progress',
                        totalCount: targetNodeIds.length,
                        successCount: 0,
                        failCount: 0,
                        nodes: {}
                    };

                    targetNodeIds.forEach(function (nodeId) {
                        var nodeToken = crypto.randomBytes(16).toString('hex');
                        job.nodes[nodeId] = {
                            nodeId: nodeId,
                            name: params.nodeNames ? (params.nodeNames[nodeId] || nodeId) : nodeId,
                            os: params.nodeOS ? (params.nodeOS[nodeId] || 'Unknown') : 'Unknown',
                            token: nodeToken,
                            status: 'queued',
                            progress: 0,
                            bytesTransferred: 0,
                            speed: 0,
                            exitCode: null,
                            error: null,
                            lastActivity: Date.now()
                        };
                    });

                    activeJobs[jobId] = job;

                    targetNodeIds.forEach(function (nodeId) {
                        dispatchNodeDeploy(job, nodeId, serverUrl);
                    });

                    res.json({ ok: true, jobId: jobId, total: targetNodeIds.length });
                } catch (e) {
                    res.status(500).json({ error: 'Deploy error: ' + e.message });
                }
            });
        });

        // 4. Get active job status
        app.get('/plugin/multideploy/api/job/:jobId', function (req, res) {
            var jobId = req.params.jobId;
            var job = activeJobs[jobId];
            if (!job) {
                for (var i = 0; i < jobHistory.length; i++) {
                    if (jobHistory[i].jobId === jobId) {
                        return res.json(jobHistory[i]);
                    }
                }
                return res.status(404).json({ error: 'Задание не найдено' });
            }
            res.json(job);
        });

        // 5. Get list of all jobs
        app.get('/plugin/multideploy/api/jobs', function (req, res) {
            var list = [];
            for (var j in activeJobs) {
                list.push(activeJobs[j]);
            }
            jobHistory.forEach(function (h) {
                if (!activeJobs[h.jobId]) list.push(h);
            });
            list.sort(function (a, b) { return b.startTime - a.startTime; });
            res.json(list.slice(0, 50));
        });

        // 6. Cancel a job
        app.post('/plugin/multideploy/api/cancel/:jobId', function (req, res) {
            var jobId = req.params.jobId;
            var job = activeJobs[jobId];
            if (!job) return res.status(404).json({ error: 'Задание не найдено' });

            for (var n in job.nodes) {
                if (job.nodes[n].status === 'queued' || job.nodes[n].status === 'downloading') {
                    job.nodes[n].status = 'failed';
                    job.nodes[n].error = 'Отменено пользователем';
                }
            }
            finalizeJob(job);
            res.json({ ok: true });
        });

        // 7. Retry failed nodes
        app.post('/plugin/multideploy/api/retry/:jobId', function (req, res) {
            var jobId = req.params.jobId;
            var job = activeJobs[jobId];
            if (!job) {
                for (var i = 0; i < jobHistory.length; i++) {
                    if (jobHistory[i].jobId === jobId) {
                        job = JSON.parse(JSON.stringify(jobHistory[i]));
                        activeJobs[jobId] = job;
                        break;
                    }
                }
            }
            if (!job) return res.status(404).json({ error: 'Задание не найдено' });

            var serverUrl = getServerUrl(req);
            job.status = 'in_progress';
            var retriedCount = 0;

            for (var nodeId in job.nodes) {
                var node = job.nodes[nodeId];
                if (node.status === 'failed') {
                    node.status = 'queued';
                    node.progress = 0;
                    node.bytesTransferred = 0;
                    node.error = null;
                    node.exitCode = null;
                    node.lastActivity = Date.now();
                    node.token = crypto.randomBytes(16).toString('hex');
                    dispatchNodeDeploy(job, nodeId, serverUrl);
                    retriedCount++;
                }
            }

            res.json({ ok: true, retriedCount: retriedCount });
        });

        // 8. Agent download endpoint
        app.get('/plugin/multideploy/dl', function (req, res) {
            var jobId = req.query.job;
            var nodeId = req.query.node;
            var token = req.query.token;

            var job = activeJobs[jobId];
            if (!job) return res.status(404).send('Job not found');

            var node = job.nodes[nodeId];
            if (!node || node.token !== token) return res.status(403).send('Invalid token');

            var filePath = path.join(filesStorageDir, job.fileId);
            if (!fs.existsSync(filePath)) return res.status(404).send('File missing');

            var stat = fs.statSync(filePath);
            var totalSize = stat.size;

            node.status = 'downloading';
            node.lastActivity = Date.now();

            res.writeHead(200, {
                'Content-Type': 'application/octet-stream',
                'Content-Length': totalSize,
                'Content-Disposition': 'attachment; filename="' + encodeURIComponent(job.fileName) + '"'
            });

            var readStream = fs.createReadStream(filePath, { highWaterMark: 64 * 1024 });
            var transferred = 0;
            var lastTime = Date.now();
            var lastTransferred = 0;

            readStream.on('data', function (chunk) {
                transferred += chunk.length;
                node.bytesTransferred = transferred;
                node.progress = totalSize > 0 ? Math.round((transferred / totalSize) * 100) : 100;
                node.lastActivity = Date.now();

                var now = Date.now();
                var diff = (now - lastTime) / 1000;
                if (diff >= 0.5) {
                    node.speed = Math.round((transferred - lastTransferred) / diff);
                    lastTime = now;
                    lastTransferred = transferred;
                }
            });

            readStream.on('end', function () {
                node.bytesTransferred = totalSize;
                node.progress = 100;
                node.lastActivity = Date.now();
            });

            readStream.on('error', function (err) {
                node.status = 'failed';
                node.error = 'Сбой передачи потока: ' + err.message;
            });

            readStream.pipe(res);
        });

        // 9. Agent completion report endpoint
        app.post('/plugin/multideploy/report', function (req, res) {
            var body = '';
            req.on('data', function (c) { body += c; });
            req.on('end', function () {
                try {
                    var report = JSON.parse(body);
                    var job = activeJobs[report.job];
                    if (job && job.nodes && job.nodes[report.node]) {
                        var node = job.nodes[report.node];
                        node.status = report.status;
                        node.exitCode = report.exitCode !== undefined ? report.exitCode : null;
                        node.error = report.error || null;
                        node.lastActivity = Date.now();
                        if (report.status === 'completed') {
                            node.progress = 100;
                        }

                        var allDone = true;
                        for (var n in job.nodes) {
                            if (job.nodes[n].status === 'queued' || job.nodes[n].status === 'downloading') {
                                allDone = false;
                                break;
                            }
                        }
                        if (allDone) {
                            finalizeJob(job);
                        }
                    }
                    res.json({ ok: true });
                } catch (e) {
                    res.status(500).json({ error: e.message });
                }
            });
        });
        return true;
    }

    // Dispatch deploy command to a single agent
    function dispatchNodeDeploy(job, nodeId, serverUrl) {
        var nodeEntry = job.nodes[nodeId];
        var agent = getAgent(nodeId);

        if (!agent) {
            nodeEntry.status = 'failed';
            nodeEntry.error = 'Компьютер не в сети (агент отключен)';
            checkJobDone(job);
            return;
        }

        var isWindows = (nodeEntry.os && nodeEntry.os.toLowerCase().indexOf('win') !== -1);
        var destPath = job.destPath;
        var fileName = job.fileName;

        if (!destPath || destPath.trim() === '') {
            destPath = isWindows ? ('C:\\Temp\\' + fileName) : ('/tmp/' + fileName);
        } else {
            if (destPath.endsWith('\\') || destPath.endsWith('/')) {
                destPath += fileName;
            }
        }

        var downloadUrl = serverUrl + '/plugin/multideploy/dl?job=' + job.jobId + '&node=' + nodeId + '&token=' + nodeEntry.token;
        var reportUrl = serverUrl + '/plugin/multideploy/report';
        var runAfter = job.runAfter ? '1' : '0';
        var runArgs = (job.runArgs || '').replace(/'/g, "''");

        var cmdString = '';

        if (isWindows) {
            var psScript = [
                "$ErrorActionPreference = 'Stop';",
                "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13;",
                "[System.Net.ServicePointManager]::ServerCertificateValidationCallback = {$true};",
                "$dest = '" + destPath.replace(/'/g, "''") + "';",
                "$dir = Split-Path $dest;",
                "if ($dir -and !(Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null };",
                "try {",
                "  $wc = New-Object System.Net.WebClient;",
                "  $wc.DownloadFile('" + downloadUrl + "', $dest);",
                "  $ec = 0;",
                "  if ('" + runAfter + "' -eq '1') {",
                "    $p = Start-Process -FilePath $dest -ArgumentList '" + runArgs + "' -Wait -PassThru -WindowStyle Hidden;",
                "    $ec = $p.ExitCode;",
                "  };",
                "  $payload = @{ job = '" + job.jobId + "'; node = '" + nodeId + "'; status = 'completed'; exitCode = $ec; bytes = (Get-Item $dest).Length } | ConvertTo-Json;",
                "  Invoke-RestMethod -Uri '" + reportUrl + "' -Method POST -Body $payload -ContentType 'application/json' | Out-Null;",
                "} catch {",
                "  $err = $_.Exception.Message;",
                "  $payload = @{ job = '" + job.jobId + "'; node = '" + nodeId + "'; status = 'failed'; error = $err } | ConvertTo-Json;",
                "  try { Invoke-RestMethod -Uri '" + reportUrl + "' -Method POST -Body $payload -ContentType 'application/json' | Out-Null; } catch {}",
                "}"
            ].join(' ');

            cmdString = 'powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "' + psScript.replace(/"/g, '\\"') + '"';
        } else {
            var safeDest = destPath.replace(/"/g, '\\"');
            cmdString = [
                'dest="' + safeDest + '";',
                'mkdir -p "$(dirname "$dest")" 2>/dev/null;',
                'if (curl -k -fsSL "' + downloadUrl + '" -o "$dest" || wget --no-check-certificate -q "' + downloadUrl + '" -O "$dest"); then',
                '  ec=0;',
                '  if [ "' + runAfter + '" = "1" ]; then',
                '    chmod +x "$dest" 2>/dev/null;',
                '    "$dest" ' + runArgs + '; ec=$?;',
                '  fi;',
                '  curl -k -s -X POST -H "Content-Type: application/json" -d \'{"job":"' + job.jobId + '","node":"' + nodeId + '","status":"completed","exitCode":\'$ec\'}\' "' + reportUrl + '" >/dev/null 2>&1;',
                'else',
                '  curl -k -s -X POST -H "Content-Type: application/json" -d \'{"job":"' + job.jobId + '","node":"' + nodeId + '","status":"failed","error":"Загрузка не удалась"}\' "' + reportUrl + '" >/dev/null 2>&1;',
                'fi'
            ].join(' ');
        }

        try {
            var msg = JSON.stringify({
                action: 'runcommands',
                cmds: cmdString,
                runAsUser: 0
            });
            agent.send(msg);
        } catch (e) {
            nodeEntry.status = 'failed';
            nodeEntry.error = 'Ошибка отправки команды агенту: ' + e.message;
            checkJobDone(job);
        }
    }

    function checkJobDone(job) {
        var allDone = true;
        for (var n in job.nodes) {
            if (job.nodes[n].status === 'queued' || job.nodes[n].status === 'downloading') {
                allDone = false;
                break;
            }
        }
        if (allDone) finalizeJob(job);
    }

    // Admin request handler
    obj.handleAdminReq = function (req, res, user) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end('<h3>MeshCentral Multi-Device File Deployer Plugin is running.</h3>');
    };

    // Exported Web UI Hooks
    obj.exports = [
        'onWebUIStartupEnd',
        'registerPluginTab',
        'onDeviceRefreshEnd'
    ];

    // ==========================================
    // CLIENT-SIDE CODE (Runs inside Web Browser)
    // ==========================================

    obj.onWebUIStartupEnd = function () {
        if (window._multideploy_injected) return;
        window._multideploy_injected = true;

        var css = document.createElement('style');
        css.id = 'multideploy-styles';
        css.innerHTML = `
            #multideploy-btn {
                margin-left: 4px;
            }
            .md-modal-overlay {
                position: fixed;
                top: 0; left: 0; right: 0; bottom: 0;
                background: rgba(0, 0, 0, 0.7);
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 99999;
                backdrop-filter: blur(3px);
            }
            .md-modal {
                background: #1e293b;
                color: #f1f5f9;
                width: 920px;
                max-width: 95vw;
                max-height: 90vh;
                border-radius: 12px;
                display: flex;
                flex-direction: column;
                box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5), 0 8px 10px -6px rgba(0, 0, 0, 0.5);
                border: 1px solid #334155;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                overflow: hidden;
            }
            .md-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 16px 20px;
                background: #0f172a;
                border-bottom: 1px solid #334155;
            }
            .md-header h2 {
                margin: 0;
                font-size: 18px;
                font-weight: 600;
                display: flex;
                align-items: center;
                gap: 8px;
            }
            .md-close {
                background: transparent;
                border: none;
                color: #94a3b8;
                font-size: 20px;
                cursor: pointer;
                padding: 4px 8px;
                border-radius: 4px;
            }
            .md-close:hover { color: #f87171; background: #334155; }
            .md-tabs {
                display: flex;
                background: #0f172a;
                border-bottom: 1px solid #334155;
                padding: 0 16px;
            }
            .md-tab {
                padding: 10px 16px;
                cursor: pointer;
                border-bottom: 2px solid transparent;
                color: #94a3b8;
                font-size: 14px;
                font-weight: 500;
                transition: all 0.2s;
            }
            .md-tab.active {
                color: #38bdf8;
                border-bottom-color: #38bdf8;
                font-weight: 600;
            }
            .md-content {
                padding: 20px;
                overflow-y: auto;
                flex: 1;
            }
            .md-section-title {
                font-size: 14px;
                font-weight: 600;
                color: #94a3b8;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin: 0 0 10px 0;
            }
            .md-card {
                background: #0f172a;
                border: 1px solid #334155;
                border-radius: 8px;
                padding: 14px;
                margin-bottom: 16px;
            }
            .md-filter-row {
                display: flex;
                gap: 10px;
                margin-bottom: 12px;
                align-items: center;
            }
            .md-input, .md-select {
                background: #1e293b;
                border: 1px solid #475569;
                color: #f1f5f9;
                padding: 8px 12px;
                border-radius: 6px;
                font-size: 13px;
                outline: none;
            }
            .md-input:focus, .md-select:focus { border-color: #38bdf8; }
            .md-nodes-table-wrap {
                max-height: 200px;
                overflow-y: auto;
                border: 1px solid #334155;
                border-radius: 6px;
            }
            .md-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
            }
            .md-table th {
                background: #1e293b;
                color: #94a3b8;
                text-align: left;
                padding: 8px 12px;
                font-weight: 600;
                position: sticky;
                top: 0;
                z-index: 10;
            }
            .md-table td {
                padding: 8px 12px;
                border-bottom: 1px solid #1e293b;
            }
            .md-table tr:hover { background: #1e293b; }
            .md-badge {
                display: inline-block;
                padding: 2px 8px;
                border-radius: 12px;
                font-size: 11px;
                font-weight: 600;
            }
            .md-badge-online { background: #065f46; color: #34d399; }
            .md-badge-offline { background: #374151; color: #9ca3af; }
            .md-badge-completed { background: #065f46; color: #34d399; }
            .md-badge-downloading { background: #1e3a8a; color: #60a5fa; }
            .md-badge-failed { background: #7f1d1d; color: #f87171; }
            .md-badge-queued { background: #78350f; color: #fbbf24; }
            .md-dropzone {
                border: 2px dashed #475569;
                border-radius: 8px;
                padding: 20px;
                text-align: center;
                cursor: pointer;
                transition: border-color 0.2s, background 0.2s;
            }
            .md-dropzone:hover, .md-dropzone.dragover {
                border-color: #38bdf8;
                background: rgba(56, 189, 248, 0.05);
            }
            .md-progress-bar-bg {
                background: #334155;
                border-radius: 6px;
                height: 10px;
                overflow: hidden;
                position: relative;
            }
            .md-progress-bar-fill {
                background: linear-gradient(90deg, #38bdf8, #10b981);
                height: 100%;
                width: 0%;
                transition: width 0.3s ease;
            }
            .md-btn {
                background: #2563eb;
                color: #ffffff;
                border: none;
                padding: 8px 16px;
                border-radius: 6px;
                font-size: 13px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.2s;
            }
            .md-btn:hover { background: #1d4ed8; }
            .md-btn-success { background: #10b981; }
            .md-btn-success:hover { background: #059669; }
            .md-btn-danger { background: #ef4444; }
            .md-btn-danger:hover { background: #dc2626; }
            .md-btn-secondary { background: #475569; }
            .md-btn-secondary:hover { background: #334155; }
            .md-footer {
                display: flex;
                justify-content: flex-end;
                gap: 10px;
                padding: 16px 20px;
                background: #0f172a;
                border-top: 1px solid #334155;
            }
        `;
        document.head.appendChild(css);

        function injectDeployButton() {
            if (document.getElementById('multideploy-btn')) return;
            var groupActionButton = document.getElementById('GroupActionButton');
            var targetBar = document.getElementById('devListToolbar')
                || document.getElementById('devicesButtonBar')
                || document.getElementById('navigationHeader')
                || document.querySelector('.page-header');

            // Use an input button so MeshCentral applies its native toolbar styling.
            if (!targetBar) return;

            var btn = document.createElement('input');
            btn.id = 'multideploy-btn';
            btn.type = 'button';
            btn.value = 'Массовая отправка файлов';
            btn.title = 'Отправить файл на несколько компьютеров одновременно';
            btn.onclick = function () {
                window.MultiDeployPlugin.openModal();
            };

            if (groupActionButton && groupActionButton.parentNode === targetBar) {
                targetBar.insertBefore(btn, groupActionButton.nextSibling);
            } else {
                targetBar.appendChild(btn);
            }
        }
        injectDeployButton();
        setInterval(injectDeployButton, 3000);

        window.MultiDeployPlugin = {
            nodes: [],
            meshes: [],
            selectedNodes: {},
            uploadedFile: null,
            activeJobId: null,
            nativeJob: null,
            nativeTransfers: {},
            pollTimer: null,
            currentTab: 'new',
            nodeLoadError: null,

            // Read nodes directly from MeshCentral frontend in-memory state
            getNodesFromBrowser: function () {
                var resultNodes = [];
                var resultMeshes = [];
                var nodeMap = {};

                function addNode(n, fallbackMeshId) {
                    if (!n) return;
                    var nid = n._id || n.nodeid || n.id;
                    if (!nid || nodeMap[nid]) return;
                    nodeMap[nid] = true;

                    var isOnline = false;
                    if (typeof n.conn === 'number') {
                        isOnline = (n.conn & 1) !== 0 || (n.conn & 3) !== 0;
                    } else if (n.connected !== undefined) {
                        isOnline = !!n.connected;
                    } else if (n.online !== undefined) {
                        isOnline = !!n.online;
                    } else if (n.connecttime && !n.disconnecttime) {
                        isOnline = true;
                    }

                    var osStr = n.os || (n.agent && n.agent.os) || '';
                    if (!osStr && n.desc) {
                        var d = n.desc.toLowerCase();
                        if (d.indexOf('win') !== -1) osStr = 'Windows';
                        else if (d.indexOf('linux') !== -1 || d.indexOf('ubuntu') !== -1) osStr = 'Linux';
                    }

                    resultNodes.push({
                        _id: nid,
                        name: n.name || nid,
                        meshid: n.meshid || fallbackMeshId || '',
                        os: osStr || 'Unknown',
                        osdesc: n.osdesc || n.desc || '',
                        ip: n.ip || (n.netif && n.netif[0] ? n.netif[0] : '') || '',
                        online: isOnline
                    });
                }

                // 1. Check window.meshes / window.meshserver.meshes
                var rawMeshes = window.meshes || (window.meshserver && window.meshserver.meshes);
                if (rawMeshes) {
                    if (Array.isArray(rawMeshes)) {
                        rawMeshes.forEach(function (m) {
                            if (m && (m._id || m.id)) {
                                resultMeshes.push({ _id: m._id || m.id, name: m.name || m._id });
                            }
                        });
                    } else if (typeof rawMeshes === 'object') {
                        for (var mid in rawMeshes) {
                            var mObj = rawMeshes[mid];
                            if (mObj && typeof mObj === 'object') {
                                resultMeshes.push({ _id: mObj._id || mid, name: mObj.name || mid });
                            }
                        }
                    }
                }

                // 2. Check window.nodes / window.meshserver.nodes
                var rawNodes = window.nodes || (window.meshserver && window.meshserver.nodes);
                if (rawNodes) {
                    if (Array.isArray(rawNodes)) {
                        rawNodes.forEach(function (n) { addNode(n); });
                    } else if (typeof rawNodes === 'object') {
                        for (var k in rawNodes) {
                            var val = rawNodes[k];
                            if (Array.isArray(val)) {
                                val.forEach(function (sub) { addNode(sub, k); });
                            } else if (val && typeof val === 'object') {
                                if (val._id && String(val._id).indexOf('node//') === 0) {
                                    addNode(val, val.meshid || k);
                                } else {
                                    for (var subk in val) {
                                        var subval = val[subk];
                                        if (subval && typeof subval === 'object') {
                                            addNode(subval, k);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // 3. Fallback: Parse from MeshCentral devices tree DOM if nodes object not exposed
                if (resultNodes.length === 0) {
                    try {
                        var nodeElements = document.querySelectorAll('[id^="d_node//"], [id^="node//"], [onclick*="node//"]');
                        nodeElements.forEach(function (el) {
                            var id = el.id ? el.id.replace(/^d_/, '') : '';
                            if (!id && el.getAttribute('onclick')) {
                                var match = el.getAttribute('onclick').match(/node\/\/[a-zA-Z0-9_\$]+/);
                                if (match) id = match[0];
                            }
                            if (id && !nodeMap[id]) {
                                var name = el.innerText.trim().split('\n')[0] || id;
                                addNode({ _id: id, name: name, conn: 1 });
                            }
                        });
                    } catch (e) {}
                }

                return { nodes: resultNodes, meshes: resultMeshes };
            },

            loadNodes: function (cb) {
                var self = this;
                self.nodeLoadError = null;

                // Step 1: Immediately read from browser's live MeshCentral memory
                var local = self.getNodesFromBrowser();
                if (local.nodes && local.nodes.length > 0) {
                    self.nodes = local.nodes;
                    self.meshes = local.meshes;
                    if (cb) { cb(); cb = null; }
                }

                // Step 2: Query plugin backend API in background to merge any missing nodes
                fetch('/plugin/multideploy/api/nodes')
                    .then(function (r) {
                        if (!r.ok) throw new Error('Сервер вернул HTTP ' + r.status);
                        return r.json();
                    })
                    .then(function (data) {
                        if (data && data.nodes && data.nodes.length > 0) {
                            var map = {};
                            self.nodes.forEach(function (n) { map[n._id] = n; });

                            data.nodes.forEach(function (n) {
                                if (!map[n._id]) {
                                    self.nodes.push(n);
                                } else {
                                    map[n._id].online = n.online;
                                    if (n.ip && !map[n._id].ip) map[n._id].ip = n.ip;
                                    if (n.os && map[n._id].os === 'Unknown') map[n._id].os = n.os;
                                }
                            });

                            if (data.meshes && data.meshes.length > 0) {
                                var mMap = {};
                                self.meshes.forEach(function (m) { mMap[m._id] = true; });
                                data.meshes.forEach(function (m) {
                                    if (!mMap[m._id]) self.meshes.push(m);
                                });
                            }
                        }

                        if (cb) cb();
                        self.filterNodes();
                    })
                    .catch(function (e) {
                        console.warn('[MultiDeploy] /api/nodes fetch:', e);
                        self.nodeLoadError = 'Не удалось загрузить список компьютеров: ' + e.message;
                        if (cb) cb();
                        self.filterNodes();
                    });
            },

            openModal: function () {
                var self = this;
                var existing = document.getElementById('multideploy-modal-overlay');
                if (existing) existing.remove();

                var overlay = document.createElement('div');
                overlay.id = 'multideploy-modal-overlay';
                overlay.className = 'md-modal-overlay';
                overlay.innerHTML = `
                    <div class="md-modal">
                        <div class="md-header">
                            <h2>📤 Отправка файлов на компьютеры (MultiDeploy)</h2>
                            <button class="md-close" onclick="window.MultiDeployPlugin.closeModal()">✕</button>
                        </div>
                        <div class="md-tabs">
                            <div class="md-tab active" id="md-tab-new" onclick="window.MultiDeployPlugin.switchTab('new')">Новая отправка</div>
                            <div class="md-tab" id="md-tab-monitor" onclick="window.MultiDeployPlugin.switchTab('monitor')">Отслеживание (<span id="md-active-counter">0</span>)</div>
                            <div class="md-tab" id="md-tab-history" onclick="window.MultiDeployPlugin.switchTab('history')">История отправок</div>
                        </div>
                        <div class="md-content" id="md-content-body">
                            <div style="text-align: center; padding: 40px; color: #94a3b8;">
                                <div style="font-size: 24px; margin-bottom: 8px;">⏳</div>
                                <div>Загрузка списка компьютеров...</div>
                            </div>
                        </div>
                        <div class="md-footer" id="md-footer-controls"></div>
                    </div>
                `;
                document.body.appendChild(overlay);

                self.loadNodes(function () {
                    self.switchTab('new');
                });
            },

            closeModal: function () {
                var overlay = document.getElementById('multideploy-modal-overlay');
                if (overlay) overlay.remove();
                if (this.pollTimer) clearInterval(this.pollTimer);
            },

            switchTab: function (tab) {
                this.currentTab = tab;
                document.querySelectorAll('.md-tab').forEach(function (t) { t.classList.remove('active'); });
                var activeTabEl = document.getElementById('md-tab-' + tab);
                if (activeTabEl) activeTabEl.classList.add('active');

                if (tab === 'new') this.renderNewTab();
                else if (tab === 'monitor') this.renderMonitorTab();
                else if (tab === 'history') this.renderHistoryTab();
            },

            renderNewTab: function () {
                var self = this;
                var body = document.getElementById('md-content-body');
                var footer = document.getElementById('md-footer-controls');

                var meshOptions = '<option value="all">-- Все группы (Все компьютеры) --</option>';
                self.meshes.forEach(function (m) {
                    meshOptions += '<option value="' + m._id + '">' + m.name + '</option>';
                });

                body.innerHTML = `
                    <div class="md-card">
                        <div class="md-section-title">1. Выбор целевых компьютеров</div>
                        <div class="md-filter-row">
                            <select id="md-mesh-filter" class="md-select" style="flex: 1;" onchange="window.MultiDeployPlugin.filterNodes()">
                                ${meshOptions}
                            </select>
                            <input type="text" id="md-search-filter" class="md-input" placeholder="Поиск по имени или IP..." style="flex: 1;" oninput="window.MultiDeployPlugin.filterNodes()">
                            <button class="md-btn md-btn-secondary" onclick="window.MultiDeployPlugin.selectAllOnline()">Только в сети</button>
                            <button class="md-btn md-btn-secondary" onclick="window.MultiDeployPlugin.toggleSelectAll()">Все</button>
                            <button class="md-btn md-btn-secondary" title="Обновить список компьютеров" onclick="window.MultiDeployPlugin.loadNodes(function(){ window.MultiDeployPlugin.filterNodes(); })">🔄</button>
                        </div>
                        <div class="md-nodes-table-wrap">
                            <table class="md-table">
                                <thead>
                                    <tr>
                                        <th style="width: 30px;"><input type="checkbox" id="md-select-all-cb" onchange="window.MultiDeployPlugin.onMasterCheckbox(this.checked)"></th>
                                        <th>Компьютер</th>
                                        <th>ОС</th>
                                        <th>IP адрес</th>
                                        <th>Статус</th>
                                    </tr>
                                </thead>
                                <tbody id="md-nodes-tbody">
                                    <!-- Rendered by filterNodes() -->
                                </tbody>
                            </table>
                        </div>
                        <div style="margin-top: 8px; font-size: 12px; color: #94a3b8;" id="md-selected-count-msg">
                            Выбрано: 0 компьютеров
                        </div>
                    </div>

                    <div class="md-card">
                        <div class="md-section-title">2. Выбор файла для отправки</div>
                        <div class="md-dropzone" id="md-dropzone" onclick="document.getElementById('md-file-input').click()">
                            <input type="file" id="md-file-input" style="display: none;" onchange="window.MultiDeployPlugin.handleFileSelect(this.files)">
                            <div id="md-file-info">
                                <div style="font-size: 24px; margin-bottom: 6px;">📁</div>
                                <div style="font-weight: 500;">Нажмите или перетащите файл сюда</div>
                                <div style="font-size: 12px; color: #64748b; margin-top: 4px;">Любые файлы: .exe, .msi, .bat, .ps1, .sh, .zip, .cfg и др.</div>
                            </div>
                        </div>
                    </div>

                    <div class="md-card">
                        <div class="md-section-title">3. Параметры назначения и выполнения</div>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px;">
                            <div>
                                <label style="font-size: 12px; color: #94a3b8; display: block; margin-bottom: 4px;">Путь назначения на клиенте:</label>
                                <input type="text" id="md-dest-path" class="md-input" style="width: 95%;" placeholder="По умолчанию: C:\\Temp\\ или /tmp/">
                            </div>
                            <div>
                                <label style="font-size: 12px; color: #94a3b8; display: block; margin-bottom: 4px;">Аргументы запуска (если запуск включен):</label>
                                <input type="text" id="md-run-args" class="md-input" style="width: 95%;" placeholder="Например: /quiet /norestart или --silent">
                            </div>
                        </div>
                        <div style="display: flex; gap: 20px; font-size: 13px;">
                            <label style="cursor: pointer; display: flex; align-items: center; gap: 6px;">
                                <input type="checkbox" id="md-overwrite-cb" checked> Перезаписывать существующий файл
                            </label>
                            <label style="cursor: pointer; display: flex; align-items: center; gap: 6px;">
                                <input type="checkbox" id="md-run-after-cb"> Запустить файл после успешной загрузки
                            </label>
                        </div>
                    </div>
                `;

                footer.innerHTML = `
                    <button class="md-btn md-btn-secondary" onclick="window.MultiDeployPlugin.closeModal()">Отмена</button>
                    <button class="md-btn md-btn-success" id="md-start-btn" onclick="window.MultiDeployPlugin.startDeployment()">
                        🚀 Начать отправку
                    </button>
                `;

                self.setupDragAndDrop();
                self.filterNodes();
            },

            setupDragAndDrop: function () {
                var self = this;
                var dropzone = document.getElementById('md-dropzone');
                if (!dropzone) return;

                ['dragenter', 'dragover'].forEach(function (evt) {
                    dropzone.addEventListener(evt, function (e) {
                        e.preventDefault();
                        e.stopPropagation();
                        dropzone.classList.add('dragover');
                    });
                });

                ['dragleave', 'drop'].forEach(function (evt) {
                    dropzone.addEventListener(evt, function (e) {
                        e.preventDefault();
                        e.stopPropagation();
                        dropzone.classList.remove('dragover');
                    });
                });

                dropzone.addEventListener('drop', function (e) {
                    var dt = e.dataTransfer;
                    if (dt && dt.files && dt.files.length) {
                        self.handleFileSelect(dt.files);
                    }
                });
            },

            handleFileSelect: function (files) {
                var self = this;
                if (!files || !files[0]) return;
                var file = files[0];

                var infoEl = document.getElementById('md-file-info');
                // Keep the file in the browser. It will be sent directly to every
                // selected MeshCentral agent using its built-in file channel.
                self.uploadedFile = {
                    file: file,
                    fileName: file.name,
                    fileSize: file.size
                };
                infoEl.innerHTML = `
                    <div style="font-size: 20px; color: #34d399; margin-bottom: 4px;">✅ Файл готов к отправке</div>
                    <div style="font-weight: 600; font-size: 14px;">${file.name}</div>
                    <div style="font-size: 12px; color: #94a3b8; margin-top: 2px;">Размер: ${self.formatBytes(file.size)}. Передача начнётся после выбора компьютеров.</div>
                `;
                return;

                infoEl.innerHTML = `
                    <div style="font-size: 20px; color: #38bdf8; margin-bottom: 4px;">⏳ Загрузка на сервер MeshCentral...</div>
                    <div style="font-size: 13px;">${file.name} (${self.formatBytes(file.size)})</div>
                    <div class="md-progress-bar-bg" style="margin-top: 10px;">
                        <div class="md-progress-bar-fill" id="md-upload-fill" style="width: 0%;"></div>
                    </div>
                `;

                var chunkSize = 2 * 1024 * 1024;
                var totalChunks = Math.ceil(file.size / chunkSize) || 1;
                var currentChunk = 0;
                var fileId = 'file_' + Date.now();

                function uploadNextChunk() {
                    var start = currentChunk * chunkSize;
                    var end = Math.min(start + chunkSize, file.size);
                    var reader = new FileReader();

                    reader.onload = function (e) {
                        var base64Data = e.target.result.split(',')[1] || '';
                        fetch('/plugin/multideploy/api/upload', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                fileId: fileId,
                                chunkIndex: currentChunk,
                                totalChunks: totalChunks,
                                chunkData: base64Data
                            })
                        })
                        .then(function (r) {
                            if (!r.ok) throw new Error('Сервер вернул HTTP ' + r.status);
                            return r.json();
                        })
                        .then(function (res) {
                            if (!res || !res.ok) throw new Error((res && res.error) || 'Сервер не подтвердил загрузку');
                            currentChunk++;
                            var pct = Math.round((currentChunk / totalChunks) * 100);
                            var fill = document.getElementById('md-upload-fill');
                            if (fill) fill.style.width = pct + '%';

                            if (currentChunk < totalChunks) {
                                uploadNextChunk();
                            } else {
                                self.uploadedFile = {
                                    fileId: fileId,
                                    fileName: file.name,
                                    fileSize: file.size,
                                    sha256: res.sha256
                                };
                                infoEl.innerHTML = `
                                    <div style="font-size: 20px; color: #34d399; margin-bottom: 4px;">✅ Файл готов к отправке</div>
                                    <div style="font-weight: 600; font-size: 14px;">${file.name}</div>
                                    <div style="font-size: 12px; color: #94a3b8; margin-top: 2px;">
                                        Размер: ${self.formatBytes(file.size)} | SHA256: ${res.sha256 ? res.sha256.substring(0, 16) + '...' : ''}
                                    </div>
                                `;
                            }
                        })
                        .catch(function (err) {
                            infoEl.innerHTML = '<div style="color: #f87171;">Ошибка загрузки файла на сервер: ' + err.message + '</div>';
                        });
                    };

                    var slice = file.slice(start, end);
                    reader.readAsDataURL(slice);
                }

                uploadNextChunk();
            },

            filterNodes: function () {
                var self = this;
                var meshFilter = document.getElementById('md-mesh-filter') ? document.getElementById('md-mesh-filter').value : 'all';
                var searchFilter = document.getElementById('md-search-filter') ? document.getElementById('md-search-filter').value.toLowerCase() : '';
                var tbody = document.getElementById('md-nodes-tbody');
                if (!tbody) return;

                var html = '';
                var visibleCount = 0;

                self.nodes.forEach(function (n) {
                    if (meshFilter !== 'all' && n.meshid !== meshFilter) return;
                    if (searchFilter) {
                        var text = (n.name + ' ' + (n.ip || '') + ' ' + (n.os || '')).toLowerCase();
                        if (text.indexOf(searchFilter) === -1) return;
                    }

                    visibleCount++;
                    var checked = self.selectedNodes[n._id] ? 'checked' : '';
                    var statusBadge = n.online 
                        ? '<span class="md-badge md-badge-online">В сети</span>' 
                        : '<span class="md-badge md-badge-offline">Не в сети</span>';

                    var osIcon = '💻';
                    var osLow = (n.os || '').toLowerCase();
                    if (osLow.indexOf('win') !== -1) osIcon = '🪟';
                    else if (osLow.indexOf('linux') !== -1 || osLow.indexOf('ubuntu') !== -1) osIcon = '🐧';
                    else if (osLow.indexOf('mac') !== -1) osIcon = '🍎';

                    html += `
                        <tr>
                            <td><input type="checkbox" ${checked} onchange="window.MultiDeployPlugin.toggleNode('${n._id}', this.checked)"></td>
                            <td><strong>${osIcon} ${n.name}</strong></td>
                            <td style="color: #94a3b8;">${n.osdesc || n.os}</td>
                            <td style="color: #94a3b8; font-family: monospace;">${n.ip || '—'}</td>
                            <td>${statusBadge}</td>
                        </tr>
                    `;
                });

                if (visibleCount === 0) {
                    var emptyMessage = self.nodeLoadError || 'Компьютеры не найдены (нажмите 🔄 для обновления)';
                    html = '<tr><td colspan="5" style="text-align: center; color: #64748b; padding: 20px;">' + emptyMessage + '</td></tr>';
                }

                tbody.innerHTML = html;
                self.updateSelectedCount();
            },

            toggleNode: function (nodeId, isChecked) {
                if (isChecked) this.selectedNodes[nodeId] = true;
                else delete this.selectedNodes[nodeId];
                this.updateSelectedCount();
            },

            onMasterCheckbox: function (checked) {
                var self = this;
                self.nodes.forEach(function (n) {
                    if (checked) self.selectedNodes[n._id] = true;
                    else delete self.selectedNodes[n._id];
                });
                self.filterNodes();
            },

            selectAllOnline: function () {
                var self = this;
                var meshFilter = document.getElementById('md-mesh-filter') ? document.getElementById('md-mesh-filter').value : 'all';
                self.selectedNodes = {};
                self.nodes.forEach(function (n) {
                    if (n.online && (meshFilter === 'all' || n.meshid === meshFilter)) {
                        self.selectedNodes[n._id] = true;
                    }
                });
                self.filterNodes();
            },

            toggleSelectAll: function () {
                var self = this;
                var allSelected = (Object.keys(self.selectedNodes).length === self.nodes.length);
                self.selectedNodes = {};
                if (!allSelected) {
                    self.nodes.forEach(function (n) { self.selectedNodes[n._id] = true; });
                }
                self.filterNodes();
            },

            updateSelectedCount: function () {
                var count = Object.keys(this.selectedNodes).length;
                var el = document.getElementById('md-selected-count-msg');
                if (el) el.innerText = 'Выбрано: ' + count + ' из ' + this.nodes.length + ' компьютеров';
            },

            startDeployment: function () {
                var self = this;
                var targetIds = Object.keys(self.selectedNodes);

                if (targetIds.length === 0) {
                    alert('Пожалуйста, выберите хотя бы один компьютер.');
                    return;
                }
                if (!self.uploadedFile) {
                    alert('Пожалуйста, выберите файл для отправки.');
                    return;
                }

                var destPath = document.getElementById('md-dest-path').value;
                var runAfter = document.getElementById('md-run-after-cb').checked;
                var runArgs = document.getElementById('md-run-args').value;
                var overwrite = document.getElementById('md-overwrite-cb').checked;

                return self.startNativeJob(targetIds, destPath, runAfter, runArgs, overwrite);

                var nodeNames = {};
                var nodeOS = {};
                self.nodes.forEach(function (n) {
                    nodeNames[n._id] = n.name;
                    nodeOS[n._id] = n.os;
                });

                var payload = {
                    fileId: self.uploadedFile.fileId,
                    fileName: self.uploadedFile.fileName,
                    fileSize: self.uploadedFile.fileSize,
                    destPath: destPath,
                    runAfter: runAfter,
                    runArgs: runArgs,
                    overwrite: overwrite,
                    targets: targetIds,
                    nodeNames: nodeNames,
                    nodeOS: nodeOS
                };

                var startBtn = document.getElementById('md-start-btn');
                if (startBtn) {
                    startBtn.disabled = true;
                    startBtn.innerText = 'Запуск задания...';
                }

                fetch('/plugin/multideploy/api/deploy', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                })
                .then(function (r) { return r.json(); })
                .then(function (res) {
                    if (res.ok) {
                        self.activeJobId = res.jobId;
                        self.switchTab('monitor');
                    } else {
                        alert('Ошибка запуска: ' + (res.error || 'Неизвестная ошибка'));
                        if (startBtn) {
                            startBtn.disabled = false;
                            startBtn.innerText = '🚀 Начать отправку';
                        }
                    }
                })
                .catch(function (e) {
                    alert('Сетевая ошибка: ' + e.message);
                    if (startBtn) {
                        startBtn.disabled = false;
                        startBtn.innerText = '🚀 Начать отправку';
                    }
                });
            },

            startNativeJob: function (targetIds, destPath, runAfter, runArgs, overwrite) {
                var self = this;
                var job = {
                    jobId: 'native_' + Date.now(),
                    file: self.uploadedFile.file,
                    fileName: self.uploadedFile.fileName,
                    fileSize: self.uploadedFile.fileSize,
                    destPath: destPath,
                    runAfter: runAfter,
                    runArgs: runArgs,
                    overwrite: overwrite,
                    totalCount: targetIds.length,
                    status: 'in_progress',
                    cancelled: false,
                    nodes: {}
                };
                self.nodes.forEach(function (node) {
                    if (targetIds.indexOf(node._id) >= 0) {
                        job.nodes[node._id] = { nodeId: node._id, name: node.name || node._id, os: node.os || '', status: 'queued', progress: 0, bytesTransferred: 0, speed: 0, error: null, exitCode: null };
                    }
                });
                self.nativeJob = job;
                self.activeJobId = job.jobId;
                self.switchTab('monitor');
                self.startNativeDeployment(job);
            },

            startNativeDeployment: function (job) {
                var self = this;
                function next() {
                    if (job.cancelled) {
                        for (var cancelledId in job.nodes) {
                            if (job.nodes[cancelledId].status === 'queued' || job.nodes[cancelledId].status === 'downloading') {
                                job.nodes[cancelledId].status = 'failed';
                                job.nodes[cancelledId].error = 'Отменено пользователем';
                            }
                        }
                        job.status = 'cancelled';
                        self.updateMonitorUI(job);
                        return;
                    }
                    var nodeId = null;
                    for (var id in job.nodes) { if (job.nodes[id].status === 'queued') { nodeId = id; break; } }
                    if (!nodeId) {
                        job.status = 'completed';
                        self.updateMonitorUI(job);
                        return;
                    }
                    var node = job.nodes[nodeId];
                    node.status = 'downloading';
                    self.updateMonitorUI(job);
                    self.sendNativeFile(job, nodeId).then(function () {
                        node.status = 'completed';
                        node.progress = 100;
                        node.bytesTransferred = job.fileSize;
                        if (job.runAfter) self.runNativeFile(job, nodeId);
                    }).catch(function (error) {
                        node.status = 'failed';
                        node.error = error.message || String(error);
                    }).then(function () {
                        self.updateMonitorUI(job);
                        next();
                    });
                }
                next();
            },

            getNativeTarget: function (job, node) {
                var isWindows = (node.os || '').toLowerCase().indexOf('win') !== -1;
                var target = (job.destPath || '').trim();
                if (!target) target = isWindows ? ('C:\\Temp\\' + job.fileName) : ('/tmp/' + job.fileName);
                if (target.endsWith('\\') || target.endsWith('/')) target += job.fileName;
                target = target.replace(/\\/g, '/');
                var slash = target.lastIndexOf('/');
                return { directory: slash >= 0 ? target.substring(0, slash) : '.', name: slash >= 0 ? target.substring(slash + 1) : target, fullPath: target, isWindows: isWindows };
            },

            sendNativeFile: function (job, nodeId) {
                var self = this;
                var node = job.nodes[nodeId];
                var target = self.getNativeTarget(job, node);
                return new Promise(function (resolve, reject) {
                    if (typeof CreateAgentRedirect !== 'function' || typeof CreateRemoteFiles !== 'function') {
                        reject(new Error('Встроенный файловый канал MeshCentral недоступен в этом интерфейсе'));
                        return;
                    }
                    self.ensureNativeDirectory(nodeId, target).then(function () {
                        var redirect, offset = 0, complete = false, settled = false, readerBusy = false;
                        var timeout = setTimeout(function () { finish(new Error('Таймаут подключения к файловому каналу')); }, 30000);
                        function finish(error) {
                            if (settled) return;
                            settled = true;
                            clearTimeout(timeout);
                            delete self.nativeTransfers[nodeId];
                            try { if (redirect) redirect.Stop(); } catch (e) {}
                            if (error) reject(error); else resolve();
                        }
                        function sendText(message) { redirect.sendText(JSON.stringify(message)); }
                        function sendNext() {
                            if (settled || readerBusy) return;
                            if (offset >= job.fileSize) {
                                if (!complete) { complete = true; sendText({ action: 'uploaddone', reqid: 1 }); }
                                return;
                            }
                            readerBusy = true;
                            var end = Math.min(offset + 65536, job.fileSize);
                            var reader = new FileReader();
                            reader.onerror = function () { finish(new Error('Не удалось прочитать выбранный файл')); };
                            reader.onload = function () {
                                readerBusy = false;
                                var bytes = new Uint8Array(reader.result);
                                if (bytes.length && (bytes[0] === 0 || bytes[0] === 123)) {
                                    var framed = new Uint8Array(bytes.length + 1);
                                    framed.set(bytes, 1);
                                    redirect.send(framed);
                                } else { redirect.send(bytes); }
                                offset = end;
                                node.bytesTransferred = offset;
                                node.progress = Math.round((offset / job.fileSize) * 100);
                                self.updateMonitorUI(job);
                            };
                            reader.readAsArrayBuffer(job.file.slice(offset, end));
                        }
                        function onMessage(data) {
                            var message = data;
                            try { if (typeof data === 'string') message = JSON.parse(data); } catch (e) { return; }
                            if (!message || parseInt(message.reqid) !== 1) return;
                            if (message.action === 'uploadstart' || message.action === 'uploadack') sendNext();
                            else if (message.action === 'uploaddone') finish();
                            else if (message.action === 'uploaderror') finish(new Error('Агент не смог записать файл в ' + target.directory));
                        }
                        var filesProtocol = CreateRemoteFiles(onMessage);
                        redirect = CreateAgentRedirect(meshserver, filesProtocol, serverPublicNamePort, authCookie, authRelayCookie, domainUrl);
                        self.nativeTransfers[nodeId] = redirect;
                        redirect.onStateChanged = function (x, state) {
                            if (state === 3) sendText({ action: 'upload', reqid: 1, path: target.directory, name: target.name, size: job.fileSize });
                            else if (state === 0 && offset > 0) finish(new Error('Файловый канал был отключён'));
                        };
                        try { redirect.Start(nodeId); } catch (e) { finish(e); }
                    }).catch(reject);
                });
            },

            ensureNativeDirectory: function (nodeId, target) {
                return new Promise(function (resolve) {
                    var command = target.isWindows ? "powershell -NoProfile -NonInteractive -Command \"New-Item -ItemType Directory -Force -Path '" + target.directory.replace(/'/g, "''") + "' | Out-Null\"" : "mkdir -p '" + target.directory.replace(/'/g, "'\\''") + "'";
                    try { meshserver.send({ action: 'runcommands', nodeids: [nodeId], type: 3, runAsUser: 0, cmds: command }); } catch (e) {}
                    setTimeout(resolve, 800);
                });
            },

            runNativeFile: function (job, nodeId) {
                var target = this.getNativeTarget(job, job.nodes[nodeId]);
                var command = target.isWindows ? "powershell -NoProfile -NonInteractive -Command \"Start-Process -FilePath '" + target.fullPath.replace(/'/g, "''") + "' -ArgumentList '" + (job.runArgs || '').replace(/'/g, "''") + "'\"" : "chmod +x '" + target.fullPath.replace(/'/g, "'\\''") + "'; '" + target.fullPath.replace(/'/g, "'\\''") + "' " + (job.runArgs || '');
                try { meshserver.send({ action: 'runcommands', nodeids: [nodeId], type: 3, runAsUser: 0, cmds: command }); } catch (e) {}
            },

            renderMonitorTab: function () {
                var self = this;
                var body = document.getElementById('md-content-body');
                var footer = document.getElementById('md-footer-controls');

                if (!self.activeJobId) {
                    body.innerHTML = `
                        <div style="text-align: center; padding: 40px; color: #64748b;">
                            <div style="font-size: 32px; margin-bottom: 8px;">📡</div>
                            <div>Нет активного задания отправки</div>
                            <button class="md-btn md-btn-secondary" style="margin-top: 12px;" onclick="window.MultiDeployPlugin.switchTab('new')">
                                Создать новую отправку
                            </button>
                        </div>
                    `;
                    footer.innerHTML = '';
                    return;
                }

                body.innerHTML = `
                    <div class="md-card">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                            <div>
                                <div style="font-size: 16px; font-weight: 600;" id="md-job-filename">—</div>
                                <div style="font-size: 12px; color: #94a3b8;" id="md-job-meta">—</div>
                            </div>
                            <div id="md-job-status-pill"></div>
                        </div>
                        <div class="md-progress-bar-bg" style="height: 12px; margin-bottom: 12px;">
                            <div class="md-progress-bar-fill" id="md-overall-progress" style="width: 0%;"></div>
                        </div>
                        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; text-align: center;">
                            <div style="background: #1e293b; padding: 8px; border-radius: 6px;">
                                <div style="font-size: 11px; color: #94a3b8;">ВСЕГО</div>
                                <div style="font-size: 18px; font-weight: 700;" id="md-stat-total">0</div>
                            </div>
                            <div style="background: #1e293b; padding: 8px; border-radius: 6px;">
                                <div style="font-size: 11px; color: #60a5fa;">В ПРОЦЕССЕ</div>
                                <div style="font-size: 18px; font-weight: 700; color: #60a5fa;" id="md-stat-progress">0</div>
                            </div>
                            <div style="background: #1e293b; padding: 8px; border-radius: 6px;">
                                <div style="font-size: 11px; color: #34d399;">УСПЕШНО</div>
                                <div style="font-size: 18px; font-weight: 700; color: #34d399;" id="md-stat-success">0</div>
                            </div>
                            <div style="background: #1e293b; padding: 8px; border-radius: 6px;">
                                <div style="font-size: 11px; color: #f87171;">ОШИБКИ</div>
                                <div style="font-size: 18px; font-weight: 700; color: #f87171;" id="md-stat-failed">0</div>
                            </div>
                        </div>
                    </div>

                    <div class="md-card">
                        <div class="md-section-title">Состояние по компьютерам в реальном времени</div>
                        <div class="md-nodes-table-wrap" style="max-height: 240px;">
                            <table class="md-table">
                                <thead>
                                    <tr>
                                        <th>Компьютер</th>
                                        <th style="width: 140px;">Статус</th>
                                        <th>Прогресс передачи</th>
                                        <th style="width: 90px;">Скорость</th>
                                        <th>Результат / Ошибка</th>
                                    </tr>
                                </thead>
                                <tbody id="md-monitor-tbody">
                                    <!-- Rendered dynamically -->
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;

                footer.innerHTML = `
                    <button class="md-btn md-btn-danger" id="md-cancel-btn" onclick="window.MultiDeployPlugin.cancelCurrentJob()">
                        ⏹️ Отменить
                    </button>
                    <button class="md-btn md-btn-secondary" id="md-retry-btn" onclick="window.MultiDeployPlugin.retryFailedCurrentJob()" style="display: none;">
                        🔁 Повторить неудачные
                    </button>
                    <button class="md-btn md-btn-secondary" onclick="window.MultiDeployPlugin.switchTab('new')">
                        ➕ Новая отправка
                    </button>
                `;

                self.startMonitoring();
            },

            startMonitoring: function () {
                var self = this;
                if (self.pollTimer) clearInterval(self.pollTimer);
                if (self.nativeJob && self.nativeJob.jobId === self.activeJobId) {
                    self.updateMonitorUI(self.nativeJob);
                    return;
                }

                function poll() {
                    if (!self.activeJobId || self.currentTab !== 'monitor') return;
                    fetch('/plugin/multideploy/api/job/' + self.activeJobId)
                        .then(function (r) { return r.json(); })
                        .then(function (job) {
                            self.updateMonitorUI(job);
                        })
                        .catch(function (e) {
                            console.error('Job poll error:', e);
                        });
                }

                poll();
                self.pollTimer = setInterval(poll, 1200);
            },

            updateMonitorUI: function (job) {
                var self = this;
                var fnEl = document.getElementById('md-job-filename');
                if (!fnEl) return;

                fnEl.innerText = job.fileName || 'Файл';
                var metaEl = document.getElementById('md-job-meta');
                if (metaEl) {
                    metaEl.innerText = 'Размер: ' + self.formatBytes(job.fileSize) + ' | Назначение: ' + (job.destPath || 'По умолчанию');
                }

                var successCount = 0;
                var failCount = 0;
                var inProgCount = 0;
                var totalBytesTarget = job.fileSize * job.totalCount;
                var totalBytesDone = 0;

                var tbody = document.getElementById('md-monitor-tbody');
                var rowsHtml = '';

                for (var nodeId in job.nodes) {
                    var n = job.nodes[nodeId];
                    totalBytesDone += (n.bytesTransferred || 0);

                    var badge = '';
                    if (n.status === 'completed') {
                        successCount++;
                        badge = '<span class="md-badge md-badge-completed">✅ Завершено</span>';
                    } else if (n.status === 'downloading') {
                        inProgCount++;
                        badge = '<span class="md-badge md-badge-downloading">🚀 Загрузка (' + (n.progress || 0) + '%)</span>';
                    } else if (n.status === 'failed') {
                        failCount++;
                        badge = '<span class="md-badge md-badge-failed">❌ Ошибка</span>';
                    } else {
                        inProgCount++;
                        badge = '<span class="md-badge md-badge-queued">⏳ В очереди</span>';
                    }

                    var speedText = n.speed ? (self.formatBytes(n.speed) + '/с') : '—';
                    var errorOrResult = n.error ? ('<span style="color: #f87171;">' + n.error + '</span>') : (n.exitCode !== null ? ('Код выхода: ' + n.exitCode) : '—');

                    rowsHtml += `
                        <tr>
                            <td><strong>${n.name || nodeId}</strong></td>
                            <td>${badge}</td>
                            <td>
                                <div class="md-progress-bar-bg">
                                    <div class="md-progress-bar-fill" style="width: ${n.progress || 0}%;"></div>
                                </div>
                                <div style="font-size: 11px; color: #94a3b8; margin-top: 2px;">
                                    ${self.formatBytes(n.bytesTransferred || 0)} / ${self.formatBytes(job.fileSize)} (${n.progress || 0}%)
                                </div>
                            </td>
                            <td style="color: #94a3b8;">${speedText}</td>
                            <td style="font-size: 12px;">${errorOrResult}</td>
                        </tr>
                    `;
                }

                if (tbody) tbody.innerHTML = rowsHtml;

                var overallPct = totalBytesTarget > 0 ? Math.round((totalBytesDone / totalBytesTarget) * 100) : 0;
                if (successCount === job.totalCount) overallPct = 100;

                var overallProgress = document.getElementById('md-overall-progress');
                if (overallProgress) overallProgress.style.width = overallPct + '%';

                document.getElementById('md-stat-total').innerText = job.totalCount;
                document.getElementById('md-stat-progress').innerText = inProgCount;
                document.getElementById('md-stat-success').innerText = successCount;
                document.getElementById('md-stat-failed').innerText = failCount;

                var retryBtn = document.getElementById('md-retry-btn');
                var cancelBtn = document.getElementById('md-cancel-btn');

                if (failCount > 0 && retryBtn) retryBtn.style.display = 'inline-block';
                if (inProgCount === 0 && cancelBtn) cancelBtn.style.display = 'none';

                var activeCounter = document.getElementById('md-active-counter');
                if (activeCounter) activeCounter.innerText = inProgCount;
            },

            cancelCurrentJob: function () {
                var self = this;
                if (!self.activeJobId) return;
                if (!confirm('Отменить отправку на оставшиеся компьютеры?')) return;
                if (self.nativeJob && self.nativeJob.jobId === self.activeJobId) {
                    self.nativeJob.cancelled = true;
                    for (var nodeId in self.nativeTransfers) {
                        try { self.nativeTransfers[nodeId].Stop(); } catch (e) {}
                    }
                    self.nativeTransfers = {};
                    return;
                }
                fetch('/plugin/multideploy/api/cancel/' + self.activeJobId, { method: 'POST' })
                    .then(function () { self.startMonitoring(); });
            },

            retryFailedCurrentJob: function () {
                var self = this;
                if (!self.activeJobId) return;
                if (self.nativeJob && self.nativeJob.jobId === self.activeJobId) {
                    var hasFailed = false;
                    for (var nodeId in self.nativeJob.nodes) {
                        var node = self.nativeJob.nodes[nodeId];
                        if (node.status === 'failed') {
                            node.status = 'queued'; node.progress = 0; node.bytesTransferred = 0; node.error = null; hasFailed = true;
                        }
                    }
                    if (hasFailed) { self.nativeJob.cancelled = false; self.nativeJob.status = 'in_progress'; self.startNativeDeployment(self.nativeJob); }
                    return;
                }
                fetch('/plugin/multideploy/api/retry/' + self.activeJobId, { method: 'POST' })
                    .then(function () { self.startMonitoring(); });
            },

            renderHistoryTab: function () {
                var self = this;
                var body = document.getElementById('md-content-body');
                var footer = document.getElementById('md-footer-controls');

                body.innerHTML = `
                    <div class="md-card">
                        <div class="md-section-title">История предыдущих отправок</div>
                        <div class="md-nodes-table-wrap" style="max-height: 380px;">
                            <table class="md-table">
                                <thead>
                                    <tr>
                                        <th>Дата и время</th>
                                        <th>Имя файла</th>
                                        <th>Размер</th>
                                        <th>Успешно / Всего</th>
                                        <th>Статус</th>
                                        <th>Действия</th>
                                    </tr>
                                </thead>
                                <tbody id="md-history-tbody">
                                    <tr><td colspan="6" style="text-align: center; padding: 20px;">Загрузка истории...</td></tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;

                footer.innerHTML = `
                    <button class="md-btn md-btn-secondary" onclick="window.MultiDeployPlugin.closeModal()">Закрыть</button>
                    <button class="md-btn md-btn-secondary" onclick="window.MultiDeployPlugin.switchTab('new')">➕ Новая отправка</button>
                `;

                fetch('/plugin/multideploy/api/jobs')
                    .then(function (r) { return r.json(); })
                    .then(function (jobs) {
                        var tbody = document.getElementById('md-history-tbody');
                        if (!tbody) return;

                        if (!jobs || jobs.length === 0) {
                            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #64748b; padding: 20px;">История пуста</td></tr>';
                            return;
                        }

                        var html = '';
                        jobs.forEach(function (j) {
                            var dateStr = new Date(j.startTime).toLocaleString('ru-RU');
                            var statusBadge = (j.status === 'completed') 
                                ? '<span class="md-badge md-badge-completed">Успешно</span>' 
                                : (j.status === 'failed' ? '<span class="md-badge md-badge-failed">С ошибками</span>' : '<span class="md-badge md-badge-queued">Частично</span>');

                            html += `
                                <tr>
                                    <td style="color: #94a3b8;">${dateStr}</td>
                                    <td><strong>${j.fileName}</strong></td>
                                    <td style="color: #94a3b8;">${self.formatBytes(j.fileSize)}</td>
                                    <td><strong>${j.successCount}</strong> из ${j.totalCount}</td>
                                    <td>${statusBadge}</td>
                                    <td>
                                        <button class="md-btn md-btn-secondary" style="padding: 4px 8px; font-size: 11px;" onclick="window.MultiDeployPlugin.viewHistoryDetails('${j.jobId}')">
                                            Просмотр
                                        </button>
                                    </td>
                                </tr>
                            `;
                        });
                        tbody.innerHTML = html;
                    });
            },

            viewHistoryDetails: function (jobId) {
                this.activeJobId = jobId;
                this.switchTab('monitor');
            },

            formatBytes: function (bytes) {
                if (!bytes || bytes === 0) return '0 Б';
                var k = 1024;
                var sizes = ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ'];
                var i = Math.floor(Math.log(bytes) / Math.log(k));
                return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
            }
        };
    };

    // Device-level tab registration hook
    obj.registerPluginTab = function () {
        return {
            tabId: 'multideployTab',
            tabTitle: 'Отправка файлов'
        };
    };

    // Device refresh hook
    obj.onDeviceRefreshEnd = function () {
        var tabContainer = document.getElementById('multideployTab') || document.getElementById('pmultideployTab');
        if (!tabContainer || tabContainer._multideploy_ready) return;
        tabContainer._multideploy_ready = true;

        tabContainer.innerHTML = `
            <div style="padding: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
                <h3 style="margin-top: 0;">📤 Отправка файлов на компьютеры</h3>
                <p style="color: #64748b;">Вы можете отправить файлы как на текущий компьютер, так и сразу на несколько устройств с отслеживанием прогресса.</p>
                <button class="md-btn md-btn-success" style="padding: 10px 18px; font-size: 14px;" onclick="window.MultiDeployPlugin.openModal()">
                    🚀 Открыть мастер массовой отправки файлов
                </button>
            </div>
        `;
    };

    return obj;
};

module.exports.multideploy = module.exports;
