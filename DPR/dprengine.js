// ============================================================
//  DPR ENGINE — dprengine.js
//  Central bridge between the website and Python EA clients
//
//  HTTP API  → port 3001  (serves the React website)
//  TCP server → port 9090  (EA clients connect here)
//
//  Flow:
//    Website  ──HTTP──►  Engine  ──TCP──►  EA (Python)
//    EA (Python)  ──TCP──►  Engine  ──db.json──►  Website (polling)
// ============================================================

const net     = require('net');
const express = require('express');
const cors    = require('cors');
const fs      = require('fs');
const path    = require('path');
const { spawn } = require('child_process');

// ── Config ───────────────────────────────────────────────────
const HTTP_PORT = 3001;
const TCP_PORT  = 9090;
const DB_PATH   = path.resolve(__dirname, '../website/server/db.json');

// ── DB helpers ───────────────────────────────────────────────
function readDb()       { return JSON.parse(fs.readFileSync(DB_PATH, 'utf-8')); }
function writeDb(data)  { fs.writeFileSync(DB_PATH, JSON.stringify(data, null, 2), 'utf-8'); }
function today()        { return new Date().toISOString().split('T')[0]; }
function normalizeMasterMode(value) {
    return String(value || '').toLowerCase() === 'automated' ? 'automated' : 'manual';
}
function normalizeMt5Path(value) {
    if (typeof value !== 'string') return '';
    const trimmed = value.trim();
    if (!trimmed) return '';
    try { return path.normalize(trimmed); }
    catch (_e) { return trimmed; }
}
function mt5PathKey(value) { return normalizeMt5Path(value).toLowerCase(); }
function safePathExists(mt5Path) {
    try { return fs.existsSync(mt5Path); }
    catch (_e) { return false; }
}
function allAccountRefs(db) {
    const masters = (db.masterAccounts || []).map((m) => ({
        kind: 'master',
        id: String(m.id),
        name: m.name || '',
        mt5Path: m.mt5Path || '',
    }));
    const slaves = (db.slaveAccounts || []).map((s) => ({
        kind: 'slave',
        id: String(s.id),
        name: s.name || '',
        mt5Path: s.mt5Path || '',
    }));
    return [...masters, ...slaves];
}
function findMt5PathConflict(db, mt5Path, exclude = null) {
    const key = mt5PathKey(mt5Path);
    if (!key) return null;
    return allAccountRefs(db).find((rec) => {
        if (exclude && rec.kind === exclude.kind && String(rec.id) === String(exclude.id)) {
            return false;
        }
        return mt5PathKey(rec.mt5Path) === key;
    }) || null;
}
function validateUniqueMt5Path(db, mt5Path, exclude = null) {
    const normalized = normalizeMt5Path(mt5Path);
    if (!normalized) {
        return { error: 'mt5Path is required' };
    }

    const conflict = findMt5PathConflict(db, normalized, exclude);
    if (conflict) {
        return {
            error: `mt5Path "${normalized}" is already assigned to ${conflict.kind}:${conflict.id}${conflict.name ? ` (${conflict.name})` : ''}`,
            conflict: { ...conflict, mt5Path: normalizeMt5Path(conflict.mt5Path) },
        };
    }
    return { mt5Path: normalized };
}
function buildMt5PathWarnings(db) {
    const missingPaths = [];
    const duplicatePaths = [];
    const unavailablePaths = [];
    const byPath = new Map();

    allAccountRefs(db).forEach((rec) => {
        const normalized = normalizeMt5Path(rec.mt5Path);
        const entry = { kind: rec.kind, id: rec.id, name: rec.name, mt5Path: normalized };

        if (!normalized) {
            missingPaths.push(entry);
            return;
        }

        if (!safePathExists(normalized)) {
            unavailablePaths.push(entry);
        }

        const key = mt5PathKey(normalized);
        const bucket = byPath.get(key) || [];
        bucket.push(entry);
        byPath.set(key, bucket);
    });

    byPath.forEach((accounts, key) => {
        if (accounts.length > 1) {
            duplicatePaths.push({ mt5Path: accounts[0].mt5Path || key, accounts });
        }
    });

    return { missingPaths, duplicatePaths, unavailablePaths };
}

// ── TCP state ────────────────────────────────────────────────
const clientsByTag = new Map();   // tag (broker) => Set<socket>
const tagBySocket  = new WeakMap(); // socket => tag
const bindBySocket = new WeakMap(); // socket => { kind: "master"|"slave", id: "..." }
const socketInfo   = new Map();   // socket => { id, tag, addr }

function removeSocket(socket) {
    const tag = tagBySocket.get(socket);
    if (tag) {
        const set = clientsByTag.get(tag);
        if (set) {
            set.delete(socket);
            if (set.size === 0) clientsByTag.delete(tag);
        }
        tagBySocket.delete(socket);
    }
    const info = socketInfo.get(socket);
    if (info) info.tag = null;
    bindBySocket.delete(socket);
}

function registerSocket(socket, tag) {
    const t = (tag || '').toUpperCase();
    removeSocket(socket);
    if (!t) return 0;
    let set = clientsByTag.get(t);
    if (!set) { set = new Set(); clientsByTag.set(t, set); }
    set.add(socket);
    tagBySocket.set(socket, t);
    const info = socketInfo.get(socket);
    if (info) info.tag = t;
    return set.size;
}

// Send a JSON command to all EAs registered under a broker tag
function sendToTag(tag, obj) {
    const t = (tag || '').toUpperCase();
    const set = clientsByTag.get(t);
    if (!set || set.size === 0) return 0;
    const payload = JSON.stringify(obj) + '\n';
    let sent = 0;
    set.forEach((sock) => {
        if (!sock || sock.destroyed) { removeSocket(sock); return; }
        try { sock.write(payload); sent++; }
        catch (e) { removeSocket(sock); try { sock.destroy(); } catch {} }
    });
    return sent;
}

function sendToTagByKind(tag, obj, kind) {
    const t = (tag || '').toUpperCase();
    const set = clientsByTag.get(t);
    if (!set || set.size === 0) return 0;
    const payload = JSON.stringify(obj) + '\n';
    let sent = 0;
    set.forEach((sock) => {
        if (!sock || sock.destroyed) { removeSocket(sock); return; }
        const bind = bindBySocket.get(sock);
        if (!bind || bind.kind !== kind) return;
        try { sock.write(payload); sent++; }
        catch (e) { removeSocket(sock); try { sock.destroy(); } catch {} }
    });
    return sent;
}

function sendToBound(kind, id, obj) {
    if (!kind || !id) return 0;
    const payload = JSON.stringify(obj) + '\n';
    let sent = 0;
    clientsByTag.forEach((set) => {
        set.forEach((sock) => {
            if (!sock || sock.destroyed) { removeSocket(sock); return; }
            const bind = bindBySocket.get(sock);
            if (!bind || bind.kind !== kind || String(bind.id) !== String(id)) return;
            try { sock.write(payload); sent++; }
            catch (_e) { removeSocket(sock); try { sock.destroy(); } catch {} }
        });
    });
    return sent;
}

function sendToSlavesByMaster(tag, masterId, obj) {
    const t = (tag || '').toUpperCase();
    const set = clientsByTag.get(t);
    if (!set || set.size === 0) return 0;
    const payload = JSON.stringify(obj) + '\n';
    let sent = 0;
    set.forEach((sock) => {
        if (!sock || sock.destroyed) { removeSocket(sock); return; }
        const bind = bindBySocket.get(sock);
        if (!bind || bind.kind !== 'slave') return;
        if (String(bind.masterId || '') !== String(masterId || '')) return;
        try { sock.write(payload); sent++; }
        catch (_e) { removeSocket(sock); try { sock.destroy(); } catch {} }
    });
    return sent;
}

// Send to every connected EA regardless of tag
// eslint-disable-next-line no-unused-vars
function broadcastAll(obj) {
    let sent = 0;
    const payload = JSON.stringify(obj) + '\n';
    clientsByTag.forEach((set) => {
        set.forEach((sock) => {
            if (!sock || sock.destroyed) { removeSocket(sock); return; }
            try { sock.write(payload); sent++; }
            catch (e) { removeSocket(sock); try { sock.destroy(); } catch {} }
        });
    });
    return sent;
}

// Summary of all connected EAs (for /api/connections)
function getConnections() {
    const result = [];
    clientsByTag.forEach((set, tag) => {
        const clients = [];
        set.forEach((sock) => {
            const info = socketInfo.get(sock);
            clients.push({ id: info?.id || 'unknown', addr: info?.addr || '' });
        });
        result.push({ tag, count: set.size, clients });
    });
    return result;
}

// ── Handle live data pushed by EAs ───────────────────────────
// EAs can push: DATA {"balance":1000,"equity":1020,"margin":50,"freeMargin":970,"pnl":20}
// Engine matches the broker tag to master/slave accounts in db.json and updates them
// Update MT5 connection status for all accounts matching the broker tag
function handleEaStatus(tag, state, message, bind) {
    try {
        const db = readDb();
        let updated = false;

        if (bind?.kind === 'master' && bind.id) {
            const m = db.masterAccounts.find((x) => x.id === bind.id);
            if (m) {
                m.mtStatus = state;
                m.mtMessage = message;
                m.lastUpdated = today();
                updated = true;
            }
        } else if (bind?.kind === 'slave' && bind.id) {
            const s = db.slaveAccounts.find((x) => x.id === bind.id);
            if (s) {
                s.mtStatus = state;
                s.mtMessage = message;
                s.lastUpdated = today();
                updated = true;
            }
        } else {
            db.masterAccounts.forEach((m) => {
                if (m.broker.toUpperCase() === tag) {
                    m.mtStatus  = state;
                    m.mtMessage = message;
                    m.lastUpdated = today();
                    updated = true;
                }
            });
            db.slaveAccounts.forEach((s) => {
                if (s.broker.toUpperCase() === tag) {
                    s.mtStatus  = state;
                    s.mtMessage = message;
                    s.lastUpdated = today();
                    updated = true;
                }
            });
        }

        if (updated) {
            writeDb(db);
            const who = bind?.id ? `${bind.kind}:${bind.id}` : `tag=${tag}`;
            console.log(`[STATUS] ${who} state=${state} message=${message}`);
        }
    } catch (e) {
        console.error(`[ENGINE] handleEaStatus error: ${e.message}`);
    }
}

function handleEaData(tag, json, bind) {
    try {
        const db = readDb();
        let updated = false;

        if (bind?.kind === 'master' && bind.id) {
            const m = db.masterAccounts.find((x) => x.id === bind.id);
            if (m) {
                if (json.balance    !== undefined) m.balance    = json.balance;
                if (json.equity     !== undefined) m.equity     = json.equity;
                if (json.margin     !== undefined) m.margin     = json.margin;
                if (json.freeMargin !== undefined) m.freeMargin = json.freeMargin;
                if (json.pnl        !== undefined) m.pnl        = json.pnl;
                if (json.totalPnl   !== undefined) m.totalPnl   = json.totalPnl;
                if (json.slaveCount !== undefined) m.slaveCount = json.slaveCount;
                m.lastUpdated = today();
                updated = true;
            }
        } else if (bind?.kind === 'slave' && bind.id) {
            const s = db.slaveAccounts.find((x) => x.id === bind.id);
            if (s) {
                if (json.balance  !== undefined) s.balance  = json.balance;
                if (json.equity   !== undefined) s.equity   = json.equity;
                if (json.margin   !== undefined) s.margin   = json.margin;
                if (json.freeMargin !== undefined) s.freeMargin = json.freeMargin;
                if (json.pnl      !== undefined) s.pnl      = json.pnl;
                if (json.totalPnl !== undefined) s.totalPnl = json.totalPnl;
                s.lastUpdated = today();
                updated = true;
            }
        } else {
            db.masterAccounts.forEach((m) => {
                if (m.broker.toUpperCase() === tag) {
                    if (json.balance    !== undefined) m.balance    = json.balance;
                    if (json.equity     !== undefined) m.equity     = json.equity;
                    if (json.margin     !== undefined) m.margin     = json.margin;
                    if (json.freeMargin !== undefined) m.freeMargin = json.freeMargin;
                    if (json.pnl        !== undefined) m.pnl        = json.pnl;
                    if (json.totalPnl   !== undefined) m.totalPnl   = json.totalPnl;
                    if (json.slaveCount !== undefined) m.slaveCount = json.slaveCount;
                    m.lastUpdated = today();
                    updated = true;
                }
            });

            db.slaveAccounts.forEach((s) => {
                if (s.broker.toUpperCase() === tag) {
                    if (json.balance  !== undefined) s.balance  = json.balance;
                    if (json.equity   !== undefined) s.equity   = json.equity;
                    if (json.margin   !== undefined) s.margin   = json.margin;
                    if (json.freeMargin !== undefined) s.freeMargin = json.freeMargin;
                    if (json.pnl      !== undefined) s.pnl      = json.pnl;
                    if (json.totalPnl !== undefined) s.totalPnl = json.totalPnl;
                    s.lastUpdated = today();
                    updated = true;
                }
            });
        }

        if (updated) {
            writeDb(db);
            const who = bind?.id ? `${bind.kind}:${bind.id}` : `tag=${tag}`;
            console.log(`[ENGINE] db updated for ${who}`);
        }
    } catch (e) {
        console.error(`[ENGINE] handleEaData error: ${e.message}`);
    }
}

// ── TCP server — EA clients connect here ─────────────────────
const tcpServer = net.createServer();

tcpServer.on('connection', (socket) => {
    socket.setKeepAlive(true, 30000);
    socket.setNoDelay(true);
    socket._buf = '';

    let activated = false;
    let clientId  = null;
    const addr    = `${socket.remoteAddress}:${socket.remotePort}`;
    socketInfo.set(socket, { id: null, tag: null, addr });
    console.log(`[TCP] New connection from ${addr}`);

    socket.on('data', (data) => {
        socket._buf += data.toString('utf8');
        let idx;
        while ((idx = socket._buf.indexOf('\n')) !== -1) {
            const line = socket._buf.slice(0, idx).trim();
            socket._buf = socket._buf.slice(idx + 1);
            if (!line) continue;

            console.log(`[TCP ←] ${clientId || addr} | ${line}`);

            // First message = client identification
            if (!activated) {
                activated = true;
                clientId  = line || Math.random().toString(36).slice(2, 8);
                const info = socketInfo.get(socket);
                if (info) info.id = clientId;
                socket.write('VALID\n');
                console.log(`[TCP] EA connected id=${clientId}`);
                continue;
            }

            // Register broker tag: "BROKER <TAG>"
            if (line.startsWith('BROKER ')) {
                const tag   = line.split(' ')[1]?.toUpperCase();
                const count = registerSocket(socket, tag);
                socket.write(`REGISTERED ${tag} COUNT ${count}\n`);
                console.log(`[TCP] id=${clientId} registered tag=${tag} peers=${count}`);
                continue;
            }

            // Optional socket-to-account binding:
            // BIND {"kind":"master"|"slave","id":"m101|s101","masterId":"m101?"}
            if (line.startsWith('BIND ')) {
                try {
                    const bind = JSON.parse(line.slice(5));
                    if (!bind?.kind || !bind?.id || !['master', 'slave'].includes(bind.kind)) {
                        throw new Error('invalid bind payload');
                    }
                    const normalized = { kind: bind.kind, id: String(bind.id) };
                    if (bind.kind === 'slave' && bind.masterId) {
                        normalized.masterId = String(bind.masterId);
                    }
                    bindBySocket.set(socket, normalized);
                    socket.write(`BOUND ${bind.kind} ${bind.id}\n`);
                    if (normalized.masterId) {
                        console.log(`[TCP] id=${clientId} bound ${bind.kind}:${bind.id} master=${normalized.masterId}`);
                    } else {
                        console.log(`[TCP] id=${clientId} bound ${bind.kind}:${bind.id}`);
                    }
                } catch (e) {
                    socket.write('BIND_ERROR\n');
                    console.log(`[TCP] Bad BIND from ${clientId}: ${e.message}`);
                }
                continue;
            }

            // Live data push from EA: "DATA {json}"
            if (line.startsWith('DATA ')) {
                try {
                    const json = JSON.parse(line.slice(5));
                    const tag  = tagBySocket.get(socket);
                    const bind = bindBySocket.get(socket);
                    if (tag) handleEaData(tag, json, bind);
                } catch (e) {
                    console.log(`[TCP] Bad DATA from ${clientId}: ${e.message}`);
                }
                continue;
            }

            // EA connection status: "STATUS {json}"
            // json: { state: "connected"|"error"|"disconnected", message: "..." }
            if (line.startsWith('STATUS ')) {
                try {
                    const json = JSON.parse(line.slice(7));
                    const tag  = tagBySocket.get(socket);
                    const bind = bindBySocket.get(socket);
                    if (tag) handleEaStatus(tag, json.state, json.message || '', bind);
                } catch (e) {
                    console.log(`[TCP] Bad STATUS from ${clientId}: ${e.message}`);
                }
                continue;
            }

            // Master-to-slave copy signal: "SIGNAL {json}"
            if (line.startsWith('SIGNAL ')) {
                try {
                    const json = JSON.parse(line.slice(7));
                    const tag = tagBySocket.get(socket);
                    const bind = bindBySocket.get(socket);
                    if (!tag) continue;
                    if (!bind || bind.kind !== 'master') {
                        console.log(`[TCP] Ignored SIGNAL from non-master id=${clientId}`);
                        continue;
                    }
                    const sourceMasterId = bind.id || json.masterId;
                    const payload = { ...json, masterId: sourceMasterId };
                    const sent = sendToSlavesByMaster(tag, sourceMasterId, payload);
                    console.log(`[COPY] tag=${tag} from=${sourceMasterId || clientId} sent_to_slaves=${sent} payload=${JSON.stringify(payload)}`);
                } catch (e) {
                    console.log(`[TCP] Bad SIGNAL from ${clientId}: ${e.message}`);
                }
                continue;
            }

            // Slave-to-master reverse close: "REVERSE_SIGNAL {json}"
            if (line.startsWith('REVERSE_SIGNAL ')) {
                try {
                    const json = JSON.parse(line.slice(15));
                    const bind = bindBySocket.get(socket);
                    if (!bind || bind.kind !== 'slave') {
                        console.log(`[TCP] Ignored REVERSE_SIGNAL from non-slave id=${clientId}`);
                        continue;
                    }
                    const targetMasterId = String(json.masterId || bind.masterId || '');
                    if (!targetMasterId) continue;
                    const sent = sendToBound('master', targetMasterId, json);
                    console.log(`[REVERSE_CLOSE] slave=${clientId} masterTicket=${json.masterTicket} sent_to_master=${sent}`);
                } catch (e) {
                    console.log(`[TCP] Bad REVERSE_SIGNAL from ${clientId}: ${e.message}`);
                }
                continue;
            }

            // Keepalive pings (empty newlines) are silently ignored above
            console.log(`[TCP] Ignored from ${clientId}: ${line}`);
        }
    });

    socket.on('end', () => {
        console.log(`[TCP] Disconnected id=${clientId}`);
        removeSocket(socket);
        socketInfo.delete(socket);
    });

    socket.on('error', (e) => {
        console.log(`[TCP] Error id=${clientId}: ${e.message}`);
        removeSocket(socket);
        socketInfo.delete(socket);
    });
});

tcpServer.listen(TCP_PORT, () => {
    console.log(`[TCP] EA listener running on port ${TCP_PORT}`);
});

// ── Express HTTP API — serves the React website ──────────────
const app = express();
app.use(cors());
app.use(express.json());

// ── Data ─────────────────────────────────────────────────────
app.get('/api/data', (_req, res) => {
    const db = readDb();
    res.json({ ...db, mt5PathWarnings: buildMt5PathWarnings(db) });
});

// Check if the MT5 path exists on the engine host machine
app.post('/api/mt5-path/check', (req, res) => {
    const mt5Path = normalizeMt5Path(req.body?.mt5Path);
    if (!mt5Path) return res.status(400).json({ error: 'mt5Path is required' });

    const exists = safePathExists(mt5Path);
    let isFile = false;
    if (exists) {
        try { isFile = fs.statSync(mt5Path).isFile(); }
        catch (_e) { isFile = false; }
    }

    res.json({ mt5Path, exists, isFile });
});

// ── Masters ──────────────────────────────────────────────────
app.post('/api/masters', (req, res) => {
    const db = readDb();
    const validation = validateUniqueMt5Path(db, req.body?.mt5Path);
    if (validation.error) {
        const code = validation.conflict ? 409 : 400;
        return res.status(code).json({ error: validation.error, conflict: validation.conflict || null });
    }
    const master = {
        ...req.body,
        mt5Path: validation.mt5Path,
        mode: normalizeMasterMode(req.body?.mode),
    };
    db.masterAccounts.push(master);
    writeDb(db);
    res.json(master);
});

app.put('/api/masters/:id', (req, res) => {
    const db  = readDb();
    const idx = db.masterAccounts.findIndex((m) => m.id === req.params.id);
    if (idx === -1) return res.status(404).json({ error: 'Not found' });
    const current = db.masterAccounts[idx];
    const hasMode = Object.prototype.hasOwnProperty.call(req.body || {}, 'mode');
    const mode = hasMode ? normalizeMasterMode(req.body?.mode) : normalizeMasterMode(current.mode);
    const hasMt5Path = Object.prototype.hasOwnProperty.call(req.body || {}, 'mt5Path');
    const requestedPath = hasMt5Path ? req.body.mt5Path : current.mt5Path;
    const validation = validateUniqueMt5Path(db, requestedPath, { kind: 'master', id: current.id });
    if (validation.error) {
        const code = validation.conflict ? 409 : 400;
        return res.status(code).json({ error: validation.error, conflict: validation.conflict || null });
    }
    db.masterAccounts[idx] = {
        ...current,
        ...req.body,
        mt5Path: validation.mt5Path,
        mode,
        lastUpdated: today(),
    };
    writeDb(db);
    res.json(db.masterAccounts[idx]);
});

app.delete('/api/masters/:id', (req, res) => {
    const db = readDb();
    db.masterAccounts = db.masterAccounts.filter((m) => m.id !== req.params.id);
    db.slaveAccounts  = db.slaveAccounts.filter((s) => s.masterId !== req.params.id);
    writeDb(db);
    res.json({ success: true });
});

// Activate one master (does not force other masters to pause)
app.post('/api/masters/:id/activate', (req, res) => {
    const db = readDb();
    const idx = db.masterAccounts.findIndex((m) => m.id === req.params.id);
    if (idx === -1) return res.status(404).json({ error: 'Not found' });
    const current = db.masterAccounts[idx];
    const validation = validateUniqueMt5Path(db, current.mt5Path, { kind: 'master', id: current.id });
    if (validation.error) {
        const code = validation.conflict ? 409 : 400;
        return res.status(code).json({ error: validation.error, conflict: validation.conflict || null });
    }
    db.masterAccounts[idx] = {
        ...current,
        mt5Path: validation.mt5Path,
        mode: normalizeMasterMode(current.mode),
        status: 'active',
        lastUpdated: today(),
    };
    writeDb(db);
    res.json({ success: true });
});

// ── Slaves ───────────────────────────────────────────────────
app.post('/api/slaves', (req, res) => {
    const db = readDb();
    const validation = validateUniqueMt5Path(db, req.body?.mt5Path);
    if (validation.error) {
        const code = validation.conflict ? 409 : 400;
        return res.status(code).json({ error: validation.error, conflict: validation.conflict || null });
    }
    const slave = { ...req.body, mt5Path: validation.mt5Path };
    db.slaveAccounts.push(slave);
    writeDb(db);
    res.json(slave);
});

app.put('/api/slaves/:id', (req, res) => {
    const db  = readDb();
    const idx = db.slaveAccounts.findIndex((s) => s.id === req.params.id);
    if (idx === -1) return res.status(404).json({ error: 'Not found' });
    const current = db.slaveAccounts[idx];
    const hasMt5Path = Object.prototype.hasOwnProperty.call(req.body || {}, 'mt5Path');
    const requestedPath = hasMt5Path ? req.body.mt5Path : current.mt5Path;
    const validation = validateUniqueMt5Path(db, requestedPath, { kind: 'slave', id: current.id });
    if (validation.error) {
        const code = validation.conflict ? 409 : 400;
        return res.status(code).json({ error: validation.error, conflict: validation.conflict || null });
    }
    db.slaveAccounts[idx] = {
        ...current,
        ...req.body,
        mt5Path: validation.mt5Path,
        lastUpdated: today(),
    };
    writeDb(db);
    res.json(db.slaveAccounts[idx]);
});

app.delete('/api/slaves/:id', (req, res) => {
    const db = readDb();
    db.slaveAccounts = db.slaveAccounts.filter((s) => s.id !== req.params.id);
    writeDb(db);
    res.json({ success: true });
});

// ── Trade command: website → engine → EA ─────────────────────
//
//  POST /api/command
//  Body: { masterId, type, lot? }
//
//  type options:
//    buy | sell | hedge          → requires "lot"
//    close_all | close_buys | close_sells | close_profits
//
//  Engine looks up the master's broker tag and routes to all
//  EAs currently connected under that tag.
//
app.post('/api/command', (req, res) => {
    const { masterId, type, lot } = req.body;

    if (!masterId || !type) {
        return res.status(400).json({ error: 'masterId and type are required' });
    }

    const db     = readDb();
    const master = db.masterAccounts.find((m) => m.id === masterId);
    if (!master) return res.status(404).json({ error: 'Master account not found' });
    if (master.status !== 'active') {
        return res.status(403).json({ error: 'Master account is not active — activate it first' });
    }
    if (normalizeMasterMode(master.mode) !== 'manual') {
        return res.status(403).json({ error: 'Master account is in automated mode — switch it to manual first' });
    }

    let payload;

    switch (type.toLowerCase()) {
        case 'buy':            payload = { action: 'BUY',            lot: parseFloat(lot) || 0.01 }; break;
        case 'sell':           payload = { action: 'SELL',           lot: parseFloat(lot) || 0.01 }; break;
        case 'hedge':          payload = { action: 'HEDGE',          lot: parseFloat(lot) || 0.01 }; break;
        case 'close_all':      payload = { action: 'CLOSE_ALL'      }; break;
        case 'close_buys':     payload = { action: 'CLOSE_BUYS'     }; break;
        case 'close_sells':    payload = { action: 'CLOSE_SELLS'    }; break;
        case 'close_profits':  payload = { action: 'CLOSE_PROFITS'  }; break;
        default:
            return res.status(400).json({ error: `Unknown command type: ${type}` });
    }

    const sent = sendToBound('master', master.id, payload);
    console.log(`[CMD] ${type.toUpperCase()} master=${master.id} lot=${lot || '-'} → sent_to_master=${sent}`);

    if (sent === 0) {
        return res.status(202).json({
            success: false,
            warning: `No EA connected for master "${master.id}"`,
            masterId: master.id,
            sent,
            payload,
        });
    }

    res.json({ success: true, masterId: master.id, sent, payload });
});

// ── Process Manager ───────────────────────────────────────────
const DPR_DIR = __dirname;
const PY_CMD  = process.platform === 'win32' ? 'python' : 'python3';

const MANAGED = [
    { name: 'master-ea-manager', cmd: PY_CMD, args: ['dpr_ea.py'],          cwd: DPR_DIR },
    { name: 'slave-ea-manager',  cmd: PY_CMD, args: ['slave_ea_manager.py'], cwd: DPR_DIR },
];

const processRegistry = new Map();
// entry shape: { proc, status, pid, restartCount, startedAt, lastRestartAt, alerts[] }

function pushAlert(name, message) {
    const r = processRegistry.get(name);
    if (!r) return;
    r.alerts.unshift({ time: new Date().toISOString(), message });
    if (r.alerts.length > 20) r.alerts.length = 20;
}

let _shuttingDown = false;

function spawnManaged(name) {
    if (_shuttingDown) return;
    const def = MANAGED.find((m) => m.name === name);
    if (!def) return;

    const r = processRegistry.get(name);
    r.status    = 'running';
    r.startedAt = new Date().toISOString();

    const proc = spawn(def.cmd, def.args, { cwd: def.cwd, stdio: 'inherit' });
    r.proc = proc;
    r.pid  = proc.pid;
    console.log(`[MANAGER] Started ${name} pid=${proc.pid}`);

    proc.on('exit', (code, signal) => {
        if (_shuttingDown) return;
        const rec = processRegistry.get(name);
        if (!rec) return;
        const msg = `${name} exited (code=${code} signal=${signal}) — restarting in 3s`;
        console.warn(`[MANAGER] ${msg}`);
        pushAlert(name, msg);
        rec.status        = 'restarting';
        rec.proc          = null;
        rec.pid           = null;
        rec.restartCount++;
        rec.lastRestartAt = new Date().toISOString();
        setTimeout(() => spawnManaged(name), 3000);
    });
}

// GET /api/system/status
app.get('/api/system/status', (_req, res) => {
    const result = [];
    processRegistry.forEach((r, name) => {
        result.push({
            name,
            status:        r.status,
            pid:           r.pid || null,
            restartCount:  r.restartCount,
            startedAt:     r.startedAt,
            lastRestartAt: r.lastRestartAt,
            alerts:        r.alerts.slice(0, 10),
        });
    });
    res.json(result);
});

// POST /api/system/restart/:name
app.post('/api/system/restart/:name', (req, res) => {
    const name = req.params.name;
    const r = processRegistry.get(name);
    if (!r) return res.status(404).json({ error: 'Unknown process' });
    pushAlert(name, 'Manual restart triggered');
    if (r.proc) { try { r.proc.kill(); } catch (_e) {} }
    res.json({ success: true });
});

// ── Connections: which EAs are live right now ─────────────────
app.get('/api/connections', (_req, res) => {
    const connections = getConnections();
    const total = connections.reduce((n, c) => n + c.count, 0);
    res.json({ total, connections });
});

// ── Start HTTP ────────────────────────────────────────────────
app.listen(HTTP_PORT, () => {
    console.log(`[HTTP] DPR Engine API running on port ${HTTP_PORT}`);
    console.log(`[INFO] DB path: ${DB_PATH}`);
});

// ── Resilience ────────────────────────────────────────────────
process.on('uncaughtException',  (e) => console.error('[UNCAUGHT]',  e));
process.on('unhandledRejection', (e) => console.error('[UNHANDLED]', e));

// ── Start managed sub-processes ───────────────────────────────
MANAGED.forEach((m) => {
    processRegistry.set(m.name, {
        proc: null, status: 'stopped', pid: null,
        restartCount: 0, startedAt: null, lastRestartAt: null, alerts: [],
    });
    spawnManaged(m.name);
});

// ── Graceful shutdown ─────────────────────────────────────────
function shutdownAll() {
    _shuttingDown = true;
    processRegistry.forEach((r, name) => {
        if (r.proc) {
            console.log(`[MANAGER] Stopping ${name} pid=${r.proc.pid}`);
            try { r.proc.kill(); } catch (_e) {}
        }
    });
}
process.on('SIGINT',  () => { shutdownAll(); process.exit(0); });
process.on('SIGTERM', () => { shutdownAll(); process.exit(0); });
