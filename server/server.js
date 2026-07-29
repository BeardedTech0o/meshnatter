/**
 * MeshSense server — HTTP polling for Heltec V3
 * ES module source — bundled to CJS by esbuild before packaging
 */
import { createServer } from 'http';


import { WebSocketServer } from 'ws';
import { fromBinary, toBinary, create } from '@bufbuild/protobuf';
import { Protobuf } from '@meshtastic/core';


const { Mesh, Portnums, Telemetry, Channel: ChannelPb, Admin, Config: ConfigNs } = Protobuf;
const { FromRadioSchema, ToRadioSchema, MeshPacketSchema, PositionSchema, UserSchema, RoutingSchema, Routing_Error } = Mesh;
const { PortNum } = Portnums;
const { TelemetrySchema } = Telemetry;
const { AdminMessageSchema, AdminMessage_ConfigType } = Admin;
const {
  ConfigSchema,
  Config_DeviceConfigSchema,
  Config_DeviceConfig_Role,
  Config_DeviceConfig_RebroadcastMode,
  Config_DeviceConfig_BuzzerMode,
} = ConfigNs;

const SERVER_PORT = parseInt(process.env.MESHNATTER_PORT || '3000');
const POLL_MS = 400;
const HEARTBEAT_MS = 8000;
const MAX_POLL_ERRORS = 5;
const RECONNECT_MS = 5000;

// Minimal HTTP server — only used for the TCP readiness check
// index.html is loaded directly by Electron as a local file
const httpServer = createServer((req, res) => {
  res.writeHead(200); res.end('Meshnatter server running');
});

const wss = new WebSocketServer({ server: httpServer });

let nodeHost = null, nodePort = 80, pollTimer = null, heartbeatTimer = null;
let active = false, polling = false, pollErrors = 0, reconnectTimer = null;
// Admin / device-config state
let myNodeNum = null;              // learned from the myInfo FromRadio frame
let sessionPasskey = null;         // echoed back by the node on admin responses
let lastDeviceConfig = null;       // last known DeviceConfig (needed for partial writes)
let configRefreshTimer = null;

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}
function log(msg) { console.log(`[${new Date().toTimeString().slice(0,8)}] ${msg}`); broadcast({ type: 'log', msg }); }
function nodeBaseUrl() { return `http://${nodeHost}${nodePort !== 80 ? ':'+nodePort : ''}`; }
function numToId(num) { return '!' + ((num >>> 0)).toString(16).padStart(8, '0'); }

async function httpGet(url, timeout = 6000) {
  const res = await fetch(url, { headers: { 'Accept': 'application/x-protobuf' }, signal: AbortSignal.timeout(timeout) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return new Uint8Array(await res.arrayBuffer());
}
async function httpPut(url, body, timeout = 6000) {
  const res = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/x-protobuf' }, body, signal: AbortSignal.timeout(timeout) });
  if (!res.ok) throw new Error(`HTTP PUT ${res.status}`);
}

async function sendWantConfig() {
  const configId = Math.floor(Math.random() * 0xFFFFFF);
  const toRadio = create(ToRadioSchema, { payloadVariant: { case: 'wantConfigId', value: configId } });
  await httpPut(`${nodeBaseUrl()}/api/v1/toradio`, toBinary(ToRadioSchema, toRadio));
  log(`wantConfig sent (id=${configId})`);
}
async function sendHeartbeat() {
  if (!active) return;
  try { await httpPut(`${nodeBaseUrl()}/api/v1/toradio`, toBinary(ToRadioSchema, create(ToRadioSchema, { payloadVariant: { case: 'heartbeat', value: {} } })), 4000); } catch {}
}

// ── DEVICE CONFIG (AdminMessage over the same ToRadio HTTP endpoint) ──────
// Enum name lists are derived from the protobuf descriptors so the UI never
// drifts from the @meshtastic/core version we actually ship.
function enumOptions(enumObj) {
  return Object.entries(enumObj)
    .filter(([k]) => /^\d+$/.test(k))
    .map(([k, v]) => ({ value: Number(k), name: String(v) }))
    .sort((a, b) => a.value - b.value);
}
const CONFIG_ENUMS = {
  role: enumOptions(Config_DeviceConfig_Role),
  rebroadcastMode: enumOptions(Config_DeviceConfig_RebroadcastMode),
  buzzerMode: enumOptions(Config_DeviceConfig_BuzzerMode),
};

// Whitelist of writable DeviceConfig fields — anything else from the renderer
// is ignored. Keeps the set_config payload well-formed.
const DEVICE_FIELDS = {
  role: 'int', rebroadcastMode: 'int', buzzerMode: 'int',
  buttonGpio: 'int', buzzerGpio: 'int', nodeInfoBroadcastSecs: 'int',
  serialEnabled: 'bool', doubleTapAsButtonPress: 'bool', isManaged: 'bool',
  disableTripleClick: 'bool', ledHeartbeatDisabled: 'bool',
  tzdef: 'string',
};

function deviceConfigToJson(dev) {
  const out = {};
  for (const key of Object.keys(DEVICE_FIELDS)) {
    const v = dev?.[key];
    if (DEVICE_FIELDS[key] === 'int') out[key] = Number(v ?? 0);
    else if (DEVICE_FIELDS[key] === 'bool') out[key] = !!v;
    else out[key] = String(v ?? '');
  }
  return out;
}

function publishDeviceConfig(dev, source) {
  lastDeviceConfig = deviceConfigToJson(dev);
  broadcast({ type: 'deviceConfig', source, config: lastDeviceConfig, enums: CONFIG_ENUMS });
}

async function sendAdmin(variantCase, variantValue, wantResponse = false) {
  if (!active) throw new Error('Not connected to a node');
  if (myNodeNum == null) throw new Error('Node identity not known yet — wait for the mesh to finish loading');
  const admin = create(AdminMessageSchema, {
    ...(sessionPasskey?.length ? { sessionPasskey } : {}),
    payloadVariant: { case: variantCase, value: variantValue },
  });
  const packetId = Math.floor(Math.random() * 0x7ffffffe) + 1;
  const mp = create(MeshPacketSchema, {
    id: packetId,
    to: myNodeNum,
    channel: 0,
    decoded: {
      portnum: PortNum.ADMIN_APP,
      payload: toBinary(AdminMessageSchema, admin),
      wantResponse,
    },
    wantAck: false,
    hopLimit: 0,
  });
  await httpPut(`${nodeBaseUrl()}/api/v1/toradio`,
    toBinary(ToRadioSchema, create(ToRadioSchema, { payloadVariant: { case: 'packet', value: mp } })));
  return packetId;
}

async function requestDeviceConfig() {
  await sendAdmin('getConfigRequest', AdminMessage_ConfigType.DEVICE_CONFIG, true);
  log('Device config requested');
}

async function setDeviceConfig(patch) {
  if (!patch || typeof patch !== 'object') throw new Error('No config supplied');
  // set_config replaces the whole DeviceConfig submessage, so merge onto what
  // we last read from the node rather than sending a sparse message.
  const merged = { ...(lastDeviceConfig || {}) };
  for (const [key, kind] of Object.entries(DEVICE_FIELDS)) {
    if (!(key in patch)) continue;
    const raw = patch[key];
    if (kind === 'int') {
      const n = Number(raw);
      if (!Number.isFinite(n) || n < 0 || n > 0xffffffff) throw new Error(`Invalid value for ${key}`);
      merged[key] = Math.floor(n);
    } else if (kind === 'bool') {
      merged[key] = !!raw;
    } else {
      if (typeof raw !== 'string' || raw.length > 64) throw new Error(`Invalid value for ${key}`);
      merged[key] = raw;
    }
  }
  const device = create(Config_DeviceConfigSchema, merged);
  const config = create(ConfigSchema, { payloadVariant: { case: 'device', value: device } });
  await sendAdmin('beginEditSettings', true);
  await sendAdmin('setConfig', config);
  await sendAdmin('commitEditSettings', true);
  lastDeviceConfig = deviceConfigToJson(device);
  log('Device config written');
  // Read it back so the UI reflects what the node actually stored.
  clearTimeout(configRefreshTimer);
  configRefreshTimer = setTimeout(() => { requestDeviceConfig().catch(() => {}); }, 3000);
}

function handleFromRadio(bytes) {
  if (!bytes?.length) return;
  let pkt; try { pkt = fromBinary(FromRadioSchema, bytes); } catch { return; }
  if (!pkt?.payloadVariant?.case) return;
  const { case: kind, value: val } = pkt.payloadVariant;

  if (kind === 'myInfo') {
    myNodeNum = Number(val.myNodeNum);
    broadcast({ type: 'myInfo', myNodeNum });
  } else if (kind === 'nodeInfo') {
    const n = val;
    broadcast({ type: 'node', node: {
      num: Number(n.num), id: numToId(Number(n.num)),
      name: n.user?.longName || n.user?.shortName || numToId(Number(n.num)),
      shortName: n.user?.shortName || '???',
      hwModel: n.user?.hwModel ?? 0,
      lat: n.position?.latitudeI ? n.position.latitudeI / 1e7 : null,
      lon: n.position?.longitudeI ? n.position.longitudeI / 1e7 : null,
      alt: n.position?.altitude ?? null,
      battery: n.deviceMetrics?.batteryLevel ?? null,
      voltage: n.deviceMetrics?.voltage ? (n.deviceMetrics.voltage / 1000).toFixed(2) : null,
      chUtil: n.deviceMetrics?.channelUtilization ?? null,
      airUtil: n.deviceMetrics?.airUtilTx ?? null,
      snr: n.snr ?? null, hopsAway: n.hopsAway ?? null,
      lastHeard: n.lastHeard ? new Date(Number(n.lastHeard) * 1000).toISOString() : null,
    }});
  } else if (kind === 'config') {
    // The node streams its whole Config after wantConfig — pick up DeviceConfig
    if (val?.payloadVariant?.case === 'device') publishDeviceConfig(val.payloadVariant.value, 'stream');
  } else if (kind === 'channel') {
    const ch = val;
    if (ch.role === 0) return;
    broadcast({ type: 'channel', channel: {
      index: ch.index,
      name: ch.settings?.name || (ch.index === 0 ? 'Primary' : `Channel ${ch.index}`),
      role: ch.role === 1 ? 'PRIMARY' : 'SECONDARY',
    }});
  } else if (kind === 'packet') {
    const mp = val;
    const from = Number(mp.from), to = Number(mp.to);
    const rxRssi = mp.rxRssi ?? null, rxSnr = mp.rxSnr ?? null;
    const hopLimit = mp.hopLimit ?? null, hopStart = mp.hopStart ?? null;
    broadcast({ type: 'signal', fromNum: from, fromId: numToId(from), rxRssi, rxSnr, hopsAway: hopStart != null && hopLimit != null ? hopStart - hopLimit : null });
    broadcast({ type: 'lastHeard', fromNum: from, ts: new Date().toISOString() });
    const decoded = mp.decoded; if (!decoded) return;
    if (decoded.portnum === PortNum.TEXT_MESSAGE_APP) {
      broadcast({ type: 'message', id: Number(mp.id), fromNum: from, fromId: numToId(from), toNum: to, toId: numToId(to), channel: mp.channel ?? 0, isDM: to !== 0xFFFFFFFF, text: new TextDecoder().decode(decoded.payload), rxRssi, rxSnr, hopLimit, hopStart, rxTime: new Date().toISOString() });
    } else if (decoded.portnum === PortNum.ROUTING_APP) {
      try {
        const routing = fromBinary(RoutingSchema, decoded.payload);
        const requestId = Number(decoded.requestId || 0);
        let ackStatus = 'ack', errorCode = null;
        if (routing.variant?.case === 'errorReason') {
          const errVal = routing.variant.value;
          if (errVal !== Routing_Error.NONE && errVal !== 0) { ackStatus = 'nack'; errorCode = Routing_Error[errVal] || `ERROR_${errVal}`; }
        }
        broadcast({ type: 'ack', requestId, fromNum: from, fromId: numToId(from), ackStatus, errorCode });
      } catch {}
    } else if (decoded.portnum === PortNum.POSITION_APP) {
      try { const pos = fromBinary(PositionSchema, decoded.payload); broadcast({ type: 'position', fromNum: from, fromId: numToId(from), lat: pos.latitudeI ? pos.latitudeI / 1e7 : null, lon: pos.longitudeI ? pos.longitudeI / 1e7 : null, alt: pos.altitude ?? null }); } catch {}
    } else if (decoded.portnum === PortNum.TELEMETRY_APP) {
      try { const tel = fromBinary(TelemetrySchema, decoded.payload); const dm = tel.variant?.case === 'deviceMetrics' ? tel.variant.value : null; broadcast({ type: 'telemetry', fromNum: from, fromId: numToId(from), batteryLevel: dm?.batteryLevel ?? null, voltage: dm?.voltage ? (dm.voltage/1000).toFixed(2) : null, chUtil: dm?.channelUtilization ?? null, airUtil: dm?.airUtilTx ?? null, rxRssi, rxSnr }); } catch {}
    } else if (decoded.portnum === PortNum.ADMIN_APP) {
      try {
        const am = fromBinary(AdminMessageSchema, decoded.payload);
        if (am.sessionPasskey?.length) sessionPasskey = am.sessionPasskey;
        if (am.payloadVariant?.case === 'getConfigResponse') {
          const cfg = am.payloadVariant.value;
          if (cfg?.payloadVariant?.case === 'device') publishDeviceConfig(cfg.payloadVariant.value, 'admin');
        }
      } catch {}
    } else if (decoded.portnum === PortNum.NODEINFO_APP) {
      try { const user = fromBinary(UserSchema, decoded.payload); broadcast({ type: 'nodeUser', fromNum: from, fromId: numToId(from), longName: user.longName, shortName: user.shortName }); } catch {}
    }
  } else if (kind === 'configCompleteId') {
    log('Mesh ready'); broadcast({ type: 'dbReady' });
  }
}

async function poll() {
  if (!active || polling) return;
  polling = true;
  try {
    let i = 0;
    while (active && i < 100) { const b = await httpGet(`${nodeBaseUrl()}/api/v1/fromradio`); if (!b.length) break; handleFromRadio(b); i++; }
    pollErrors = 0;
  } catch (e) {
    if (!active) return;
    if (++pollErrors >= MAX_POLL_ERRORS) { log('Reconnecting...'); broadcast({ type: 'reconnecting' }); stopTimers(); reconnectTimer = setTimeout(attemptReconnect, RECONNECT_MS); }
  } finally { polling = false; }
}

async function attemptReconnect() {
  if (!nodeHost) return;
  try { await fetch(`${nodeBaseUrl()}/`, { signal: AbortSignal.timeout(5000) }); active = true; pollErrors = 0; await sendWantConfig(); startTimers(); broadcast({ type: 'connected', ip: nodeHost }); log('Reconnected'); }
  catch { reconnectTimer = setTimeout(attemptReconnect, RECONNECT_MS); }
}

function startTimers() { stopTimers(); pollTimer = setInterval(poll, POLL_MS); heartbeatTimer = setInterval(sendHeartbeat, HEARTBEAT_MS); }
function stopTimers() { clearInterval(pollTimer); clearInterval(heartbeatTimer); clearTimeout(reconnectTimer); pollTimer=null; heartbeatTimer=null; reconnectTimer=null; polling=false; }

async function sendText(text, destNum, channelIndex) {
  // Sanitise input
  if (typeof text !== 'string' || text.length === 0 || text.length > 237) {
    throw new Error('Message must be 1-237 characters (Meshtastic limit)');
  }
  const mp = create(MeshPacketSchema, { to: destNum ?? 0xFFFFFFFF, channel: channelIndex ?? 0, decoded: { payload: new TextEncoder().encode(text), portnum: PortNum.TEXT_MESSAGE_APP }, wantAck: true, hopLimit: 3 });
  await httpPut(`${nodeBaseUrl()}/api/v1/toradio`, toBinary(ToRadioSchema, create(ToRadioSchema, { payloadVariant: { case: 'packet', value: mp } })));
  return Number(mp.id);
}

async function doConnect(ip, port) {
  port = port || 80;
  // Validate IP/hostname — only allow valid IPs and local hostnames
  // Prevent SSRF to internal services other than Meshtastic nodes
  const ipOk = /^[a-zA-Z0-9._-]+$/.test(ip) && ip.length < 64;
  const portOk = Number.isInteger(port) && port > 0 && port < 65536;
  if (!ipOk || !portOk) {
    broadcast({ type: 'connectError', error: 'Invalid IP address or port' });
    return;
  }
  nodeHost = ip; nodePort = port; active = false; pollErrors = 0;
  const baseUrl = `http://${ip}${port !== 80 ? ':'+port : ''}`;
  log(`Connecting to ${baseUrl}...`);
  try { await fetch(`http://${ip}${port !== 80 ? ":"+port : ""}/`, { signal: AbortSignal.timeout(5000) }); }
  catch (e) { broadcast({ type: 'connectError', error: `Cannot reach ${ip} — ${e.message}` }); return; }
  await (async()=>{ try { let n=0; while(n<50){const b=await httpGet(`http://${ip}${port !== 80 ? ":"+port : ""}/api/v1/fromradio`,3000);if(!b.length)break;n++;} } catch{} })();
  active = true;
  try { await sendWantConfig(); } catch (e) { broadcast({ type: 'connectError', error: `Init failed: ${e.message}` }); active=false; return; }
  broadcast({ type: 'connected', ip });
  startTimers();
}

function doDisconnect(silent=false) {
  active=false; pollErrors=0; nodeHost=null;
  myNodeNum=null; sessionPasskey=null; lastDeviceConfig=null;
  clearTimeout(configRefreshTimer); configRefreshTimer=null;
  stopTimers();
  if(!silent) broadcast({ type: 'disconnected' });
}

wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: active ? 'connected' : 'disconnected', ip: nodeHost }));
  ws.on('message', async raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'connect') { doDisconnect(true); await doConnect(msg.ip, msg.port || 80); }
    else if (msg.type === 'disconnect') { doDisconnect(); }
    else if (msg.type === 'getDeviceConfig') {
      if (lastDeviceConfig) ws.send(JSON.stringify({ type: 'deviceConfig', source: 'cache', config: lastDeviceConfig, enums: CONFIG_ENUMS }));
      try { await requestDeviceConfig(); }
      catch (e) { ws.send(JSON.stringify({ type: 'configResult', ok: false, action: 'read', error: e.message })); }
    }
    else if (msg.type === 'setDeviceConfig') {
      try { await setDeviceConfig(msg.config); broadcast({ type: 'configResult', ok: true, action: 'write' }); }
      catch (e) { ws.send(JSON.stringify({ type: 'configResult', ok: false, action: 'write', error: e.message })); }
    }
    else if (msg.type === 'sendMessage') {
      try { const id = await sendText(msg.text, msg.destinationNum??null, msg.channelIndex??0); broadcast({ type: 'messageSent', text: msg.text, destinationNum: msg.destinationNum, channelIndex: msg.channelIndex, packetId: id, ts: new Date().toISOString() }); }
      catch (e) { ws.send(JSON.stringify({ type: 'sendError', error: e.message })); }
    }
  });
});

httpServer.listen(SERVER_PORT, '127.0.0.1', () => {
  log(`Server listening on port ${SERVER_PORT}`);
});
