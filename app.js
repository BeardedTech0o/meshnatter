const ONLINE_MS = 30 * 60 * 1000;

const S = {
  ws:null, connected:false, myNodeNum:null,
  nodes:{}, channels:{}, conversations:{}, activeTab:null, unread:{},
  msgIndex:{},
  filter:'all', startTime:null, selectedNode:null,
  uptimeTimer:null, wsTimer:null,
};

// ── WEBSOCKET ──────────────────────────────────────────────────────
function getWSPort() {
  // When running as Electron app, port is passed via additionalArguments
  // With sandbox:true, window.process may not be available
  // Check multiple sources
  try {
    // Electron exposes additionalArguments via process.argv in renderer
    const argv = (typeof process !== 'undefined' && process.argv) ? process.argv :
                 (window.process?.argv) ? window.process.argv : [];
    for (const a of argv) {
      if (typeof a === 'string') {
        const m = a.match(/--ws-port=(\d+)/);
        if (m) return parseInt(m[1]);
      }
    }
  } catch {}
  // Fallback for HTTP mode (dev)
  if (location.port) return parseInt(location.port);
  return 3000;
}

function connectWS() {
  if (S.ws) try { S.ws.close(); } catch {}
  const wsPort = getWSPort();
  S.ws = new WebSocket('ws://127.0.0.1:' + wsPort);
  S.ws.onopen = () => {
    clearInterval(S.wsTimer);
    S.wsTimer = setInterval(() => { try { S.ws.send(JSON.stringify({type:'ping'})); } catch {} }, 20000);
    sysMsg('Connected to Meshnatter server');
  };
  S.ws.onclose = () => { clearInterval(S.wsTimer); sysMsg('Reconnecting...'); setTimeout(connectWS, 2000); };
  S.ws.onerror = () => {};
  S.ws.onmessage = e => {
    try {
      // Only accept messages from our local server (already enforced by OS since
      // the WS only binds to 127.0.0.1, but validate the data shape too)
      const msg = JSON.parse(e.data);
      if (typeof msg !== 'object' || typeof msg.type !== 'string') return;
      handle(msg);
    } catch(ex) { console.error('[ws] Message parse error:', ex); }
  };
}

document.addEventListener('visibilitychange', () => {
  if (!document.hidden && S.ws?.readyState !== WebSocket.OPEN) setTimeout(connectWS, 500);
});

function wsSend(o) { if(S.ws?.readyState===1) S.ws.send(JSON.stringify(o)); }

function isOnline(n) {
  if (n.num===S.myNodeNum) return true;
  if (!n.lastHeard) return false;
  return (Date.now()-new Date(n.lastHeard).getTime()) < ONLINE_MS;
}

// ── MESSAGE HANDLER ────────────────────────────────────────────────
function handle(msg) {
  switch(msg.type) {
    case 'pong': break;

    case 'connected':
      S.connected=true; S.startTime=Date.now();
      setConnUI(true, msg.ip);
      { const sb=document.getElementById('sendBtn'); if(sb) sb.disabled=false; }
      { const ci=document.getElementById('compInp'); if(ci) ci.disabled=false; }
      startUptime();
      toast('Connected to '+msg.ip, 'ok');
      break;

    case 'reconnecting':
      setConnUI('reconnecting', 'Reconnecting...');
      break;

    case 'disconnected': case 'connectError':
      S.connected=false;
      setConnUI(false, msg.type==='connectError' ? msg.error : 'Disconnected');
      { const sb=document.getElementById('sendBtn'); if(sb) sb.disabled=true; }
      { const ci=document.getElementById('compInp'); if(ci) ci.disabled=true; }
      clearInterval(S.uptimeTimer);
      { const ut=document.getElementById('uptimeText'); if(ut) ut.textContent=''; }
      if(msg.type==='connectError') toast(msg.error,'err');
      break;

    case 'myInfo':
      S.myNodeNum=msg.myNodeNum;
      sysMsg('Your node: '+numToId(msg.myNodeNum));
      break;

    case 'channel':
      S.channels[msg.channel.index]=msg.channel;
      ensureChTab(msg.channel); renderChatList();
      if(activeRightTab==='channels') renderChannelsList();
      bumpChannelBadge();
      break;

    case 'node':
      S.nodes[msg.node.num]={...(S.nodes[msg.node.num]||{}), ...msg.node};
      renderChatList(); renderStats(); renderMap();
      if(S.selectedNode===msg.node.num) renderNodeDetail(msg.node.num);
      break;

    case 'lastHeard':
      if(S.nodes[msg.fromNum]){
        S.nodes[msg.fromNum].lastHeard=msg.ts;
        renderChatList(); renderStats();
      }
      break;

    case 'signal':
      if(S.nodes[msg.fromNum]){
        if(msg.rxRssi!=null) S.nodes[msg.fromNum].rssi=msg.rxRssi;
        if(msg.rxSnr!=null)  S.nodes[msg.fromNum].snr=msg.rxSnr;
        if(msg.hopsAway!=null) S.nodes[msg.fromNum].hopsAway=msg.hopsAway;
        renderStats();
        if(S.selectedNode===msg.fromNum) renderNodeDetail(msg.fromNum);
      }
      break;

    case 'position':
      if(S.nodes[msg.fromNum]){
        S.nodes[msg.fromNum].lat=msg.lat;
        S.nodes[msg.fromNum].lon=msg.lon;
        S.nodes[msg.fromNum].alt=msg.alt;
        renderMap(); renderChatList();
        if(S.selectedNode===msg.fromNum) renderNodeDetail(msg.fromNum);
      }
      break;

    case 'telemetry':
      if(S.nodes[msg.fromNum]){
        if(msg.batteryLevel!=null) S.nodes[msg.fromNum].battery=msg.batteryLevel;
        if(msg.voltage!=null)      S.nodes[msg.fromNum].voltage=msg.voltage;
        if(msg.chUtil!=null)       S.nodes[msg.fromNum].chUtil=msg.chUtil;
        if(msg.airUtil!=null)      S.nodes[msg.fromNum].airUtil=msg.airUtil;
        if(msg.rxRssi!=null)       S.nodes[msg.fromNum].rssi=msg.rxRssi;
        if(msg.rxSnr!=null)        S.nodes[msg.fromNum].snr=msg.rxSnr;
        renderStats();
        if(S.selectedNode===msg.fromNum) renderNodeDetail(msg.fromNum);
      }
      break;

    case 'nodeUser':
      if(S.nodes[msg.fromNum]){
        if(msg.longName)  S.nodes[msg.fromNum].name=msg.longName;
        if(msg.shortName) S.nodes[msg.fromNum].shortName=msg.shortName;
        renderChatList();
      }
      break;

    case 'message': {
      const isDM=msg.isDM && msg.toNum===S.myNodeNum;
      const tabId=isDM ? 'dm:'+msg.fromNum : 'ch:'+msg.channel;
      if(isDM) ensureDMTab(msg.fromNum);
      pushConv(tabId, {
        id:msg.id, fromNum:msg.fromNum, fromId:msg.fromId,
        fromName:S.nodes[msg.fromNum]?.name||msg.fromId,
        text:msg.text, ts:new Date(), mine:false,
        rxRssi:msg.rxRssi, rxSnr:msg.rxSnr,
        hops:(msg.hopStart!=null&&msg.hopLimit!=null)?msg.hopStart-msg.hopLimit:null
      });
      if(document.hidden||S.activeTab!==tabId){
        try{ new Notification(S.nodes[msg.fromNum]?.name||'Mesh',{body:msg.text}); }catch{}
      }
      break;
    }

    case 'messageSent': {
      const tabId=msg.destinationNum?'dm:'+msg.destinationNum:'ch:'+(msg.channelIndex||0);
      const idx=pushConv(tabId, {
        fromNum:S.myNodeNum, fromName:'You',
        text:msg.text, ts:new Date(), mine:true,
        packetId:msg.packetId, status:'sent'
      });
      S.msgIndex[msg.packetId]={tabId, idx};
      break;
    }

    case 'ack': {
      const ref=S.msgIndex[msg.requestId];
      if(ref){
        const conv=S.conversations[ref.tabId];
        if(conv&&conv[ref.idx]){
          const m=conv[ref.idx];
          const via=S.nodes[msg.fromNum]?.name||msg.fromId;
          if(msg.ackStatus==='ack'){
            m.status='ack'; m.statusVia=via;
            toast('Delivered via '+via,'ok');
          } else {
            m.status='nack'; m.statusError=msg.errorCode;
            // Map error codes to readable labels
            const errLabels={
              'NO_ROUTE':'No route to node',
              'TIMEOUT':'Timed out',
              'NO_INTERFACE':'No radio interface',
              'MAX_RETRANSMIT':'Max retransmissions reached',
              'BAD_REQUEST':'Bad request',
              'NOT_AUTHORIZED':'Not authorised',
              'PKT_TOO_LARGE':'Packet too large',
            };
            const errMsg=errLabels[msg.errorCode]||msg.errorCode||'Unknown error';
            m.statusLabel=errMsg;
            toast(errMsg,'err');
          }
          if(S.activeTab===ref.tabId) renderMessages();
        }
      }
      break;
    }

    case 'dbReady':
      sysMsg('Mesh ready');
      if(!Object.keys(S.channels).length){
        S.channels[0]={index:0,name:'Primary',role:'PRIMARY'};
        ensureChTab(S.channels[0]); renderChatList();
      }
      break;

    case 'sendError': toast(msg.error,'err'); break;
    case 'log': console.log('[server]',msg.msg); break;
  }
}

// ── FILTER ────────────────────────────────────────────────────────
function setFilter(f){
  S.filter=f;
  document.getElementById('ftAll').classList.toggle('active',f==='all');
  document.getElementById('ftOnline').classList.toggle('active',f==='online');
  renderChatList();
}

// ── CONVERSATIONS ─────────────────────────────────────────────────
function ensureChTab(ch){
  const id='ch:'+ch.index;
  if(!S.conversations[id]){S.conversations[id]=[];S.unread[id]=0;}
  if(!S.activeTab) setActiveTab(id);
}
function ensureDMTab(num){
  const id='dm:'+num;
  if(!S.conversations[id]){S.conversations[id]=[];S.unread[id]=0;}
  return id;
}
function pushConv(tabId,m){
  if(!S.conversations[tabId]){S.conversations[tabId]=[];S.unread[tabId]=0;}
  const idx=S.conversations[tabId].length;
  S.conversations[tabId].push(m);
  if(S.conversations[tabId].length>500) S.conversations[tabId].shift();
  if(S.activeTab!==tabId) {
    S.unread[tabId]=(S.unread[tabId]||0)+1;
    // Bump right panel badge
    if(!m.sys) bumpMessageBadge();
  }
  renderChatList();
  if(S.activeTab===tabId) renderMessages();
  // Refresh channels tab if open
  if(activeRightTab==='channels') renderChannelsList();
  return idx;
}
function setActiveTab(id){
  S.activeTab=id; S.unread[id]=0;
  renderChatList(); renderMessages(); updateHeader();
}
function openDMWith(num){
  ensureDMTab(num); renderChatList(); setActiveTab('dm:'+num);
  document.getElementById('compInp').focus();
}
function sysMsg(text){
  const keys=Object.keys(S.conversations);
  const id=keys.length?keys[0]:'ch:0';
  if(!S.conversations[id]) S.conversations[id]=[];
  S.conversations[id].push({sys:true,text,ts:new Date()});
  if(S.activeTab===id) renderMessages();
}

// ── CHAT LIST ─────────────────────────────────────────────────────
function renderChatList(){
  const el=document.getElementById('chatList');
  const allNodes=Object.values(S.nodes);
  const onlineCount=allNodes.filter(isOnline).length;
  const filtered=S.filter==='online'?allNodes.filter(isOnline):allNodes;

  // Sort nodes: my node first, then by lastHeard descending (most recent first), then never-heard last
  const sorted=[...filtered].sort((a,b)=>{
    if(a.num===S.myNodeNum) return -1;
    if(b.num===S.myNodeNum) return 1;
    const ta=a.lastHeard?new Date(a.lastHeard).getTime():0;
    const tb=b.lastHeard?new Date(b.lastHeard).getTime():0;
    return tb-ta;
  });

  let html='';

  // Channels
  const chans=Object.values(S.channels).sort((a,b)=>a.index-b.index);
  if(chans.length){
    html+='<div class="list-hdr">Channels</div>';
    chans.forEach(ch=>{
      const id='ch:'+ch.index;
      const msgs=(S.conversations[id]||[]).filter(m=>!m.sys);
      const last=msgs[msgs.length-1];
      const unread=S.unread[id]||0;
      const active=S.activeTab===id;
      const preview=last?(last.mine?'You: '+esc(last.text):esc(last.fromName||'?')+': '+esc(last.text)):'No messages yet';
      const tstr=last?timeStr(last.ts):'';
      html+=`<div class="chat-row${active?' active':''}" onclick="setActiveTab('${id}')">
        <div class="avatar ch-av"><span class="icon sm">campaign</span></div>
        <div class="row-info">
          <div class="row-name">${esc(ch.name)}<span style="font-size:10px;color:var(--label-3);font-weight:400">${ch.role==='PRIMARY'?'Primary':''}</span></div>
          <div class="row-preview">${last&&last.mine?tickPreview(last.status):''}${preview}</div>
        </div>
        <div class="row-meta">
          <span class="row-time">${tstr}</span>
          ${unread?`<span class="unread-badge">${unread}</span>`:''}
        </div>
      </div>`;
    });
  }

  // Nodes
  if(sorted.length){
    html+=`<div class="list-hdr">Nodes <span class="list-hdr-count">${onlineCount} online / ${allNodes.length}</span></div>`;
    sorted.forEach(n=>{
      const isMe=n.num===S.myNodeNum;
      const online=isOnline(n);
      const id='dm:'+n.num;
      const msgs=(S.conversations[id]||[]).filter(m=>!m.sys);
      const last=msgs[msgs.length-1];
      const unread=S.unread[id]||0;
      const selActive=S.selectedNode===n.num;
      const sc=sigColor(n.rssi);
      const preview=last?(last.mine?'You: '+esc(last.text):esc(last.text)):
        (n.rssi!=null?`${n.rssi} dBm${n.hopsAway!=null?' · '+n.hopsAway+' hop'+(n.hopsAway!==1?'s':''):''}`:
        (n.lastHeard?timeAgo(n.lastHeard):'No data'));
      const tstr=n.lastHeard?timeStr(new Date(n.lastHeard)):'';
      html+=`<div class="chat-row${selActive?' active':''}" onclick="selectNodeRow(${n.num})">
        <div class="avatar node-av${isMe?' me-av':''}${online&&!isMe?' online-ring':''}${online?'':' offline-av'}">${esc(n.shortName||'???')}</div>
        <div class="row-info">
          <div class="row-name">${esc(n.name||n.id)}${isMe?'<span class="you-badge">you</span>':''}</div>
          <div class="row-preview">
            <div class="sig-dot" style="background:${sc};${online?'box-shadow:0 0 4px '+sc:'opacity:.4'}"></div>
            ${last&&last.mine?tickPreview(last.status):''}${preview}
          </div>
        </div>
        <div class="row-meta">
          <span class="row-time">${tstr}</span>
          ${unread?`<span class="unread-badge">${unread}</span>`:''}
        </div>
      </div>`;
    });
  } else if(!allNodes.length){
    html+='<div style="padding:24px 16px;text-align:center;color:var(--label-3);font-size:13px;line-height:1.6">Connect to your node to see the mesh</div>';
  } else if(S.filter==='online'){
    html+=`<div style="padding:16px;text-align:center;color:var(--label-3);font-size:13px">${allNodes.length} node${allNodes.length!==1?'s':''} in mesh — switch to All to see them</div>`;
  }

  el.innerHTML=html;
}

function tickPreview(status){
  if(!status||status==='pending') return '<span class="icon sm" style="color:var(--label-3);font-size:12px">schedule</span> ';
  if(status==='sent')   return '<span class="icon sm" style="color:var(--label-3);font-size:12px">done</span> ';
  if(status==='ack')    return '<span class="icon sm" style="color:var(--tick-blue);font-size:12px">done_all</span> ';
  if(status==='nack')   return '<span class="icon sm" style="color:var(--red);font-size:12px">error_outline</span> ';
  return '';
}

// ── CHAT HEADER ───────────────────────────────────────────────────
function updateHeader(){
  const nameEl=document.getElementById('chName');
  const subEl=document.getElementById('chSub');
  const aviEl=document.getElementById('chAvi');
  const destLabel=document.getElementById('composeDestLabel');

  if(!S.activeTab){
    nameEl.textContent='Select a conversation'; destLabel.textContent='No conversation selected'; return;
  }
  if(S.activeTab.startsWith('ch:')){
    const idx=parseInt(S.activeTab.slice(3));
    const ch=S.channels[idx];
    nameEl.textContent=ch?ch.name:'Channel '+idx;
    aviEl.innerHTML='<span class="icon" style="color:var(--blue)">campaign</span>';
    destLabel.textContent=(ch?ch.name:'Channel '+idx)+' — all nodes';
    subEl.innerHTML=document.getElementById('statBar')?document.getElementById('statBar').outerHTML:'<div class="stat-bar" id="statBar"></div>';
  } else {
    const num=parseInt(S.activeTab.slice(3));
    const n=S.nodes[num];
    nameEl.textContent=n?n.name:numToId(num);
    aviEl.innerHTML='<span class="icon" style="color:var(--label-3)">person</span>';
    destLabel.textContent=(n?n.name:numToId(num))+' — direct message';
    const online=n&&isOnline(n);
    subEl.textContent=n?(online?'Online now':'Last seen '+(n.lastHeard?timeAgo(n.lastHeard):'unknown')):numToId(num);
  }
}

// ── MESSAGES ──────────────────────────────────────────────────────
function renderMessages(){
  const feed=document.getElementById('msgFeed');
  const msgs=S.activeTab?(S.conversations[S.activeTab]||[]):[];

  if(!msgs.length){
    let title='', sub='';
    if(S.activeTab?.startsWith('ch:')){
      const idx=parseInt(S.activeTab.slice(3));
      title=S.channels[idx]?.name||'Channel'; sub='No messages yet';
    } else if(S.activeTab?.startsWith('dm:')){
      const num=parseInt(S.activeTab.slice(3));
      title=S.nodes[num]?.name||numToId(num); sub='No messages — say something!';
    } else {
      title='Meshnatter'; sub='Connect to get started';
    }
    feed.innerHTML=`<div class="msg-empty"><span class="icon xl">forum</span><div class="msg-empty-title">${esc(title)}</div><div class="msg-empty-sub">${esc(sub)}</div></div>`;
    return;
  }

  let html='', lastDate='';
  msgs.forEach(m=>{
    const d=m.ts.toLocaleDateString([],{weekday:'long',month:'long',day:'numeric'});
    if(d!==lastDate){ html+=`<div class="date-div">${d}</div>`; lastDate=d; }
    const t=m.ts.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});

    if(m.sys){ html+=`<div class="msg sys-msg"><div class="bubble"><div class="bubble-text">${esc(m.text)}</div></div></div>`; return; }

    const cls=m.mine?'out':'in';
    const st=m.status||'sent';

    // Tick icon
    let tickHtml='';
    if(m.mine){
      if(st==='pending') tickHtml=`<span class="ticks pending"><span class="icon sm">schedule</span></span>`;
      else if(st==='sent') tickHtml=`<span class="ticks sent"><span class="icon sm">done</span></span>`;
      else if(st==='ack')  tickHtml=`<span class="ticks ack"><span class="icon sm">done_all</span></span>`;
      else if(st==='nack') tickHtml=`<span class="ticks nack"><span class="icon sm">error_outline</span></span>`;
    }

    // Status line below bubble
    let statusLine='';
    if(m.mine){
      const labels={
        pending:`<span class="icon sm">schedule</span> Sending…`,
        sent:   `<span class="icon sm">done</span> Sent`,
        ack:    `<span class="icon sm">done_all</span> Delivered`+(m.statusVia?' via '+esc(m.statusVia):''),
        nack:   `<span class="icon sm">error_outline</span> `+(m.statusLabel||m.statusError||'Failed'),
        relayed:`<span class="icon sm">sync</span> Relayed`,
      };
      statusLine=`<div class="msg-status-line ${st}">${labels[st]||''}</div>`;
    }

    // Incoming signal info
    let sigLine='';
    if(!m.mine && (m.rxRssi!=null||m.hops!=null)){
      const parts=[];
      if(m.hops!=null) parts.push(m.hops+' hop'+(m.hops!==1?'s':''));
      if(m.rxRssi!=null) parts.push(m.rxRssi+' dBm');
      if(m.rxSnr!=null) parts.push(m.rxSnr.toFixed(1)+' dB SNR');
      sigLine=`<div class="bubble-sig">${parts.join(' · ')}</div>`;
    }

    html+=`<div class="msg ${cls}">
      <div class="bubble">
        ${!m.mine?`<span class="bubble-sender">${esc(m.fromName||m.fromId||'?')}</span>`:''}
        <div class="bubble-text">${esc(m.text)}</div>
        <div class="bubble-footer">
          <span class="bubble-time">${t}</span>
          ${tickHtml}
        </div>
      </div>
      ${statusLine}
      ${sigLine}
    </div>`;
  });

  feed.innerHTML=html;
  feed.scrollTop=feed.scrollHeight;
}

function sendMsg(){
  const inp=document.getElementById('compInp');
  const text=inp.value.trim();
  if(!text||!S.connected||!S.activeTab) return;
  let destinationNum=null, channelIndex=0;
  if(S.activeTab.startsWith('ch:')) channelIndex=parseInt(S.activeTab.slice(3));
  else destinationNum=parseInt(S.activeTab.slice(3));
  wsSend({type:'sendMessage',text,destinationNum,channelIndex});
  inp.value=''; inp.focus();
}

// ── STATS ─────────────────────────────────────────────────────────
function renderStats(){
  const nodes=Object.values(S.nodes);
  const online=nodes.filter(isOnline).length;
  const ws=nodes.filter(n=>n.rssi!=null);
  const snr=ws.length?(ws.reduce((s,n)=>s+(n.snr||0),0)/ws.length).toFixed(1):'--';
  const rssi=ws.length?Math.round(ws.reduce((s,n)=>s+n.rssi,0)/ws.length):'--';
  sv('scNodes',nodes.length); sv('scOnline',online); sv('scSnr',snr); sv('scRssi',rssi);
}
function sv(id,v){const e=document.getElementById(id);if(e)e.textContent=v;}

// ── NODE DETAIL ───────────────────────────────────────────────────
function selectNodeRow(num){
  if(S.selectedNode===num && document.getElementById('nodeDetail').classList.contains('show')){
    closeNodeDetail(); return;
  }
  S.selectedNode=num; renderChatList(); renderNodeDetail(num);
}
function renderNodeDetail(num){
  const n=S.nodes[num]; if(!n) return;
  const isMe=n.num===S.myNodeNum;
  document.getElementById('nodeDetail').classList.add('show');
  document.getElementById('ndName').innerHTML=esc(n.name)+(isMe?' <span style="font-size:11px;color:var(--green);font-weight:500">(you)</span>':'');
  const stats=[
    {l:'RSSI',   v:n.rssi!=null?n.rssi+' dBm':'—'},
    {l:'SNR',    v:n.snr!=null?n.snr.toFixed(1)+' dB':'—'},
    {l:'Hops',   v:n.hopsAway!=null?String(n.hopsAway):(isMe?'0':'—')},
    {l:'Battery',v:n.battery!=null?n.battery+'%':'—'},
    {l:'Voltage',v:n.voltage!=null?n.voltage+'V':'—'},
    {l:'Ch Util',v:n.chUtil!=null?n.chUtil.toFixed(1)+'%':'—'},
  ];
  document.getElementById('ndGrid').innerHTML=stats.map(s=>`<div class="nd-stat"><span class="nd-sl">${s.l}</span><span class="nd-sv">${s.v}</span></div>`).join('');
  const posEl=document.getElementById('ndPos');
  posEl.innerHTML=n.lat!=null
    ?`<span class="icon sm" style="color:var(--label-3);vertical-align:middle">location_on</span> ${n.lat.toFixed(5)}, ${n.lon.toFixed(5)}${n.alt?' · '+n.alt+'m':''}`
    :`<span class="icon sm" style="color:var(--amber);vertical-align:middle">location_off</span> No GPS position`;
  document.getElementById('ndLast').textContent=n.lastHeard?'Last heard: '+timeAgo(n.lastHeard):(isMe?'Your node':'Never heard');
  document.getElementById('ndActions').innerHTML=
    `<button class="nd-btn primary" onclick="openDMWith(${n.num})"><span class="icon sm">chat</span>Message</button>`
    +(n.lat!=null?`<button class="nd-btn" onclick="flyTo(${n.num})"><span class="icon sm">my_location</span>Locate</button>`:'')
    +`<button class="nd-btn" onclick="showJson(${n.num})"><span class="icon sm">data_object</span>Raw</button>`;
}
function closeNodeDetail(){
  S.selectedNode=null;
  document.getElementById('nodeDetail').classList.remove('show');
  renderChatList();
}

// ── MAP ───────────────────────────────────────────────────────────
const map=L.map('map',{zoomControl:true,attributionControl:true}).setView([51.5,-1.5],6);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'&copy; OpenStreetMap',maxZoom:18}).addTo(map);

// Invalidate map size once fonts+layout load
window.addEventListener('load', () => setTimeout(() => map.invalidateSize(), 300));

const markers={};

function sigColor(rssi){
  if(rssi==null) return 'var(--label-4)';
  if(rssi>=-100) return 'var(--sig-ex)';
  if(rssi>=-110) return 'var(--sig-gd)';
  if(rssi>=-120) return 'var(--sig-ok)';
  return 'var(--sig-bad)';
}
function sigColorHex(rssi){
  if(rssi==null) return '#54545870';
  if(rssi>=-100) return '#30d158';
  if(rssi>=-110) return '#a4d65e';
  if(rssi>=-120) return '#ff9f0a';
  return '#ff453a';
}

function renderMap(){
  let onMap=0;
  Object.values(S.nodes).forEach(n=>{
    if(n.lat==null||n.lon==null) return;
    onMap++;
    const isMe=n.num===S.myNodeNum;
    const sel=S.selectedNode===n.num;
    const online=isOnline(n);
    const sz=sel?22:(isMe?18:14);
    // All online nodes: green filled. My node: blue filled. Offline: faded red.
    const fill=online?(isMe?'#0a84ff':'#30d158'):'#ff453a';
    const pinClass=`mesh-pin ${online?(isMe?'me-pin':'online'):'offline'}`;
    const icon=L.divIcon({
      className:'',
      html:`<div class="${pinClass}" style="width:${sz}px;height:${sz}px;background:${fill};border:2px solid ${online?(isMe?'#5ac8fa':'#34e86a'):'#ff453a'};"></div>`,
      iconSize:[sz,sz],iconAnchor:[sz/2,sz/2],tooltipAnchor:[sz/2+4,0]
    });
    if(markers[n.num]){
      markers[n.num].setLatLng([n.lat,n.lon]);
      markers[n.num].setIcon(icon);
    } else {
      const m=L.marker([n.lat,n.lon],{icon,zIndexOffset:isMe?1000:0}).addTo(map);
      // Permanent label showing node name
      const labelText=isMe?n.name+' (you)':n.name+(online?'':' · offline');
      m.bindTooltip(labelText,{
        permanent:true,
        className:'node-map-label',
        direction:'right',
        offset:[sz/2+4,0]
      });
      m.on('click',()=>selectNodeRow(n.num));
      markers[n.num]=m;
    }
    // Update label
    try{
      const t=markers[n.num].getTooltip();
      if(t) t.setContent(isMe?n.name+' (you)':n.name+(online?'':' · offline'));
    }catch{}
  });
  Object.keys(markers).forEach(num=>{if(!S.nodes[num]){map.removeLayer(markers[num]);delete markers[num];}});
  sv('mNodes',Object.keys(S.nodes).length); sv('mOnMap',onMap);
}

function flyTo(num){
  const n=S.nodes[num];
  if(!n||n.lat==null){toast('No GPS position for this node','wrn');return;}
  map.flyTo([n.lat,n.lon],Math.max(map.getZoom(),14),{duration:.8});
}

// ── CONNECT / DISCONNECT ──────────────────────────────────────────
function toggleConn(){
  if(S.connected){
    wsSend({type:'disconnect'});
    S.connected=false; S.myNodeNum=null;
    S.nodes={}; S.channels={}; S.conversations={}; S.unread={};
    S.activeTab=null; S.msgIndex={};
    Object.values(markers).forEach(m=>map.removeLayer(m));
    for(const k in markers) delete markers[k];
    setConnUI(false,'Disconnected');
    document.getElementById('sendBtn').disabled=true;
    document.getElementById('compInp').disabled=true;
    clearInterval(S.uptimeTimer);
    document.getElementById('uptimeText').textContent='';
    renderChatList(); renderStats(); renderMap(); renderMessages(); closeNodeDetail();
    updateHeader();
  } else {
    const ip=document.getElementById('nodeIp').value.trim();
    if(!ip){toast('Enter your node\'s IP address','wrn');return;}
    setConnUI('connecting','Connecting…');
    wsSend({type:'connect',ip,port:selectedPort});
  }
}

function setConnUI(state,text){
  const btn=document.getElementById('connBtn');
  const dot=document.getElementById('cDot');
  const ctxt=document.getElementById('cText');
  if(!btn||!dot||!ctxt) return;
  if(state===true){
    btn.textContent='Disconnect'; btn.classList.add('live');
    dot.className='status-dot live'; ctxt.textContent='Connected to '+text;
  } else if(state==='connecting'||state==='reconnecting'){
    btn.textContent='Cancel'; btn.classList.remove('live');
    dot.className='status-dot '+state; ctxt.textContent=text;
  } else {
    btn.textContent='Connect'; btn.classList.remove('live');
    dot.className='status-dot'; ctxt.textContent=text||'Not connected';
  }
}

function startUptime(){
  clearInterval(S.uptimeTimer);
  S.uptimeTimer=setInterval(()=>{
    if(!S.startTime) return;
    const s=Math.floor((Date.now()-S.startTime)/1000);
    const h=Math.floor(s/3600), m=Math.floor((s%3600)/60), sec=s%60;
    const el=document.getElementById('uptimeText');
    if(el) el.textContent=h?`${h}h ${m}m`:`${m}m ${sec}s`;
  },1000);
}

// ── UTILITIES ─────────────────────────────────────────────────────
function showJson(num){
  const n=S.nodes[num]; if(!n) return;
  document.getElementById('jsonPre').innerHTML=JSON.stringify(n,null,2)
    .replace(/("(?:[^"\\]|\\.)*"(?:\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,m=>{
      if(/^".*:$/.test(m)) return `<span class="k">${m}</span>`;
      if(/^"/.test(m))     return `<span class="s">${m}</span>`;
      if(/true|false/.test(m)) return `<span class="b">${m}</span>`;
      return `<span class="n">${m}</span>`;
    });
  document.getElementById('jsonOv').classList.add('show');
}

function numToId(num){ if(num==null) return '!unknown'; return '!'+(num>>>0).toString(16).padStart(8,'0'); }
function esc(t){
  // Escape all HTML special chars to prevent XSS from node names/messages
  return String(t||'')
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;')
    .replace(/'/g,'&#x27;');
}
function timeAgo(iso){
  const s=Math.floor((Date.now()-new Date(iso).getTime())/1000);
  if(s<60) return s+'s ago';
  if(s<3600) return Math.floor(s/60)+'m ago';
  if(s<86400) return Math.floor(s/3600)+'h ago';
  return Math.floor(s/86400)+'d ago';
}
function timeStr(d){
  if(!d) return '';
  const now=new Date(), td=new Date(d);
  if(td.toDateString()===now.toDateString()) return td.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  if(now-td<7*86400000) return td.toLocaleDateString([],{weekday:'short'});
  return td.toLocaleDateString([],{day:'numeric',month:'short'});
}
function toast(msg,type='inf'){
  const el=document.createElement('div'); el.className='toast '+type;
  const icons={ok:'check_circle',err:'error',wrn:'warning',inf:'info'};
  const colors={ok:'var(--green)',err:'var(--red)',wrn:'var(--amber)',inf:'var(--blue)'};
  el.innerHTML=`<span class="icon sm" style="color:${colors[type]||colors.inf}">${icons[type]||'info'}</span>${esc(msg)}`;
  document.getElementById('toasts').appendChild(el);
  setTimeout(()=>{el.style.transition='opacity .3s';el.style.opacity='0';},3500);
  setTimeout(()=>el.remove(),3900);
}

// Notification permission
if('Notification' in window && Notification.permission==='default') Notification.requestPermission();

document.addEventListener('keydown',e=>{
  if(e.key==='Escape') document.querySelectorAll('.ov.show').forEach(o=>o.classList.remove('show'));
});



// ── RIGHT PANEL TABS ──────────────────────────────────────────
let activeRightTab = 'messages';

function switchRightTab(tab) {
  activeRightTab = tab;
  // Update tab buttons
  document.getElementById('tabBtnMessages').classList.toggle('active', tab === 'messages');
  document.getElementById('tabBtnChannels').classList.toggle('active', tab === 'channels');
  // Show/hide panes
  document.getElementById('tabPaneMessages').classList.toggle('active', tab === 'messages');
  document.getElementById('tabPaneChannels').classList.toggle('active', tab === 'channels');
  // Clear badge when switching to that tab
  if (tab === 'channels') {
    document.getElementById('tabBadgeChannels').style.display = 'none';
    document.getElementById('tabBadgeChannels').textContent = '0';
    renderChannelsList();
  }
  if (tab === 'messages') {
    document.getElementById('tabBadgeMessages').style.display = 'none';
    document.getElementById('tabBadgeMessages').textContent = '0';
  }
}

function bumpChannelBadge() {
  if (activeRightTab !== 'channels') {
    const b = document.getElementById('tabBadgeChannels');
    const n = (parseInt(b.textContent) || 0) + 1;
    b.textContent = n;
    b.style.display = 'inline-block';
  }
}

function bumpMessageBadge() {
  if (activeRightTab !== 'messages') {
    const b = document.getElementById('tabBadgeMessages');
    const n = (parseInt(b.textContent) || 0) + 1;
    b.textContent = n;
    b.style.display = 'inline-block';
  }
}

function renderChannelsList() {
  const el = document.getElementById('channelsList');
  const chans = Object.values(S.channels).sort((a,b) => a.index - b.index);
  
  if (!chans.length) {
    el.innerHTML = `<div class="ch-empty"><span class="icon">hub</span><div>No channels discovered yet.<br>Connect to your node to load channels.</div></div>`;
    return;
  }

  el.innerHTML = chans.map(ch => {
    const tabId = 'ch:' + ch.index;
    const msgs = (S.conversations[tabId] || []).filter(m => !m.sys);
    const last = msgs[msgs.length - 1];
    const unread = S.unread[tabId] || 0;
    const active = S.activeTab === tabId;
    const nodeCount = Object.values(S.nodes).length;
    const onlineCount = Object.values(S.nodes).filter(isOnline).length;
    const lastPreview = last
      ? `${last.mine ? 'You' : esc(last.fromName || '?')}: ${esc(last.text)}`
      : 'No messages yet';
    const lastTime = last ? timeStr(last.ts) : '';
    const isPrimary = ch.role === 'PRIMARY';
    return `<div class="channel-row ${active ? 'active' : ''}" onclick="openChannel('${tabId}')">
      <div class="ch-row-icon">
        <span class="icon">${isPrimary ? 'campaign' : 'hub'}</span>
      </div>
      <div class="ch-row-info">
        <div class="ch-row-name">${esc(ch.name)} ${isPrimary ? '<span style="font-size:10px;color:var(--blue);font-weight:400">PRIMARY</span>' : ''}</div>
        <div class="ch-row-sub">${nodeCount} node${nodeCount !== 1 ? 's' : ''} · ${onlineCount} online · ${lastPreview}</div>
      </div>
      <div class="ch-row-meta">
        <span style="font-size:11px;color:var(--label-3)">${lastTime}</span>
        ${unread ? `<span class="ch-row-badge">${unread}</span>` : ''}
      </div>
    </div>`;
  }).join('');
}

function openChannel(tabId) {
  setActiveTab(tabId);
  switchRightTab('messages');
  document.getElementById('compInp').focus();
}

// Port selection
let selectedPort = 80;
function setPort(p) { selectedPort = p; }

// Init — wait for DOM to be fully parsed before touching any elements
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init(); // already parsed
}

function init() {
  renderChatList();
  renderMessages();
  // Slight delay so ALL DOMContentLoaded listeners (incl. layout engine) fire first
  setTimeout(connectWS, 50);
}
