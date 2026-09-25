'use strict';

/* Dhan Universal Market Data Backend v2.0
   Data plane first. Strategy remains in the PWA.
*/
const express=require('express');
const cors=require('cors');
const WebSocket=require('ws');
const webpush=require('web-push');
const crypto=require('crypto');
const app=express();
app.use(cors({origin:true,credentials:false}));
app.use(express.json({limit:'1mb'}));

const PORT=Number(process.env.PORT||3000);
const DHAN_CLIENT_ID=String(process.env.DHAN_CLIENT_ID||'').trim();
const DHAN_ACCESS_TOKEN=String(process.env.DHAN_ACCESS_TOKEN||'').trim();
const DHAN_PIN=String(process.env.DHAN_PIN||'').trim();
const DHAN_TOTP_SECRET=String(process.env.DHAN_TOTP_SECRET||'').replace(/\s+/g,'').trim();
const CLIENT_API_KEY=String(process.env.CLIENT_API_KEY||'').trim();
const INSTRUMENT_URL=String(process.env.INSTRUMENT_URL||'https://images.dhan.co/api-data/api-scrip-master.csv').trim();
const VAPID_PUBLIC_KEY=String(process.env.VAPID_PUBLIC_KEY||'').trim();
const VAPID_PRIVATE_KEY=String(process.env.VAPID_PRIVATE_KEY||'').trim();
const VAPID_SUBJECT=String(process.env.VAPID_SUBJECT||'mailto:alerts@example.com').trim();
const MAX_TICKS=Number(process.env.MAX_TICKS_PER_INSTRUMENT||3000);
const MAX_CANDLES=Number(process.env.MAX_CANDLES_PER_INSTRUMENT||2000);
const STALE_MS=Number(process.env.TICK_STALE_MS||15000);
const INDEXES={NIFTY:{securityId:'13',exchangeSegment:'IDX_I',symbol:'NIFTY 50',strikeStep:50,futSeg:'NSE_FNO',futMatch:nm=>nm==='NIFTY'||(nm.startsWith('NIFTY')&&!nm.includes('BANK')&&!nm.includes('FIN')&&!nm.includes('MID')&&!nm.includes('NEXT'))},SENSEX:{securityId:'1',exchangeSegment:'IDX_I',symbol:'SENSEX',strikeStep:100,futSeg:'BSE_FNO',futMatch:nm=>nm.startsWith('SENSEX')&&!nm.includes('BANKEX')}};
// NIFTY/SENSEX are index values: the exchange never publishes a real traded "volume" for them (nothing
// is bought/sold at the index level), so Dhan's own IDX_I feed and history return 0/meaningless volume.
// Fix: mirror volume from each index's current-month futures contract (real traded volume) into the
// index's ticks/candles, matched minute-by-minute. resolveFutures()/getLinkedFuture() below do this.
const indexKeySet=new Set(Object.values(INDEXES).map(v=>key(v.exchangeSegment,v.securityId)));
function parseExpiry(str){if(!str)return null;const t=Date.parse(str);if(Number.isFinite(t))return t;const m=String(str).match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})/);if(m){const mo={JAN:0,FEB:1,MAR:2,APR:3,MAY:4,JUN:5,JUL:6,AUG:7,SEP:8,OCT:9,NOV:10,DEC:11}[m[2].toUpperCase()];if(mo!=null)return new Date(Number(m[3]),mo,Number(m[1])).getTime()}return null}
function resolveFutures(){
 for(const cfg of Object.values(INDEXES)){
  const cands=instruments.filter(x=>x.exchangeSegment===cfg.futSeg&&String(x.instrumentType||'').toUpperCase().includes('FUT')&&cfg.futMatch(String(x.symbol||x.tradingSymbol||x.displayName||'').toUpperCase()));
  if(!cands.length)continue;
  const today=now(),withExpiry=cands.map(x=>({...x,expiryTs:parseExpiry(x.expiry)})).filter(x=>x.expiryTs&&x.expiryTs>=today-86400000).sort((a,b)=>a.expiryTs-b.expiryTs);
  const chosen=withExpiry[0]||cands[0];
  if(chosen)cfg.future={securityId:chosen.securityId,exchangeSegment:chosen.exchangeSegment,symbol:chosen.tradingSymbol||chosen.symbol,expiry:chosen.expiry}
 }
 console.log('Linked futures (volume source) resolved:',Object.fromEntries(Object.entries(INDEXES).map(([k,v])=>[k,v.future?v.future.symbol:null])));
}
function getLinkedFuture(seg,sid){for(const cfg of Object.values(INDEXES))if(cfg.exchangeSegment===seg&&cfg.securityId===String(sid))return cfg.future||null;return null}
const SEGMENT_MAP={'NSE:E':'NSE_EQ','NSE:D':'NSE_FNO','NSE:I':'IDX_I','NSE:C':'NSE_CURRENCY','BSE:E':'BSE_EQ','BSE:D':'BSE_FNO','BSE:C':'BSE_CURRENCY','BSE:I':'IDX_I','MCX:M':'MCX_COMM'};
const CODE_SEGMENT={0:'IDX_I',1:'NSE_EQ',2:'NSE_FNO',3:'NSE_CURRENCY',4:'BSE_EQ',5:'MCX_COMM',7:'BSE_CURRENCY',8:'BSE_FNO'};
const RESP={TICKER:2,QUOTE:4,OI:5,PREV_CLOSE:6,STATUS:7,FULL:8,DISCONNECT:50,INDEX:2};
function now(){return Date.now()} function key(seg,sid){return `${seg}:${sid}`} function n(v){const x=Number(v);return Number.isFinite(x)?x:null}
let token=DHAN_ACCESS_TOKEN,tokenAt=DHAN_ACCESS_TOKEN?now():0,loginPromise=null;
async function getToken(){
 if(token&&(!DHAN_PIN||now()-tokenAt<20*3600*1000))return token;
 if(!DHAN_CLIENT_ID||!DHAN_PIN||!DHAN_TOTP_SECRET)return token||null;
 if(loginPromise)return loginPromise;
 loginPromise=(async()=>{try{const {authenticator}=require('otplib');const totp=authenticator.generate(DHAN_TOTP_SECRET);const u='https://auth.dhan.co/app/generateAccessToken?dhanClientId='+encodeURIComponent(DHAN_CLIENT_ID)+'&pin='+encodeURIComponent(DHAN_PIN)+'&totp='+encodeURIComponent(totp);const r=await fetch(u,{method:'POST'});const j=await r.json().catch(()=>({}));const t=j.accessToken||j.access_token||j.token||j.accesstoken;if(t){token=t;tokenAt=now();console.log('Dhan access token refreshed.')}else console.log('Dhan token refresh returned no access token.');return token||null}catch(e){console.log('Dhan token refresh failed:',e.message);return token||null}finally{loginPromise=null}})();
 return loginPromise;
}
async function dhanPost(path,body){const t=await getToken();if(!t)throw Error('Dhan access token is not configured');const r=await fetch('https://api.dhan.co/v2'+path,{method:'POST',headers:{'Content-Type':'application/json','Accept':'application/json','access-token':t,'client-id':DHAN_CLIENT_ID},body:JSON.stringify(body)});const j=await r.json().catch(()=>({}));if(!r.ok||j.status==='failure')throw Error(j.remarks||j.message||`Dhan HTTP ${r.status}`);return j}

// Instrument master: correct Dhan compact CSV mapping, especially NSE + I -> IDX_I.
let instruments=[],byKey=new Map(),loadedAt=0,loadPromise=null;
function parseCSV(line){const a=[];let s='',q=false;for(let i=0;i<line.length;i++){const c=line[i];if(c==='"'){if(q&&line[i+1]==='"'){s+='"';i++}else q=!q}else if(c===','&&!q){a.push(s);s=''}else s+=c}a.push(s);return a}
function addInstrument(h,v){const x={};for(let i=0;i<h.length;i++)x[h[i]]=v[i]??'';const ex=String(x.SEM_EXM_EXCH_ID||'').trim().toUpperCase(),sg=String(x.SEM_SEGMENT||'').trim().toUpperCase(),sid=String(x.SEM_SMST_SECURITY_ID||'').trim();if(!sid)return;const es=SEGMENT_MAP[`${ex}:${sg}`];if(!es)return;const item={exchangeSegment:es,securityId:sid,symbol:x.SM_SYMBOL_NAME||x.SEM_CUSTOM_SYMBOL||'',tradingSymbol:x.SEM_TRADING_SYMBOL||'',displayName:x.SEM_CUSTOM_SYMBOL||'',instrument:x.SEM_INSTRUMENT_NAME||'',instrumentType:x.SEM_EXCH_INSTRUMENT_TYPE||'',underlyingSymbol:x.SEM_CUSTOM_SYMBOL||'',expiry:x.SEM_EXPIRY_DATE||'',strike:n(x.SEM_STRIKE_PRICE),optionType:x.SEM_OPTION_TYPE||'',lotSize:n(x.SEM_LOT_UNITS),tickSize:n(x.SEM_TICK_SIZE)};instruments.push(item);byKey.set(key(es,sid),item)}
async function loadInstruments(force=false){if(!force&&instruments.length&&now()-loadedAt<6*3600*1000)return; if(loadPromise)return loadPromise;loadPromise=(async()=>{try{const r=await fetch(INSTRUMENT_URL);if(!r.ok)throw Error(`Instrument master HTTP ${r.status}`);const reader=r.body?.getReader();if(!reader)throw Error('Instrument master has no streaming body');instruments=[];byKey=new Map();const dec=new TextDecoder();let carry='',h=null;for(;;){const z=await reader.read();if(z.done)break;carry+=dec.decode(z.value,{stream:true});const lines=carry.split(/\r?\n/);carry=lines.pop()||'';for(const line of lines){if(!line.trim())continue;const vals=parseCSV(line);if(!h)h=vals.map(x=>x.trim());else addInstrument(h,vals)}}carry+=dec.decode();if(carry.trim()){const vals=parseCSV(carry);if(!h)h=vals.map(x=>x.trim());else addInstrument(h,vals)}loadedAt=now();console.log('Instrument master loaded:',instruments.length);resolveFutures()}finally{loadPromise=null}})();return loadPromise}
function lookup(seg,sid){return byKey.get(key(seg,String(sid)))||null}

const market=new Map();
function state(seg,sid){const k=key(seg,String(sid));if(!market.has(k))market.set(k,{exchangeSegment:seg,securityId:String(sid),quote:{},ticks:[],candles:new Map(),depth:[],lastUpdate:0,previousClose:null});return market.get(k)}
function append(s,t){s.ticks.push(t);if(s.ticks.length>MAX_TICKS)s.ticks.splice(0,s.ticks.length-MAX_TICKS);s.lastUpdate=now()}
function candle(s,t,m){const ms=m*60000,b=Math.floor(t.ts/ms)*ms;let c=s.candles.get(b),p=s.quote.ltp??t.ltp;if(!c){c={timestamp:b,open:p,high:p,low:p,close:p,volume:t.volume??0,oi:t.oi??null,source:'live'};s.candles.set(b,c)}else{c.high=Math.max(c.high,p);c.low=Math.min(c.low,p);c.close=p;if(t.volume!=null)c.volume=t.volume;if(t.oi!=null)c.oi=t.oi}while(s.candles.size>MAX_CANDLES)s.candles.delete(s.candles.keys().next().value)}
function derived(s,t){[1,3,5,15,25,60].forEach(m=>candle(s,t,m))}

let feed=null,feedState='DISCONNECTED',feedLast=0,reconnectTimer=null,reconnectMs=1000;let packetCount=0,tickCount=0;const clients=new Set();const desired=new Map();const pushes=new Map();
const linkedVolumeTarget=new Map(); // key(futSeg,futSid) -> key(idxSeg,idxSid): where a futures tick's volume should be mirrored
function modeRank(m){return m==='full'?3:m==='quote'?2:1} function best(a,b){return modeRank(a)>=modeRank(b)?a:b} function reqCode(m){return m==='full'?21:m==='quote'?17:15}
function sendSub(mode,items){if(!feed||feed.readyState!==WebSocket.OPEN)return;for(let i=0;i<items.length;i+=100){const b=items.slice(i,i+100);feed.send(JSON.stringify({RequestCode:reqCode(mode),InstrumentCount:b.length,InstrumentList:b.map(x=>({ExchangeSegment:x.exchangeSegment,SecurityId:String(x.securityId)}))}))}}
function rebuild(){
 desired.clear();
 for(const c of clients)for(const [k,v] of c.subscriptions)desired.set(k,best(desired.get(k)||'ticker',v.mode));
 // Whenever a client subscribes to NIFTY/SENSEX, silently also subscribe (mode 'quote', for its volume
 // field) to that index's linked futures contract so we have a real volume to mirror in. The client
 // never sees this second subscription; it only ever receives ticks for the index it asked for.
 linkedVolumeTarget.clear();
 for(const k of [...desired.keys()]){
  const [seg,sid]=k.split(':'),fut=getLinkedFuture(seg,sid);
  if(fut){const fk=key(fut.exchangeSegment,fut.securityId);desired.set(fk,best(desired.get(fk)||'ticker','quote'));linkedVolumeTarget.set(fk,k)}
 }
}
function connect(){if(!DHAN_CLIENT_ID){feedState='NO_CLIENT_ID';broadcast({type:'status',feedState});return}getToken().then(t=>{if(!t){feedState='NO_TOKEN';broadcast({type:'status',feedState});return}feedState='CONNECTING';broadcast({type:'status',feedState});const u='wss://api-feed.dhan.co?version=2&token='+encodeURIComponent(t)+'&clientId='+encodeURIComponent(DHAN_CLIENT_ID)+'&authType=2';feed=new WebSocket(u);feed.binaryType='arraybuffer';feed.on('open',()=>{feedState='CONNECTED';feedLast=now();reconnectMs=1000;broadcast({type:'status',feedState});rebuild();const g={ticker:[],quote:[],full:[]};for(const [k,m] of desired){const [seg,sid]=k.split(':');g[m].push({exchangeSegment:seg,securityId:sid})}for(const m of Object.keys(g))sendSub(m,g[m]);console.log('Dhan feed connected; subscriptions:',desired.size)});feed.on('message',b=>{feedLast=now();handlePacket(Buffer.from(b))});feed.on('close',()=>{feedState='DISCONNECTED';broadcast({type:'status',feedState});schedule()});feed.on('error',e=>console.log('Dhan feed error:',e.message))}).catch(e=>{console.log('feed connect error:',e.message);schedule()})}
function schedule(){if(reconnectTimer)return;reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect()},reconnectMs);reconnectMs=Math.min(reconnectMs*2,30000)}
function header(b){if(b.length<8)return null;return{code:b.readUInt8(0),seg:CODE_SEGMENT[b.readUInt8(3)]||'',sid:String(b.readUInt32LE(4))}}
function depth5(b,o){const a=[];for(let i=0;i<5;i++){const x=o+i*20;if(x+20>b.length)break;a.push({bidQty:b.readInt32LE(x),askQty:b.readInt32LE(x+4),bidOrders:b.readInt16LE(x+8),askOrders:b.readInt16LE(x+10),bidPrice:b.readFloatLE(x+12),askPrice:b.readFloatLE(x+16)})}return a}
function normalize(h,s,f){
  tickCount++;
  const receivedAt=now();
  const rawTs=f.ltt?Number(f.ltt)*1000:receivedAt;
  const sane=Number.isFinite(rawTs)&&Math.abs(rawTs-receivedAt)<=120000;
  const ts=sane?rawTs:receivedAt;
  const t={ts,exchangeTimestamp:Number.isFinite(rawTs)?rawTs:null,timestampSource:sane?"exchange":"server-received",receivedAt,exchangeSegment:h.seg,securityId:h.sid,...f};
  s.quote={...s.quote,...f,exchangeSegment:h.seg,securityId:h.sid,lastTickTimestamp:ts};
  const selfKey=key(h.seg,h.sid);
  if(indexKeySet.has(selfKey)&&s.curMinuteVolume!=null){
   // This tick's own "volume" (if any) came straight off the IDX_I feed and isn't real traded volume;
   // keep showing the value mirrored from the linked futures contract instead (see below).
   s.quote.volume=s.curMinuteVolume;t.volume=s.curMinuteVolume;t.volumeSource='LINKED_FUTURES'
  }
  append(s,t);derived(s,t);broadcastTick(h.seg,h.sid,t);

  const idxKey=linkedVolumeTarget.get(selfKey);
  if(idxKey&&f.volume!=null){
   const cum=Number(f.volume);
   if(Number.isFinite(cum)){
    let delta=0;
    if(s.lastCumVolume!=null){delta=cum-s.lastCumVolume;if(delta<0)delta=cum} // negative delta = new session, treat as fresh count
    s.lastCumVolume=cum;
    const [iseg,isid]=idxKey.split(':'),idx=state(iseg,isid);
    const bucket=Math.floor(ts/60000)*60000;
    if(idx.curMinuteBucket!==bucket){idx.curMinuteBucket=bucket;idx.curMinuteVolume=0}
    idx.curMinuteVolume=(idx.curMinuteVolume||0)+Math.max(0,delta);
    idx.quote.volume=idx.curMinuteVolume;idx.quote.volumeSource='LINKED_FUTURES';idx.lastUpdate=now();
    if(idx.quote.ltp!=null)derived(idx,{ts,volume:idx.curMinuteVolume}); // keep /api/v1/candles volume in sync too
    broadcastTick(iseg,isid,{ts:now(),ltp:idx.quote.ltp,volume:idx.curMinuteVolume,volumeSource:'LINKED_FUTURES',exchangeSegment:iseg,securityId:isid});
   }
  }
}
function handlePacket(b){packetCount++;const h=header(b);if(!h||!h.seg)return;const s=state(h.seg,h.sid);try{if(h.code===2)normalize(h,s,{ltp:b.readFloatLE(8),ltt:b.readUInt32LE(12)});else if(h.code===4)normalize(h,s,{ltp:b.readFloatLE(8),lastQty:b.readInt16LE(12),ltt:b.readUInt32LE(14),atp:b.readFloatLE(18),volume:b.readUInt32LE(22),sellQty:b.readUInt32LE(26),buyQty:b.readUInt32LE(30),open:b.readFloatLE(34),close:b.readFloatLE(38),high:b.readFloatLE(42),low:b.readFloatLE(46)});else if(h.code===5){const oi=b.readUInt32LE(8);s.quote.oi=oi;s.oi=oi;broadcastTick(h.seg,h.sid,{ts:now(),oi,exchangeSegment:h.seg,securityId:h.sid})}else if(h.code===6){s.previousClose=b.readFloatLE(8);s.quote.previousClose=s.previousClose;s.oiPrev=b.readUInt32LE(12)}else if(h.code===8){const q={ltp:b.readFloatLE(8),lastQty:b.readInt16LE(12),ltt:b.readUInt32LE(14),atp:b.readFloatLE(18),volume:b.readUInt32LE(22),sellQty:b.readUInt32LE(26),buyQty:b.readUInt32LE(30),oi:b.readUInt32LE(34),oiDayHigh:b.readUInt32LE(38),oiDayLow:b.readUInt32LE(42),open:b.readFloatLE(46),close:b.readFloatLE(50),high:b.readFloatLE(54),low:b.readFloatLE(58)};s.depth=depth5(b,62);normalize(h,s,{...q,depth:s.depth})}}catch(e){console.log('Packet decode error:',e.message)}}
function broadcast(o){const r=JSON.stringify(o);for(const c of clients)if(c.ws.readyState===WebSocket.OPEN)c.ws.send(r)}
function broadcastTick(seg,sid,t){const k=key(seg,sid),r=JSON.stringify({type:'tick',data:{exchangeSegment:seg,securityId:String(sid),instrument:lookup(seg,sid)||INDEXES.NIFTY,tick:t}});for(const c of clients)if(c.subscriptions.has(k)&&c.ws.readyState===WebSocket.OPEN)c.ws.send(r)}

let vapidReady=false; if(VAPID_PUBLIC_KEY&&VAPID_PRIVATE_KEY&&VAPID_SUBJECT){try{webpush.setVapidDetails(VAPID_SUBJECT,VAPID_PUBLIC_KEY,VAPID_PRIVATE_KEY);vapidReady=true;console.log('VAPID ready')}catch(e){console.log('VAPID error:',e.message)}}
async function push(payload){if(!vapidReady)throw Error('VAPID is not configured'); if(!pushes.size)throw Error('No active push subscription');for(const [id,s] of [...pushes]){try{await webpush.sendNotification(s,JSON.stringify(payload))}catch(e){if(e.statusCode===404||e.statusCode===410)pushes.delete(id);else console.log('Push failed:',e.message)}}}

function auth(req,res,next){if(!CLIENT_API_KEY)return next();const k=req.get('x-client-key')||req.query.key||'';if(k!==CLIENT_API_KEY)return res.status(401).json({success:false,error:'Unauthorized'});next()}app.use('/api',auth);
app.get('/',(req,res)=>res.status(200).type('html').send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="cache-control" content="no-store"><title>Dhan PWA Backend</title><style>body{font-family:system-ui,sans-serif;background:#0b1220;color:#eef2ff;padding:24px;line-height:1.5}.card{background:#111b2e;padding:18px;border-radius:14px;max-width:760px}h1{margin:0 0 8px}a{color:#7dd3fc}.good{color:#4ade80}.warn{color:#fbbf24}.bad{color:#fb7185}.muted{color:#94a3b8}code{background:#1e293b;padding:2px 6px;border-radius:6px}.row{margin:10px 0}</style></head><body><div class="card"><h1>Dhan PWA Compatible Backend</h1><div class="good">● ONLINE</div><p>Frontend-agnostic tick-by-tick Dhan market-data gateway.</p><p>Version: <code>3.0-PWA</code></p><div class="row">Dhan feed: <strong id="feed" class="warn">CHECKING...</strong></div><div class="row muted" id="stats">Checking live status...</div><p><a href="/health">Health</a> · <a href="/api/health">API Health JSON</a> · <a href="/api/v1/status">Status</a> · <a href="/api/v1/bootstrap?symbol=NIFTY">NIFTY Bootstrap</a> · <a href="/api/v1/instruments/search?q=NIFTY&exchangeSegment=IDX_I&limit=10">Instrument Search</a> · <a href="/push-status">Push Status</a></p></div><script>async function refresh(){try{const r=await fetch('/api/health',{cache:'no-store'}),j=await r.json(),f=document.getElementById('feed');f.textContent=j.feedState||'UNKNOWN';f.className=j.feedState==='CONNECTED'?'good':'warn';document.getElementById('stats').textContent='Ticks: '+j.ticks+' · Packets: '+j.packets+' · Subscriptions: '+j.subscribedInstruments+' · Clients: '+j.connectedClients+' · Instruments: '+j.instruments+' · Push devices: '+j.pushSubscribers}catch(e){document.getElementById('feed').textContent='UNAVAILABLE';document.getElementById('feed').className='bad'}}refresh();setInterval(refresh,3000)</script></body></html>`));
app.get('/health',(req,res)=>res.json({success:true,service:'Dhan PWA Compatible Backend',version:'3.0-PWA',feedState,feedLastMessageAt:feedLast||null,dhanConfigured:!!(DHAN_CLIENT_ID&&(token||DHAN_PIN)),subscribedInstruments:desired.size,connectedClients:clients.size,instruments:instruments.length,ticks:tickCount,packets:packetCount,pushSubscribers:pushes.size,vapidReady}));
app.get('/api/health',(req,res)=>res.json({success:true,version:'3.0-PWA',feedState,feedLastMessageAt:feedLast||null,stale:feedLast?now()-feedLast>STALE_MS:true,ticks:tickCount,packets:packetCount,subscribedInstruments:desired.size,connectedClients:clients.size,instruments:instruments.length,pushSubscribers:pushes.size,vapidReady}));
app.get('/api/v1/time',(req,res)=>res.json({success:true,serverNow:now(),timeZone:'Asia/Kolkata',iso:new Date().toISOString()}));
app.get('/api/v1/status',(req,res)=>res.json({success:true,version:'3.0-PWA',feedState,feedLastMessageAt:feedLast||null,stale:feedLast?now()-feedLast>STALE_MS:true,desiredSubscriptions:desired.size,clients:clients.size,instruments:instruments.length}));
app.get('/api/v1/bootstrap',async(req,res)=>{try{const symbol=String(req.query.symbol||'NIFTY').toUpperCase();const x=INDEXES[symbol];if(!x)throw Error('Unsupported index '+symbol);const s=state(x.exchangeSegment,x.securityId);res.json({success:true,data:{...x,futMatch:undefined,quote:s.quote,lastUpdate:s.lastUpdate,linkedFuture:x.future||null,feedState}})}catch(e){res.status(400).json({success:false,error:e.message})}});
app.get('/api/v1/instruments/search',async(req,res)=>{try{const q=String(req.query.q||'').toUpperCase();const seg=String(req.query.exchangeSegment||'').toUpperCase();const direct=Object.entries(INDEXES).filter(([k,v])=>(!q||k.includes(q)||v.symbol.toUpperCase().includes(q))&&(!seg||seg===v.exchangeSegment)).map(([k,v])=>({exchangeSegment:v.exchangeSegment,securityId:v.securityId,symbol:k,displayName:v.symbol,instrument:'INDEX',instrumentType:'INDEX',strikeStep:v.strikeStep}));await loadInstruments();const out=[...direct,...instruments.filter(x=>(!q||[x.symbol,x.tradingSymbol,x.displayName,x.securityId].some(v=>String(v).toUpperCase().includes(q)))&&(!seg||x.exchangeSegment===seg))].slice(0,Math.min(Number(req.query.limit||50),200));const uniq=[];const seen=new Set();for(const x of out){const k=key(x.exchangeSegment,x.securityId);if(!seen.has(k)){seen.add(k);uniq.push(x)}}res.json({success:true,count:uniq.length,data:uniq})}catch(e){res.status(502).json({success:false,error:e.message})}});
app.get('/api/v1/snapshot',async(req,res)=>{const seg=String(req.query.exchangeSegment||''),sid=String(req.query.securityId||'');if(!seg||!sid)return res.status(400).json({success:false,error:'exchangeSegment and securityId required'});const s=state(seg,sid);res.json({success:true,data:{instrument:lookup(seg,sid),quote:s.quote,depth:s.depth,previousClose:s.previousClose,lastUpdate:s.lastUpdate,stale:!s.lastUpdate||now()-s.lastUpdate>STALE_MS}})});
app.get('/api/v1/ticks',(req,res)=>{const s=state(String(req.query.exchangeSegment||''),String(req.query.securityId||''));const lim=Math.min(Number(req.query.limit||500),MAX_TICKS);res.json({success:true,data:s.ticks.slice(-lim)})});
app.get('/api/v1/candles',(req,res)=>{const s=state(String(req.query.exchangeSegment||''),String(req.query.securityId||''));const tf=Number(req.query.timeframe||1),lim=Math.min(Number(req.query.limit||500),MAX_CANDLES);let a=[...s.candles.values()];if(tf>1)a=a.filter(c=>Math.floor(c.timestamp/(tf*60000))===Math.floor(c.timestamp/(tf*60000)));res.json({success:true,timeframe:tf,data:a.slice(-lim)})});
app.post('/api/v1/history',async(req,res)=>{
 try{
  const body=req.body||{};
  const j=await dhanPost('/charts/intraday',body);
  // NIFTY/SENSEX index history has no real volume in it (see comment near INDEXES above). Transparently
  // swap in the linked futures contract's per-candle volume, matched by timestamp, before replying —
  // the PWA calls this same endpoint the same way and just receives a meaningful volume array back.
  const fut=getLinkedFuture(String(body.exchangeSegment||'').toUpperCase(),String(body.securityId||''));
  if(fut&&Array.isArray(j.timestamp)){
   try{
    const fj=await dhanPost('/charts/intraday',{...body,securityId:String(fut.securityId),exchangeSegment:fut.exchangeSegment,instrument:'FUTIDX'});
    if(Array.isArray(fj.volume)&&Array.isArray(fj.timestamp)){
     const volByTs=new Map();for(let i=0;i<fj.timestamp.length;i++)volByTs.set(Number(fj.timestamp[i]),Number(fj.volume[i]||0));
     j.volume=j.timestamp.map(t=>volByTs.get(Number(t))??0);
     j.volumeSource='LINKED_FUTURES';j.linkedFuture=fut;
    }
   }catch(e){console.log('Linked futures history fetch failed:',e.message)}
  }
  res.json(j)
 }catch(e){res.status(502).json({success:false,error:e.message})}
});
app.post('/api/v1/quote',async(req,res)=>{try{res.json(await dhanPost('/marketfeed/quote',req.body))}catch(e){res.status(502).json({success:false,error:e.message})}});
app.get('/api/v1/vapid-public-key',(req,res)=>res.json({success:true,publicKey:VAPID_PUBLIC_KEY||null}));
function savePushSubscription(req,res){const {subscription,clientId='default'}=req.body||{};if(!subscription?.endpoint)return res.status(400).json({success:false,error:'subscription required'});const id=crypto.createHash('sha256').update(subscription.endpoint).digest('hex');pushes.set(id,{...subscription,clientId});res.json({success:true,subscribers:pushes.size});}
async function sendTestPush(res){try{await push({type:'TEST',title:'RSI DEMA Volume',body:'Test alarm pipeline is working.'});res.json({success:true,subscribers:pushes.size})}catch(e){res.status(503).json({success:false,error:e.message})}}
app.post('/api/v1/push/subscribe',savePushSubscription);
app.post('/api/v1/push/test',async(req,res)=>sendTestPush(res));
app.post('/api/v1/push/event',async(req,res)=>{const b=req.body||{};if(!b.title||!b.body)return res.status(400).json({success:false,error:'title and body required'});try{await push({type:b.type||'EVENT',title:String(b.title),body:String(b.body)});res.json({success:true,subscribers:pushes.size})}catch(e){res.status(503).json({success:false,error:e.message})}});
// Legacy aliases kept intentionally so older PWAs can use the same universal backend.
app.get('/vapid-public-key',(req,res)=>res.json({success:true,publicKey:VAPID_PUBLIC_KEY||null}));
app.post('/subscribe',savePushSubscription);
app.post('/test-push',async(req,res)=>sendTestPush(res));
app.get('/push-status',(req,res)=>res.json({success:true,vapidReady,subscribers:pushes.size}));

const server=app.listen(PORT,()=>console.log(`Dhan Ultimate Backend v2.0 listening on :${PORT}`));
const wss=new WebSocket.Server({server,path:'/ws'});
wss.on('connection',(ws)=>{const c={ws,subscriptions:new Map()};clients.add(c);ws.send(JSON.stringify({type:'hello',service:'Dhan Ultimate Backend',version:'3.0-PWA',feedState}));ws.on('message',raw=>{try{const m=JSON.parse(raw.toString());if(m.action==='subscribe'){for(const x of (m.instruments||[])){if(!x.exchangeSegment||!x.securityId)continue;const k=key(x.exchangeSegment,x.securityId);c.subscriptions.set(k,{mode:x.mode||'quote'});desired.set(k,best(desired.get(k)||'ticker',x.mode||'quote'))}rebuild();for(const [k,mode] of desired){const [seg,sid]=k.split(':');sendSub(mode,[{exchangeSegment:seg,securityId:sid}])}ws.send(JSON.stringify({type:'subscribed',count:c.subscriptions.size}))}else if(m.action==='unsubscribe'){for(const x of (m.instruments||[]))c.subscriptions.delete(key(x.exchangeSegment,String(x.securityId)));rebuild()}else if(m.action==='snapshot'){const s=state(String(m.exchangeSegment),String(m.securityId));ws.send(JSON.stringify({type:'snapshot',data:{instrument:lookup(s.exchangeSegment,s.securityId),quote:s.quote,lastUpdate:s.lastUpdate,feedState}}))}else if(m.action==='ping')ws.send(JSON.stringify({type:'pong',ts:now()}))}catch(e){ws.send(JSON.stringify({type:'error',error:e.message}))}});ws.on('close',()=>{clients.delete(c);rebuild()})});
setInterval(()=>{if(feedState==='CONNECTED'&&feedLast&&now()-feedLast>STALE_MS*2){try{feed.close()}catch{}}if(feedState==='DISCONNECTED'||feedState==='NO_TOKEN')connect()},10000);
// Resolve the NIFTY/SENSEX -> futures link before the feed subscribes, so live volume works from tick one.
// loadInstruments() re-runs every 6h on its own (and re-resolves futures each time) to pick up monthly rollovers.
loadInstruments().then(()=>connect()).catch(e=>{console.log('Instrument master load failed at startup:',e.message);connect()});
process.on('SIGTERM',()=>{try{feed?.close()}catch{}server.close(()=>process.exit(0))});
