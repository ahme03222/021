// ================================================================
//  PANDA BAMBOO FACTORY — Railway / Node.js v3.0
//  Firebase Realtime Database
//  Environment Variables:
//    FIREBASE_DATABASE_URL  e.g. https://YOUR-DB.firebaseio.com
//    FIREBASE_API_KEY       Firebase API key
//    BOT_TOKEN              Telegram Bot Token
//    ADMIN_IDS              comma-separated admin Telegram IDs
//    PORT                   (optional, Railway sets this automatically)
// ================================================================

import express from 'express';
import { webcrypto } from 'crypto';
const crypto = webcrypto;

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Parse env (replaces Cloudflare's `env` object) ──────────────
const env = {
  FIREBASE_DATABASE_URL : process.env.FIREBASE_DATABASE_URL,
  FIREBASE_API_KEY      : process.env.FIREBASE_API_KEY,
  BOT_TOKEN             : process.env.BOT_TOKEN,
  ADMIN_IDS             : process.env.ADMIN_IDS || '',
  ENVIRONMENT           : process.env.ENVIRONMENT || 'production',
};

// ── Middleware ───────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Action');
  res.setHeader('Access-Control-Max-Age',       '86400');
  res.setHeader('X-Content-Type-Options',   'nosniff');
  res.setHeader('X-Frame-Options',          'DENY');
  res.setHeader('X-XSS-Protection',         '1; mode=block');
  res.setHeader('Referrer-Policy',          'no-referrer');
  res.setHeader('Permissions-Policy',       'geolocation=(), microphone=(), camera=()');
  res.removeHeader('X-Powered-By');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  if (req.path === '/api' && req.method !== 'POST')
    return res.status(405).json({ success:false, error:'Method not allowed' });
  next();
});
app.use(express.text({ limit: '10kb', type: '*/*' }));

// Input validation helpers
function isValidUid(uid){ return typeof uid==='string' && /^\d{5,15}$/.test(uid); }
function isValidAddress(addr){ return typeof addr==='string' && addr.length>=10 && addr.length<=100; }
function isValidAmount(n){ return typeof n==='number' && isFinite(n) && n>0 && n<1e9; }

// ── Helpers ──────────────────────────────────────────────────────
const ok   = (d)     => ({ success: true,  data: d });
const fail = (m, s=400) => ({ success: false, error: m, _status: s });

function sanitise(i){
  if(!i) return i;
  return i
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi,'')
    .replace(/[<>]/g, m => m==='<'?'&lt;':'&gt;');
}

// ── Game config (unchanged) ──────────────────────────────────────
const G = {
  BAMBOO_PER_COIN:20, TON_PER_COIN:0.00005, TON_TO_BAMBOO:50000,
  MIN_WITHDRAW:200, MIN_DEPOSIT_TON:1,
  REF_BONUS_PCT:20,
  WELCOME_BAMBOO:0,
  WELCOME_COINS :195,
  WELCOME_RATE  :4.167,
  MAX_TANK_LVL:27,
  MAX_RETRY:3, RETRY_DELAY_MS:100,
  ITEMS:{
    bamboo_stick :{price:7500,    power:50    },
    panda_paw    :{price:25000,   power:200   },
    leaf_fan     :{price:125000,  power:1200  },
    bamboo_energy:{price:625000,  power:7500  },
    panda_den    :{price:3130000, power:45000 },
    bamboo_forest:{price:6500000, power:110000},
  },
  TANK:{
    1 :{cap:5000,      upgCost:1000      },
    2 :{cap:10000,     upgCost:3000      },
    3 :{cap:20000,     upgCost:8000      },
    4 :{cap:40000,     upgCost:20000     },
    5 :{cap:80000,     upgCost:50000     },
    6 :{cap:150000,    upgCost:120000    },
    7 :{cap:250000,    upgCost:250000    },
    8 :{cap:400000,    upgCost:450000    },
    9 :{cap:600000,    upgCost:750000    },
    10:{cap:900000,    upgCost:1200000   },
    11:{cap:1300000,   upgCost:1800000   },
    12:{cap:1800000,   upgCost:2700000   },
    13:{cap:2500000,   upgCost:4000000   },
    14:{cap:3300000,   upgCost:5500000   },
    15:{cap:4300000,   upgCost:8000000   },
    16:{cap:5500000,   upgCost:11000000  },
    17:{cap:7000000,   upgCost:15000000  },
    18:{cap:8800000,   upgCost:20000000  },
    19:{cap:11000000,  upgCost:27000000  },
    20:{cap:14000000,  upgCost:35000000  },
    21:{cap:17500000,  upgCost:45000000  },
    22:{cap:22000000,  upgCost:58000000  },
    23:{cap:28000000,  upgCost:75000000  },
    24:{cap:35000000,  upgCost:95000000  },
    25:{cap:44000000,  upgCost:120000000 },
    26:{cap:55000000,  upgCost:150000000 },
    27:{cap:70000000,  upgCost:200000000 },
  },
  REF_TASKS:{
    r1  :{n:1,   bam:50,     coins:2   },
    r5  :{n:5,   bam:250,    coins:10  },
    r10 :{n:10,  bam:600,    coins:25  },
    r20 :{n:20,  bam:1500,   coins:60  },
    r50 :{n:50,  bam:4000,   coins:150 },
    r70 :{n:70,  bam:6000,   coins:220 },
    r100:{n:100, bam:10000,  coins:400 },
    r200:{n:200, bam:20000,  coins:800 },
    r500:{n:500, bam:50000,  coins:2000},
  },
  REF_ACTIVE_TASKS:{
    ra1  :{n:1,   bam:10000,   coins:40   },
    ra5  :{n:5,   bam:50000,   coins:200  },
    ra10 :{n:10,  bam:120000,  coins:500  },
    ra20 :{n:20,  bam:300000,  coins:1200 },
    ra50 :{n:50,  bam:800000,  coins:3000 },
    ra70 :{n:70,  bam:1200000, coins:4400 },
    ra100:{n:100, bam:2000000, coins:8000 },
    ra200:{n:200, bam:4000000, coins:16000},
    ra500:{n:500, bam:10000000,coins:40000},
  },
  SOC_TASKS:{
    tg_payouts:1000,
    tg_news   :500,
    tg_ch     :1000,
    tg_grp    :500,
    tg_bot    :300,
  },
  BOT_USERNAME:'PandaBamboBot',
};

// ── Default partner tasks ────────────────────────────────────────
const DEFAULT_PARTNER_TASKS = [
  {
    id: 'partner_payouts',
    name: 'Join Payouts Channel',
    type: 'channel',
    link: 'https://t.me/PandaBambooPayouts',
    bambooReward: 100,
    targetUsers: null,
    status: 'active',
    isDefault: true,
  },
  {
    id: 'partner_news',
    name: 'Join Mining News Channel',
    type: 'channel',
    link: 'https://t.me/PandaMiningNews',
    bambooReward: 100,
    targetUsers: null,
    status: 'active',
    isDefault: true,
  },
];

// ── Firebase helpers ──────────────────────────────────────────────
function fbUrl(path){
  const b = env.FIREBASE_DATABASE_URL?.replace(/\/$/,'');
  if(!b) throw new Error('FIREBASE_DATABASE_URL not set');
  const k = env.FIREBASE_API_KEY;
  if(!k) throw new Error('FIREBASE_API_KEY not set');
  return `${b}/${path.replace(/^\//,'')}.json?key=${k}`;
}
async function dbGet(path){
  try{
    const r = await fetch(fbUrl(path));
    if(!r.ok) throw new Error(`GET ${r.status}`);
    return { success:true, data: await r.json() };
  }catch(e){ console.error('DB GET',path,e.message); return { success:false, error:e.message }; }
}
async function dbSet(path, data){
  try{
    const r = await fetch(fbUrl(path),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    if(!r.ok) throw new Error(`SET ${r.status}`);
    return { success:true };
  }catch(e){ console.error('DB SET',path,e.message); return { success:false, error:e.message }; }
}
async function dbUpdate(path, updates){
  try{
    const r = await fetch(fbUrl(path),{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(updates)});
    if(!r.ok) throw new Error(`UPDATE ${r.status}`);
    return { success:true };
  }catch(e){ console.error('DB UPDATE',path,e.message); return { success:false, error:e.message }; }
}
async function dbPush(path, data){
  try{
    const r = await fetch(fbUrl(path),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    if(!r.ok) throw new Error(`PUSH ${r.status}`);
    const j = await r.json();
    return { success:true, data:{ id:j.name } };
  }catch(e){ console.error('DB PUSH',path,e.message); return { success:false, error:e.message }; }
}
async function dbDelete(path){
  try{
    const r = await fetch(fbUrl(path),{method:'DELETE'});
    if(!r.ok) throw new Error(`DELETE ${r.status}`);
    return { success:true };
  }catch(e){ console.error('DB DELETE',path,e.message); return { success:false, error:e.message }; }
}

// ── Rate limiter ──────────────────────────────────────────────────
const _rl = new Map();
function rateOk(ip){
  const now = Date.now();
  const d   = _rl.get(ip) || { c:0, r:now+60000 };
  if(now > d.r){ d.c=0; d.r=now+60000; }
  d.c++; _rl.set(ip,d);
  return d.c <= 60;
}

// ── Per-user per-action cooldown ─────────────────────────────────
const _userActionTs = new Map();
const ACTION_COOLDOWNS = {
  collect      : 2500,
  buyItem      : 2500,
  upgradeTank  : 2500,
  exchange     : 2500,
  withdraw     : 5000,
  claimTask    : 2500,
  verifyTask   : 2500,
  createTask   : 5000,
};
function userActionOk(uid, action){
  const cd = ACTION_COOLDOWNS[action];
  if(!cd) return true;
  const key  = `${uid}:${action}`;
  const now  = Date.now();
  const last = _userActionTs.get(key) || 0;
  if(now - last < cd) return false;
  _userActionTs.set(key, now);
  return true;
}

// ── Logging ───────────────────────────────────────────────────────
const BALANCE_CHANGE_EVENTS = new Set([
  'collect','buy_item','upgrade_tank','exchange',
  'withdraw_request','deposit_completed','claim_task',
  'verify_task','create_task','admin_set_balance',
  'admin_confirm_deposit','referral_commission',
]);
function log(uid, type, details={}, meta={}){
  if(!BALANCE_CHANGE_EVENTS.has(type)) return;
  const ts   = Date.now();
  const date = new Date(ts).toISOString();
  const entry = { ts, date, type, ...details };
  dbPush(`users/${uid}/log`, entry).catch(e=>console.error('LOG ERROR:',e.message));
}

// ── Telegram validation ───────────────────────────────────────────
async function validateTg(initData, botToken){
  try{
    if(!initData) return { valid:false, error:'No init data' };
    const p          = new URLSearchParams(initData);
    const startParam = (p.get('start_param')||'').replace(/\D/g,'');
    if(!botToken){
      const u = p.get('user');
      if(!u) return { valid:false, error:'No user' };
      return { valid:true, user:JSON.parse(decodeURIComponent(u)), startParam };
    }
    const hash = p.get('hash');
    if(!hash) return { valid:false, error:'No hash' };
    p.delete('hash');
    const authDate = parseInt(p.get('auth_date')||'0');
    if(Date.now()/1000 - authDate > 900) return { valid:false, error:'Expired' };
    const dc  = [...p.entries()].sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>`${k}=${v}`).join('\n');
    const enc = new TextEncoder();
    const sec = await crypto.subtle.importKey('raw',enc.encode('WebAppData'),{name:'HMAC',hash:'SHA-256'},false,['sign']);
    const kb  = await crypto.subtle.sign('HMAC',sec,enc.encode(botToken));
    const key = await crypto.subtle.importKey('raw',kb,{name:'HMAC',hash:'SHA-256'},false,['sign']);
    const sig = await crypto.subtle.sign('HMAC',key,enc.encode(dc));
    const hex = [...new Uint8Array(sig)].map(b=>b.toString(16).padStart(2,'0')).join('');
    if(hex !== hash) return { valid:false, error:'Bad hash' };
    const u = p.get('user');
    if(!u) return { valid:false, error:'No user' };
    return { valid:true, user:JSON.parse(decodeURIComponent(u)), startParam };
  }catch(e){ return { valid:false, error:e.message }; }
}

// ── Tank sync & rate helpers ──────────────────────────────────────
function syncTank(user){
  const now = Date.now(); const sec = (now-(user.lastSeen||now))/1000;
  if(sec<=0||!user.miningRate){ user.lastSeen=now; return; }
  const cfg  = G.TANK[user.tankLevel||1]||G.TANK[1];
  const rate = user.miningRate/3600;
  user.tankAccrued = Math.min(cfg.cap,(user.tankAccrued||0)+rate*sec);
  user.lastSeen    = now;
}
function recalcRate(m){ return Object.entries(m||{}).reduce((s,[id,c])=>s+(G.ITEMS[id]?.power||0)*c,0); }

// ── Telegram notification ─────────────────────────────────────────
async function sendTgNotification(userId, message){
  try{
    if(!env.BOT_TOKEN) return;
    await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`,{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({ chat_id:userId, text:message, parse_mode:'HTML' }),
    });
  }catch(e){ console.error('sendTgNotification error:',e.message); }
}

// ── Referral registration ─────────────────────────────────────────
async function registerReferral(uid, user, referrerId){
  try{
    const rr   = await dbGet(`users/${referrerId}/referrals`);
    const refs = rr.data || {};
    if(!refs[uid]){
      await dbSet(`users/${referrerId}/referrals/${uid}`,{
        userId:uid,
        firstName:user.firstName, lastName:user.lastName,
        username:user.username,   photoUrl:user.photoUrl,
        joinedAt:Date.now(),      earned:0,
      });
      const notifKey = `notifSent/ref_${uid}_${referrerId}`;
      const already  = await dbGet(notifKey);
      if(!already.data){
        const myTs = Date.now();
        await dbSet(notifKey,{ts:myTs,by:uid});
        await new Promise(r=>setTimeout(r,150));
        const confirm = await dbGet(notifKey);
        if(confirm.data && confirm.data.ts===myTs){
          console.log(`Referral registered: ${uid} referred by ${referrerId}`);
          const refName  = (user.firstName||'Someone').slice(0,32);
          const notifMsg = `🎉 <b>Congratulations!</b> <b>${refName}</b> just registered using your referral link!\n\n🐼 You will automatically earn <b>20% commission</b> on all their Market purchases.\n\n<i>Track your earnings in the Friends section</i>`;
          sendTgNotification(referrerId,notifMsg).catch(()=>{});
        }
      }
    }
  }catch(e){ console.error('registerReferral error:',e.message); }
}

// ── Seed partner tasks ────────────────────────────────────────────
async function seedPartnerTasks(){
  try{
    const tpr      = await dbGet('tasks/partner');
    const existing = tpr.data || {};
    for(const task of DEFAULT_PARTNER_TASKS){
      if(!existing[task.id]){
        const now      = Date.now();
        const taskData = { ...task, completions:0, completedBy:[], createdAt:now, updatedAt:now };
        await dbSet(`tasks/partner/${task.id}`, taskData);
        console.log(`Seeded partner task: ${task.id}`);
      }
    }
  }catch(e){ console.error('seedPartnerTasks error:',e.message); }
}

// ── Telegram channel membership check ────────────────────────────
async function checkMembership(userId, channelLink){
  try{
    if(!env.BOT_TOKEN){ console.log('No BOT_TOKEN, skipping check'); return true; }
    let username = channelLink;
    if(channelLink.includes('t.me/')) username = channelLink.split('t.me/')[1].split('?')[0].split('/')[0];
    if(username.startsWith('@')) username = username.substring(1);
    const res = await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN}/getChatMember`,{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({ chat_id:`@${username}`, user_id:parseInt(userId) }),
    });
    const j = await res.json();
    if(!j.ok){ console.error('TG API:',j); return false; }
    return ['member','administrator','creator'].includes(j.result?.status);
  }catch(e){ console.error('checkMembership:',e.message); return false; }
}

// ── User factory ──────────────────────────────────────────────────
function makeUser(uid, tg={}, ref=null){
  return {
    userId:uid, firstName:(tg.first_name||'').slice(0,64),
    lastName:(tg.last_name||'').slice(0,64), username:(tg.username||'').slice(0,64),
    photoUrl:(tg.photo_url||'').slice(0,512),
    bamboo:G.WELCOME_BAMBOO, coins:G.WELCOME_COINS, miningRate:G.WELCOME_RATE,
    totalEarned:0, machines:{}, tankLevel:1, tankAccrued:0,
    lastSeen:Date.now(), createdAt:Date.now(),
    welcomeBonusGiven:true, hasDeposited:false, tonBalance:0,
    referralCode:String(uid), referredBy:ref||null, completedTasks:[],
  };
}

function extractStartParam(initDataStr){
  try{
    const p  = new URLSearchParams(initDataStr||'');
    const sp = p.get('start_param');
    if(sp) return sp.replace(/\D/g,'');
    const userRaw = p.get('user');
    if(userRaw){
      const u = JSON.parse(decodeURIComponent(userRaw));
      if(u.start_param) return String(u.start_param).replace(/\D/g,'');
    }
  }catch(_){}
  return '';
}

// ── Leaderboard update ────────────────────────────────────────────
async function updateLeaderboardEntry(uid, user){
  try{
    const COMP_DURATION_MS = 10*24*60*60*1000;
    let meta = (await dbGet('competition/meta')).data;
    const nowMs = Date.now();
    if(!meta||!meta.endDate||!meta.startDate){
      meta = { startDate:meta?.startDate||nowMs, endDate:meta?.endDate||(nowMs+COMP_DURATION_MS) };
      await dbSet('competition/meta', meta);
    }
    if(nowMs > meta.endDate){
      meta = { startDate:nowMs, endDate:nowMs+COMP_DURATION_MS };
      await dbSet('competition/meta', meta);
      await dbSet('competition/snapshots', null);
      await dbSet('competition/users', null);
      await dbSet('competition/leaderboard', null);
    }
    const compStarted = nowMs >= meta.startDate;
    if(!compStarted) return;
    const rr      = await dbGet(`users/${uid}/referrals`);
    const refIds  = rr.data ? Object.keys(rr.data) : [];
    let activeNow = 0;
    for(const refId of refIds){
      const hd = await dbGet(`users/${refId}/hasDeposited`);
      if(hd.data===true) activeNow++;
    }
    const miningNow = Math.round((user.miningRate||0)*24);
    const snapKey   = `competition/snapshots/${uid}`;
    let snap        = (await dbGet(snapKey)).data;
    if(!snap){
      snap = { activeRefs:activeNow, miningPerDay:miningNow, ts:Date.now() };
      await dbSet(snapKey, snap);
    }
    const activeScore = Math.max(0, activeNow - snap.activeRefs);
    const miningScore = Math.max(0, miningNow - snap.miningPerDay);
    const entry = {
      userId:uid,
      name:`${user.firstName||''} ${user.lastName||''}`.trim()||'Panda',
      photo:user.photoUrl||null,
      activeScore, miningScore, activeNow, miningNow, ts:Date.now(),
    };
    await dbSet(`competition/users/${uid}`, entry);
    const allr    = await dbGet('competition/users');
    const all     = allr.data ? Object.values(allr.data) : [];
    const byActive  = [...all].sort((a,b)=>b.activeScore-a.activeScore).slice(0,50).map(u=>({userId:u.userId,name:u.name,photo:u.photo,score:u.activeScore}));
    const byMining  = [...all].sort((a,b)=>b.miningScore-a.miningScore).slice(0,50).map(u=>({userId:u.userId,name:u.name,photo:u.photo,score:u.miningScore}));
    await dbSet('competition/leaderboard', { activeRefs:byActive, miningSpeed:byMining, updatedAt:Date.now() });
  }catch(e){ console.error('updateLeaderboardEntry:',e.message); }
}

// ── Handlers ──────────────────────────────────────────────────────
async function hGetState(uid, tg, data={}, _meta={}){
  try{
    const rawRef = (
      data?._startParam ||
      extractStartParam(data?._initData||'') ||
      (data?.start_param||'').toString().replace(/\D/g,'')
    ).replace(/\D/g,'');
    const ref = rawRef && rawRef !== uid ? rawRef : null;

    const ur = await dbGet(`users/${uid}`); let user = ur.data;
    seedPartnerTasks().catch(e=>console.error('seed:',e.message));

    if(!user){
      user = makeUser(uid, tg, ref);
      if(user.referredBy) await registerReferral(uid, user, user.referredBy);
      await dbSet(`users/${uid}`, user);
    }else{
      syncTank(user);
      let needsSave = false;
      if(!user.welcomeBonusGiven){
        user.coins      = (user.coins||0)      + G.WELCOME_COINS;
        user.bamboo     = (user.bamboo||0)      + G.WELCOME_BAMBOO;
        user.miningRate = Math.max(user.miningRate||0, G.WELCOME_RATE);
        user.welcomeBonusGiven = true;
        needsSave = true;
        log(uid,'welcome_bonus_granted',{coins_added:G.WELCOME_COINS,bamboo_added:G.WELCOME_BAMBOO,miningRate_set:G.WELCOME_RATE},_meta);
      }
      if(tg){
        if(tg.first_name) user.firstName = tg.first_name.slice(0,64);
        if(tg.last_name)  user.lastName  = tg.last_name.slice(0,64);
        if(tg.username)   user.username  = tg.username.slice(0,64);
        if(tg.photo_url)  user.photoUrl  = tg.photo_url.slice(0,512);
      }
      await dbUpdate(`users/${uid}`,{
        firstName:user.firstName, lastName:user.lastName,
        username:user.username,   photoUrl:user.photoUrl,
        tankAccrued:user.tankAccrued, lastSeen:user.lastSeen,
        ...(needsSave?{coins:user.coins,bamboo:user.bamboo,miningRate:user.miningRate,welcomeBonusGiven:true}:{}),
      });
    }
    updateLeaderboardEntry(uid, user).catch(()=>{});
    const rr       = await dbGet(`users/${uid}/referrals`);
    const refList  = Object.values(rr.data||{});
    const referrals = await Promise.all(refList.map(async r=>{
      let deposited = r.hasDeposited||false;
      if(!deposited){
        const ud = await dbGet(`users/${r.userId}/hasDeposited`);
        deposited = ud.data===true;
        if(deposited) await dbUpdate(`users/${uid}/referrals/${r.userId}`,{hasDeposited:true}).catch(()=>{});
      }
      return { userId:r.userId, name:`${r.firstName||''} ${r.lastName||''}`.trim()||'Friend', photo:r.photoUrl||null, date:r.joinedAt?new Date(r.joinedAt).toLocaleDateString():'', earned:r.earned||0, hasDeposited:deposited };
    }));
    const er = await dbGet(`users/${uid}/exchHistory`);
    const exchHistory = er.data?Object.values(er.data).sort((a,b)=>b.ts-a.ts).slice(0,30):[];
    const wr = await dbGet(`users/${uid}/wdHistory`);
    const wdHistory = wr.data?Object.values(wr.data).sort((a,b)=>b.ts-a.ts).slice(0,30):[];
    const dr = await dbGet(`users/${uid}/deposits`);
    const pendingDeposit = (dr.data?Object.values(dr.data):[]).find(d=>d.status==='pending')||null;
    const tpr = await dbGet('tasks/partner');
    const tcr = await dbGet('tasks/community');
    const tasks = {
      partner  : tpr.data?Object.values(tpr.data).filter(t=>t.status==='active'):[],
      community: tcr.data?Object.values(tcr.data).filter(t=>t.status==='active'):[],
    };
    const lr = await dbGet(`users/${uid}/log`);
    const balanceLog = lr.data?Object.values(lr.data).sort((a,b)=>b.ts-a.ts).slice(0,50):[];
    const depr    = await dbGet(`users/${uid}/deposits`);
    const deposits = (depr.data?Object.values(depr.data):[]).map(d=>({amount:d.amount||0,status:d.status||'pending',ts:d.timestamp||d.ts||0}));
    return { success:true, data:{ user:{bamboo:user.bamboo||0,coins:user.coins||0,miningRate:user.miningRate||0,totalEarned:user.totalEarned||0,machines:user.machines||{},tankLevel:user.tankLevel||1,tankAccrued:user.tankAccrued||0,hasDeposited:user.hasDeposited||false,tonBalance:user.tonBalance||0}, referrals, completedTasks:user.completedTasks||[], exchHistory, wdHistory, deposits, balanceLog, pendingDeposit, tasks } };
  }catch(e){ console.error('getState',e); return { success:false, error:e.message, errorCode:'GET_STATE_ERROR' }; }
}

async function hCollect(uid, data, _meta={}){
  try{
    const r = await dbGet(`users/${uid}`); const user = r.data;
    if(!user) return { success:false, error:'User not found' };
    syncTank(user); const actual = Math.floor(user.tankAccrued);
    if(actual<1) return { success:false, error:'Tank is empty' };
    const nb = (user.bamboo||0)+actual;
    await dbUpdate(`users/${uid}`,{bamboo:nb,totalEarned:(user.totalEarned||0)+actual,tankAccrued:user.tankAccrued-actual,lastSeen:user.lastSeen});
    log(uid,'collect',{collected:actual,bamboo_before:(user.bamboo||0),bamboo_after:nb,tankLevel:user.tankLevel||1},_meta);
    return { success:true, data:{ collected:actual, bamboo:nb } };
  }catch(e){ return { success:false, error:e.message }; }
}

async function hBuyItem(uid, data, _meta={}){
  try{
    const { itemId, qty=1 } = data; const item = G.ITEMS[itemId];
    if(!item) return { success:false, error:'Unknown item' };
    const q = Math.max(1,Math.min(10,parseInt(qty)||1)); const total = item.price*q;
    const r = await dbGet(`users/${uid}`); const user = r.data;
    if(!user) return { success:false, error:'User not found' };
    if((user.bamboo||0)<total) return { success:false, error:'Not enough Bamboo' };
    const machines = user.machines||{}; machines[itemId]=(machines[itemId]||0)+q;
    const newRate  = recalcRate(machines); const nb = (user.bamboo||0)-total;
    await dbUpdate(`users/${uid}`,{bamboo:nb,machines,miningRate:newRate});
    log(uid,'buy_item',{itemId,qty:q,totalCost:total,bamboo_before:(user.bamboo||0),bamboo_after:nb,miningRate_before:user.miningRate||0,miningRate_after:newRate},_meta);
    if(user.referredBy && user.referredBy!==uid){
      const comm = Math.floor(total*G.REF_BONUS_PCT/100);
      const rr   = await dbGet(`users/${user.referredBy}`);
      if(rr.data){
        await dbUpdate(`users/${user.referredBy}`,{bamboo:(rr.data.bamboo||0)+comm});
        await dbPush(`users/${user.referredBy}/referralEarnings`,{fromUserId:uid,amount:comm,timestamp:Date.now()});
        await dbUpdate(`users/${user.referredBy}/referrals/${uid}`,{earned:(rr.data.referrals?.[uid]?.earned||0)+comm});
        log(user.referredBy,'referral_commission',{fromUserId:uid,commission:comm,bamboo_before:(rr.data.bamboo||0),bamboo_after:(rr.data.bamboo||0)+comm});
        const buyerName = (user.firstName||'Your friend').slice(0,32);
        const notifMsg  = `💰 <b>Commission earned!</b>\n\n<b>${buyerName}</b> made a purchase from the Market\nYou earned <b>${comm} Bamboo</b> (20% commission) 🎋\n\n<i>Your balance has been updated automatically</i>`;
        sendTgNotification(user.referredBy, notifMsg).catch(()=>{});
      }
    }
    return { success:true, data:{ bamboo:nb, miningRate:newRate, machines } };
  }catch(e){ return { success:false, error:e.message }; }
}

async function hUpgradeTank(uid, data, _meta={}){
  try{
    const r = await dbGet(`users/${uid}`); const user = r.data;
    if(!user) return { success:false, error:'User not found' };
    const cur = user.tankLevel||1; const next = cur+1;
    if(next>G.MAX_TANK_LVL) return { success:false, error:'Max level' };
    if(parseInt(data.newLevel)!==next) return { success:false, error:'Level mismatch' };
    const cost = G.TANK[next].upgCost;
    if((user.bamboo||0)<cost) return { success:false, error:'Not enough Bamboo' };
    const nb = (user.bamboo||0)-cost;
    await dbUpdate(`users/${uid}`,{bamboo:nb,tankLevel:next});
    log(uid,'upgrade_tank',{tankLevel_before:cur,tankLevel_after:next,cost,bamboo_before:(user.bamboo||0),bamboo_after:nb,newCap:G.TANK[next].cap,coins_balance:user.coins||0,miningRate:user.miningRate||0},_meta);
    return { success:true, data:{ tankLevel:next, bamboo:nb } };
  }catch(e){ return { success:false, error:e.message }; }
}

async function hExchange(uid, data, _meta={}){
  try{
    const lockKey = `exchangeLocks/${uid}`;
    const lockRec = await dbGet(lockKey);
    const now     = Date.now();
    if(lockRec.data && (now-(lockRec.data.ts||0))<15000) return { success:false, error:'Exchange in progress. Please wait.' };
    await dbSet(lockKey,{ts:now});
    try{
      const r = await dbGet(`users/${uid}`); const user = r.data;
      if(!user){ await dbSet(lockKey,{ts:0}); return { success:false, error:'User not found' }; }
      if(data.coinsAmount!==undefined){ await dbSet(lockKey,{ts:0}); return { success:false, error:'Coins to Bamboo exchange is disabled' }; }
      if(data.bambooAmount===undefined){ await dbSet(lockKey,{ts:0}); return { success:false, error:'Specify bambooAmount' }; }
      let nb=user.bamboo||0, nc=user.coins||0;
      if(typeof data.bambooAmount !== 'number' && typeof data.bambooAmount !== 'string') { await dbSet(lockKey,{ts:0}); return { success:false, error:'Invalid bambooAmount type' }; }
      const bam = Math.floor(parseInt(data.bambooAmount)||0);
      if(bam<G.BAMBOO_PER_COIN){ await dbSet(lockKey,{ts:0}); return { success:false, error:`Min ${G.BAMBOO_PER_COIN} Bamboo` }; }
      if(nb<bam){ await dbSet(lockKey,{ts:0}); return { success:false, error:'Not enough Bamboo' }; }
      const coins = Math.floor(bam/G.BAMBOO_PER_COIN);
      nb-=bam; nc+=coins;
      const entry = { bam, coins, dir:'B→C', ts:now };
      await dbUpdate(`users/${uid}`,{bamboo:nb,coins:nc});
      await dbPush(`users/${uid}/exchHistory`,entry);
      log(uid,'exchange',{bamboo_spent:bam,coins_received:coins,bamboo_before:user.bamboo||0,bamboo_after:nb,coins_before:user.coins||0,coins_after:nc},_meta);
      await dbSet(lockKey,{ts:0});
      return { success:true, data:{ bamboo:nb, coins:nc, entry } };
    }catch(innerErr){ await dbSet(lockKey,{ts:0}).catch(()=>{}); throw innerErr; }
  }catch(e){ return { success:false, error:e.message }; }
}

async function hWithdraw(uid, data, _meta={}){
  try{
    const addr = (data.address||'').trim(); const amt = parseFloat(data.amount)||0;
    if(!addr||addr.length<10) return { success:false, error:'Invalid TON address' };
    if(!isValidAddress(addr))  return { success:false, error:'Invalid address format' };
    if(!isFinite(amt)||amt<G.MIN_WITHDRAW) return { success:false, error:`Min ${G.MIN_WITHDRAW} Coins` };
    if(amt>1000000)           return { success:false, error:'Amount too large' };
    const lockKey = `withdrawLocks/${uid}`;
    const lockRec = await dbGet(lockKey);
    const now     = Date.now();
    if(lockRec.data && (now-(lockRec.data.ts||0))<60000) return { success:false, error:'A withdrawal is already being processed. Please wait 60 seconds.' };
    await dbSet(lockKey,{ts:now,uid});
    try{
      const r = await dbGet(`users/${uid}`); const user = r.data;
      if(!user){ await dbSet(lockKey,{ts:0}); return { success:false, error:'User not found' }; }
      if((user.coins||0)<amt){ await dbSet(lockKey,{ts:0}); return { success:false, error:'Not enough Coins' }; }
      if((now-(user._lastWdTs||0))<60000){ await dbSet(lockKey,{ts:0}); return { success:false, error:'Please wait 60 seconds before next withdrawal' }; }
      if(!user.hasDeposited){
        const fp = (data.deviceFingerprint||'').trim();
        if(fp && fp.length>8){
          const safeKey = fp.replace(/[^a-zA-Z0-9_-]/g,'_').slice(0,120);
          const fpRec   = await dbGet(`deviceFingerprints/${safeKey}`);
          if(fpRec.data && fpRec.data.uid && fpRec.data.uid!==uid){
            await dbSet(lockKey,{ts:0});
            await dbSet(`flaggedWithdrawals/fp_${uid}_${now}`,{userId:uid,reason:'duplicate_device',fingerprint:fp.slice(0,40),existingUser:fpRec.data.uid,amount:amt,ts:now});
            return { success:false, error:'MULTI_ACCOUNT', errorCode:'MULTI_ACCOUNT' };
          }
          if(!fpRec.data) await dbSet(`deviceFingerprints/${safeKey}`,{uid,ts:now});
        }
      }
      const tpr          = await dbGet('tasks/partner');
      const partnerTasks = tpr.data?Object.values(tpr.data).filter(t=>t.status==='active'):[];
      const completedTasks  = user.completedTasks||[];
      const missingPartner  = partnerTasks.filter(t=>!completedTasks.includes(t.id));
      if(missingPartner.length>0){ await dbSet(lockKey,{ts:0}); return { success:false, error:'Complete all partner tasks first', errorCode:'PARTNER_TASKS_REQUIRED', missing:missingPartner.length }; }
      const wdId = `wd_${uid}_${now}`; const ton = amt*G.TON_PER_COIN;
      const upd  = { coins:(user.coins||0)-amt, _lastWdTs:now };
      await dbUpdate(`users/${uid}`,upd);
      const rec = { wdId, userId:uid, address:addr, amt, ton, status:'pending', ts:now };
      await dbSet(`users/${uid}/wdHistory/${wdId}`,rec);
      await dbSet(`withdrawQueue/${wdId}`,rec);
      log(uid,'withdraw_request',{wdId,amount_coins:amt,amount_ton:ton,address:addr,coins_before:(user.coins||0),coins_after:upd.coins},_meta);
      await dbSet(lockKey,{ts:0});
      return { success:true, data:{ wdId, coins:upd.coins, status:'pending' } };
    }catch(innerErr){ await dbSet(lockKey,{ts:0}).catch(()=>{}); throw innerErr; }
  }catch(e){ return { success:false, error:e.message }; }
}

async function hDeposit(uid, data, _meta={}){
  try{
    const amt    = parseFloat(data.amount)||0;
    const txHash = (data.txHash||'').slice(0,256).trim();
    if(!txHash||txHash.length<10) return { success:false, error:'Invalid txHash' };
    if(!isFinite(amt)||amt<G.MIN_DEPOSIT_TON) return { success:false, error:'Invalid deposit data' };
    const safeHash = txHash.replace(/[^a-zA-Z0-9]/g,'_');
    const dup      = await dbGet(`txHashes/${safeHash}`);
    if(dup.data) return { success:false, error:'Duplicate transaction' };
    const depId = `dep_${uid}_${Date.now()}`;
    const rec   = { depId, userId:uid, txHash, amount:amt, status:'pending', ts:Date.now() };
    const ur    = await dbGet(`users/${uid}`); const u = ur.data||{};
    await dbSet(`users/${uid}/deposits/${depId}`,rec);
    await dbSet(`pendingDeposits/${depId}`,rec);
    await dbSet(`txHashes/${safeHash}`,{depId,userId:uid,ts:Date.now()});
    log(uid,'deposit_initiated',{depId,txHash,amount_ton:amt,bamboo_before:(u.bamboo||0),coins_before:(u.coins||0),tonBalance_before:(u.tonBalance||0)},_meta);
    return { success:true, data:{ depositId:depId, message:'Transaction registered. Your balance will be added within 3 minutes.' } };
  }catch(e){ return { success:false, error:e.message }; }
}

async function hClaimTask(uid, data, _meta={}){
  try{
    const tid     = data.taskId;
    const lockKey = `taskLocks/${uid}_${tid}`;
    const lockRec = await dbGet(lockKey);
    const now     = Date.now();
    if(lockRec.data && (now-(lockRec.data.ts||0))<30000) return { success:false, error:'Already processing. Please wait.' };
    await dbSet(lockKey,{ts:now});
    try{
      const r = await dbGet(`users/${uid}`); const user = r.data;
      if(!user){ await dbSet(lockKey,{ts:0}); return { success:false, error:'User not found' }; }
      if((user.completedTasks||[]).includes(tid)){ await dbSet(lockKey,{ts:0}); return { success:false, error:'Already claimed' }; }
      let bam=0, coins=0;
      if(G.REF_TASKS[tid]){
        const t  = G.REF_TASKS[tid];
        const rr = await dbGet(`users/${uid}/referrals`);
        const rc = rr.data?Object.keys(rr.data).length:0;
        if(rc<t.n){ await dbSet(lockKey,{ts:0}); return { success:false, error:`Need ${t.n} referrals (have ${rc})` }; }
        bam=t.bam; coins=t.coins;
      }else if(G.REF_ACTIVE_TASKS[tid]){
        const t      = G.REF_ACTIVE_TASKS[tid];
        const rr     = await dbGet(`users/${uid}/referrals`);
        const refIds = rr.data?Object.keys(rr.data):[];
        let activeCount = 0;
        for(const refId of refIds){ const hdR=await dbGet(`users/${refId}/hasDeposited`); if(hdR.data===true) activeCount++; }
        if(activeCount<t.n){ await dbSet(lockKey,{ts:0}); return { success:false, error:`Need ${t.n} active referrals who deposited (have ${activeCount})` }; }
        bam=t.bam; coins=t.coins;
      }else if(G.SOC_TASKS[tid]){ bam=G.SOC_TASKS[tid]; }
      else{ await dbSet(lockKey,{ts:0}); return { success:false, error:'Unknown task' }; }
      const nb=(user.bamboo||0)+bam; const nc=(user.coins||0)+coins;
      await dbUpdate(`users/${uid}`,{completedTasks:[...(user.completedTasks||[]),tid],bamboo:nb,coins:nc});
      log(uid,'claim_task',{taskId:tid,bamboo_reward:bam,coins_reward:coins,bamboo_before:user.bamboo||0,bamboo_after:nb,coins_before:user.coins||0,coins_after:nc},_meta);
      await dbSet(lockKey,{ts:0});
      return { success:true, data:{ bamboo:nb, coins:nc, bam, coins } };
    }catch(innerErr){ await dbSet(lockKey,{ts:0}).catch(()=>{}); throw innerErr; }
  }catch(e){ return { success:false, error:e.message }; }
}

async function hVerifyTask(uid, data, _meta={}){
  try{
    const { taskId, taskType, taskCategory } = data;
    if(!taskId||typeof taskId!=='string'||taskId.length>100) return { success:false, error:'Invalid taskId' };
    const cat = taskCategory||'community';
    let tr = await dbGet(`tasks/${cat}/${taskId}`);
    let task=tr.data, taskCat=cat;
    if(!task){
      const other = cat==='community'?'partner':'community';
      tr=await dbGet(`tasks/${other}/${taskId}`); task=tr.data; taskCat=other;
    }
    if(!task) return { success:false, error:'Task not found' };
    if(task.status!=='active') return { success:false, error:'Task is no longer active' };
    const ur = await dbGet(`users/${uid}`); const u=ur.data||{};
    if((u.completedTasks||[]).includes(taskId)) return { success:false, error:'Task already completed' };
    if((task.completedBy||[]).includes(uid))    return { success:false, error:'Task already completed' };
    if(task.type==='channel'){
      const isMember = await checkMembership(uid, task.link);
      if(!isMember) return { success:false, error:'Not a member of the channel. Join first then try again!' };
    }
    const bam           = task.bambooReward||500;
    const newCompletions  = (task.completions||0)+1;
    const newCompletedBy  = [...(task.completedBy||[]),uid];
    const taskUpdates   = { completions:newCompletions, completedBy:newCompletedBy, updatedAt:Date.now() };
    if(task.targetUsers!=null && newCompletions>=(task.targetUsers||Infinity)) taskUpdates.status='completed';
    await dbUpdate(`tasks/${taskCat}/${taskId}`,taskUpdates);
    const newCompleted = [...(u.completedTasks||[]),taskId];
    await dbUpdate(`users/${uid}`,{completedTasks:newCompleted,bamboo:(u.bamboo||0)+bam});
    log(uid,'verify_task',{taskId,taskType:task.type,taskCategory:taskCat,bamboo_reward:bam,bamboo_before:(u.bamboo||0),bamboo_after:(u.bamboo||0)+bam},_meta);
    return { success:true, data:{ bambooAdded:bam, completions:newCompletions } };
  }catch(e){ console.error('verifyTask:',e); return { success:false, error:e.message }; }
}

async function hCreateTask(uid, data, _meta={}){
  try{
    const { type, link, targetUsers } = data;
    if(!['channel','bot'].includes(type)) return { success:false, error:'Invalid type. Must be channel or bot' };
    const target = parseInt(targetUsers)||0;
    if(target<100)    return { success:false, error:'Minimum target is 100 users' };
    if(target>100000) return { success:false, error:'Maximum target is 100,000 users' };
    if(!link||!link.includes('t.me/')) return { success:false, error:'Valid Telegram link required' };
    const COINS_PER_USER = 60;
    const cost = target*COINS_PER_USER;
    const ur   = await dbGet(`users/${uid}`); const u = ur.data;
    if(!u) return { success:false, error:'User not found' };
    if((u.coins||0)<cost) return { success:false, error:`Insufficient Coins. Need ${cost} Coins` };
    await dbUpdate(`users/${uid}`,{coins:(u.coins||0)-cost});
    const username = link.split('t.me/')[1]?.split('?')[0]?.split('/')[0]||link;
    const now      = Date.now();
    const taskId   = `task_${now}_${Math.random().toString(36).substring(2,10)}`;
    const taskData = {
      id:taskId, creatorId:uid, type, link, name:`@${username}`,
      targetUsers:target, bambooReward:500,
      completions:0, completedBy:[], status:'active',
      createdAt:now, expiresAt:now+(30*24*60*60*1000), updatedAt:now,
    };
    await dbSet(`tasks/community/${taskId}`,taskData);
    await dbPush(`users/${uid}/transactions`,{type:'create_task',taskId,taskType:type,targetUsers:target,cost,coinsCost:cost,timestamp:now});
    log(uid,'create_task',{taskId,taskType:type,targetUsers:target,coins_spent:cost,coins_before:(u.coins||0)+cost,coins_after:(u.coins||0),taskLink:link},_meta);
    return { success:true, data:{ taskId, type, targetUsers:target, totalCost:cost, bambooReward:500 } };
  }catch(e){ console.error('createTask:',e); return { success:false, error:e.message }; }
}

async function hGetLeaderboard(uid, _meta={}){
  try{
    const COMP_DURATION_MS = 10*24*60*60*1000;
    let meta    = (await dbGet('competition/meta')).data;
    const nowMs = Date.now();
    if(!meta||!meta.endDate||!meta.startDate){
      meta = { startDate:meta?.startDate||nowMs, endDate:meta?.endDate||(nowMs+COMP_DURATION_MS) };
      await dbSet('competition/meta',meta);
    }
    if(nowMs>meta.endDate){
      meta = { startDate:nowMs, endDate:nowMs+COMP_DURATION_MS };
      await dbSet('competition/meta',meta);
    }
    const lbr  = await dbGet('competition/leaderboard');
    const lb   = lbr.data||{ activeRefs:[], miningSpeed:[] };
    const snap = (await dbGet(`competition/snapshots/${uid}`)).data||null;
    return { success:true, data:{ endDate:meta.endDate, startDate:meta.startDate, activeRefs:lb.activeRefs||[], miningSpeed:lb.miningSpeed||[], mySnapshot:snap } };
  }catch(e){ return { success:false, error:e.message }; }
}

async function hSaveSeasonAlloc(uid, data={}){
  try{
    const { coinsAlloc=0, refsAlloc=0, compAlloc=0, compRank=0, compTon=0, total=0, totalTon='0' } = data;
    const rec = { uid, coinsAlloc, refsAlloc, compAlloc, compRank, compTon, total, totalTon, updatedAt:Date.now() };
    await dbSet(`season2/alloc/${uid}`,rec);
    return { success:true };
  }catch(e){ return { success:false, error:e.message }; }
}

async function hGetSeasonAlloc(uid){
  try{
    const r = await dbGet(`season2/alloc/${uid}`);
    return { success:true, data:r.data||null };
  }catch(e){ return { success:false, error:e.message }; }
}

async function hAdmin(action, data){
  switch(action){
    case 'adminGetUser':{ const r=await dbGet(`users/${data.userId}`); return { success:true, data:r.data||null }; }
    case 'adminSetBalance':{
      const r = await dbGet(`users/${data.userId}`); if(!r.data) return { success:false, error:'Not found' };
      const u = {};
      if(data.bamboo!==undefined)     u.bamboo     = Math.max(0,parseFloat(data.bamboo));
      if(data.coins!==undefined)      u.coins      = Math.max(0,parseFloat(data.coins));
      if(data.tonBalance!==undefined) u.tonBalance = Math.max(0,parseFloat(data.tonBalance));
      await dbUpdate(`users/${data.userId}`,u);
      log(data.userId,'admin_set_balance',{bamboo_set:data.bamboo,coins_set:data.coins,ton_set:data.tonBalance,bamboo_before:r.data.bamboo||0,coins_before:r.data.coins||0,by:'admin'});
      return { success:true };
    }
    case 'adminConfirmDeposit':{
      const dep = await dbGet(`users/${data.userId}/deposits/${data.depositId}`);
      if(!dep.data) return { success:false, error:'Not found' };
      const ton = parseFloat(data.amount||dep.data.amount); const bamboo = Math.floor(ton*G.TON_TO_BAMBOO);
      await dbUpdate(`users/${data.userId}/deposits/${data.depositId}`,{status:'completed',completedAt:Date.now()});
      const u = await dbGet(`users/${data.userId}`);
      if(u.data) await dbUpdate(`users/${data.userId}`,{bamboo:(u.data.bamboo||0)+bamboo,tonBalance:(u.data.tonBalance||0)+ton,hasDeposited:true});
      if(u.data?.referredBy) await dbUpdate(`users/${u.data.referredBy}/referrals/${data.userId}`,{hasDeposited:true}).catch(()=>{});
      await dbDelete(`pendingDeposits/${data.depositId}`);
      log(data.userId,'admin_confirm_deposit',{depositId:data.depositId,amount_ton:ton,bamboo_added:bamboo,by:'admin'});
      return { success:true, data:{ bambooAdded:bamboo } };
    }
    case 'adminApproveWithdraw':{
      await dbUpdate(`users/${data.userId}/wdHistory/${data.wdId}`,{status:'approved',approvedAt:Date.now()});
      await dbDelete(`withdrawQueue/${data.wdId}`);
      return { success:true };
    }
    case 'adminRejectWithdraw':{
      const wd = await dbGet(`users/${data.userId}/wdHistory/${data.wdId}`);
      if(!wd.data) return { success:false, error:'Not found' };
      await dbUpdate(`users/${data.userId}/wdHistory/${data.wdId}`,{status:'rejected',rejectedAt:Date.now()});
      if(data.refund){ const u=await dbGet(`users/${data.userId}`); if(u.data) await dbUpdate(`users/${data.userId}`,{coins:(u.data.coins||0)+(wd.data.amt||0)}); }
      await dbDelete(`withdrawQueue/${data.wdId}`);
      return { success:true };
    }
    case 'adminGetQueue':{
      const w = await dbGet('withdrawQueue'); const d = await dbGet('pendingDeposits');
      return { success:true, data:{ withdrawals:w.data?Object.values(w.data):[], deposits:d.data?Object.values(d.data):[] } };
    }
    default: return { success:false, error:'Unknown admin action' };
  }
}

// ── Routes ────────────────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({ success:true, data:{ status:'ok', ts:Date.now(), env:env.ENVIRONMENT } });
});

app.get('/tonconnect-manifest.json', (_req, res) => {
  res.json({
    url:'https://pandabambo.vercel.app',
    name:'PandaBambooBot',
    iconUrl:'https://i.supaimg.com/ec27537b-aa6a-42cf-8ba1-d6850eeea36d/87e9d1bd-c053-466a-a29e-40483a009e8f.png',
    description:'Panda Bamboo Factory',
  });
});

app.post('/api', async (req, res) => {
  // IP
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress || 'unknown';
  if(!rateOk(ip)) return res.status(429).json({ success:false, error:'Rate limit exceeded' });

  // Parse body
  let body;
  try{
    const raw = req.body;
    if(!raw || raw.length > 10240) return res.status(413).json({ success:false, error:'Payload too large' });
    body = JSON.parse(sanitise(raw));
  }catch(_){ return res.status(400).json({ success:false, error:'Invalid JSON' }); }

  const authHeader = req.headers['authorization'] || '';
  const action     = req.headers['x-action'] || body.action;
  const data       = body.data || {};
  if(!action) return res.status(400).json({ success:false, error:'Missing action' });

  // Admin actions
  const ADMIN_ACTIONS = new Set(['adminGetUser','adminSetBalance','adminConfirmDeposit','adminApproveWithdraw','adminRejectWithdraw','adminGetQueue']);
  if(ADMIN_ACTIONS.has(action)){
    const v = await validateTg(authHeader.replace('Telegram ',''), env.BOT_TOKEN);
    if(!v.valid) return res.status(401).json({ success:false, error:'Unauthorized' });
    const adminIds = (env.ADMIN_IDS||'').split(',').map(s=>s.trim());
    if(!adminIds.includes(String(v.user?.id))) return res.status(403).json({ success:false, error:'Forbidden' });
    return res.json(await hAdmin(action, data));
  }

  // Ping (no auth)
  if(action==='ping') return res.json({ success:true, data:{ pong:true, ts:Date.now() } });

  // Telegram auth
  if(!authHeader.startsWith('Telegram ')) return res.status(401).json({ success:false, error:'Telegram authentication required' });
  const v = await validateTg(authHeader.replace('Telegram ',''), env.BOT_TOKEN);
  if(!v.valid){
    console.error('TG validation failed:', v.error);
    return res.status(401).json({
      success:false, error:'Invalid Telegram authentication', errorCode:'INVALID_TELEGRAM_AUTH',
      debug:{ hasInitData:!!authHeader, botTokenConfigured:!!env.BOT_TOKEN, environment:env.ENVIRONMENT, validationError:v.error }
    });
  }

  const uid   = String(v.user.id);
  const _meta = { ip, ua:req.headers['user-agent']||'' };
  console.log(`[${new Date().toISOString()}] User:${uid} Action:${action} IP:${ip}`);

  if(!userActionOk(uid, action)) return res.status(429).json({ success:false, error:'Too fast. Please wait a moment before trying again.' });

  let result;
  switch(action){
    case 'getState'       : result = await hGetState      (uid, v.user, {...data,_startParam:v.startParam||''}, _meta); break;
    case 'collect'        : result = await hCollect       (uid, data, _meta); break;
    case 'buyItem'        : result = await hBuyItem       (uid, data, _meta); break;
    case 'upgradeTank'    : result = await hUpgradeTank   (uid, data, _meta); break;
    case 'exchange'       : result = await hExchange      (uid, data, _meta); break;
    case 'withdraw'       : result = await hWithdraw      (uid, data, _meta); break;
    case 'deposit'        : result = await hDeposit       (uid, data, _meta); break;
    case 'claimTask'      : result = await hClaimTask     (uid, data, _meta); break;
    case 'verifyTask'     : result = await hVerifyTask    (uid, data, _meta); break;
    case 'createTask'     : result = await hCreateTask    (uid, data, _meta); break;
    case 'getLeaderboard' : result = await hGetLeaderboard(uid, _meta); break;
    case 'saveSeasonAlloc': result = await hSaveSeasonAlloc(uid, data); break;
    case 'getSeasonAlloc' : result = await hGetSeasonAlloc(uid); break;
    default               : return res.status(400).json({ success:false, error:'Unknown action' });
  }

  const status = result?._status || 200;
  if(result?._status) delete result._status;
  res.status(status).json(result);
});

// ── Start ─────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`🐼 Panda Bamboo Factory server running on port ${PORT}`);
  console.log(`   Firebase: ${env.FIREBASE_DATABASE_URL ? '✅ configured' : '❌ MISSING'}`);
  console.log(`   Bot Token: ${env.BOT_TOKEN ? '✅ configured' : '❌ MISSING'}`);
});
