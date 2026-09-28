import express from 'express';
import { WebSocketServer } from 'ws';
import admin from 'firebase-admin';
import crypto from 'crypto';
import http from 'http';

const PORT           = Number(process.env.PORT || 8080);
const BOT_TOKEN      = process.env.BOT_TOKEN || '';
const SA_JSON        = process.env.FIREBASE_SERVICE_ACCOUNT || '';
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
const DEV_ALLOW_ANY  = (process.env.NODE_ENV || 'development') !== 'production';
const ADMIN_KEY       = process.env.ADMIN_KEY || '';
const ADMIN_USERS     = new Set((process.env.ADMIN_USERS || '').split(',').map(s => s.trim()).filter(Boolean));

if (!SA_JSON) { console.error('FATAL: FIREBASE_SERVICE_ACCOUNT missing'); process.exit(1); }

const serviceAccount = JSON.parse(SA_JSON);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://renewed-8fe38-default-rtdb.firebaseio.com',
});
const db = admin.database();
const SV = admin.database.ServerValue;

const SELECTION_SECONDS = 30;
const SPINNING_SECONDS  = 2;
const RESULTS_SECONDS   = 5;
const MAX_PICKS         = 3;
const PAYOUT_RATE       = 0.80;
const REFERRAL_RATE     = 0.05;

const BOT_NAMES = [
  'Dawit','Abel','Yonas','Biruk','Tesfaye','Mulugeta','Getachew','Henok','Ermias','Amanuel',
  'Solomon','Fikru','Samuel','Kebede','Bereket','Natnael','Tadesse','Mekonnen','Tewodros','Gebre',
  'Hailu','Abebe','Adane','Alemu','Amare','Andualem','Ayele','Bekele','Binyam','Daniel',
  'Demeke','Dereje','Desalegn','Desta','Endale','Esubalew','Fikadu','Gebremedhin','Girmay','Girma',
  'Habtamu','Hussen','Ibrahim','Kifle','Lemma','Melaku','Mengistu','Mesfin','Negash','Nega',
  'Tsegaye','Wondimu','Worku','Yohannes','Yosef','Zerihun','Zewdu','Abdi','Adugna','Afework',
  'Agumas','Alemayehu','Alula','Assefa','Ayalew','Balcha','Belete','Bogale','Chalachew','Dagne',
  'Damtew','Dires','Ewnetu','Fantahun','Fisseha','Gashaw','Gedamu','Getu','Gizaw','Goitom',
  'Goshu','Gudeta','Haile','Hiruy','Isayas','Kassa','Kefelegn','Kiflom','Lema','Melese',
  'Mihret','Million','Molla','Mulatu','Negasi','Nigussie','Petros','Robel','Sisay','Tekle'
];
const BOT_START_WALLET = 1_000_000;
const BOT_MIN = 3;
const BOT_MAX = 6;

function validateInitData(initData, maxAge = 86400) {
  try {
    const p = new URLSearchParams(initData);
    const hash = p.get('hash');
    if (!hash) return null;
    p.delete('hash');
    const dcs = [...p.entries()].map(([k, v]) => `${k}=${v}`).sort().join('\n');
    const secret = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const mac = crypto.createHmac('sha256', secret).update(dcs).digest('hex');
    const a = Buffer.from(mac, 'hex'), b = Buffer.from(hash, 'hex');
    if (a.length !== b.length) return null;
    if (!crypto.timingSafeEqual(a, b)) return null;
    const authDate = Number(p.get('auth_date') || 0);
    if (!authDate || Date.now() / 1000 - authDate > maxAge) return null;
    const u = p.get('user');
    return u ? JSON.parse(u) : null;
  } catch { return null; }
}

class Mutex {
  constructor() { this._t = Promise.resolve(); }
  run(fn) { const n = this._t.then(fn, fn); this._t = n.then(() => {}, () => {}); return n; }
}

class Room {
  constructor(id, bet) {
    this.id = id;
    this.bet = bet;
    this.phase = 'selection';
    this.round = 1;
    this.endsAt = Date.now() + SELECTION_SECONDS * 1000;
    this.players = new Map();
    this.taken = new Map();
    this.winningNumber = null;
    this.winners = [];
    this.prizePool = 0;
    this.mutex = new Mutex();
    this.botTimers = [];
    this.dirty = true;
    this.lastResult = null;
    this.subscribers = new Set();
  }
  snapshot() {
    const players = {};
    for (const [n, p] of this.players) {
      players[n] = { name: n, picks: [...p.picks], isBot: !!p.isBot, bet: this.bet };
    }
    return {
      gameState: this.phase,
      round: this.round,
      betAmount: this.bet,
      selectionEndsAt: this.endsAt,
      numbersTaken: Object.fromEntries([...this.taken.keys()].map(n => [n, true])),
      players,
      winners: [...this.winners],
      winningNumber: this.winningNumber,
      prizePool: this.prizePool,
      potAmount: this.prizePool,
      totalBets: this.players.size * this.bet,
      serverNow: Date.now(),
      endsAt: this.endsAt,
      lastResult: this.lastResult,
    };
  }
  async push() {
    const snap = this.snapshot();
    await db.ref('rooms/' + this.id).set(snap).catch(() => {});
    const msg = JSON.stringify({ type: 'state', room: this.id, state: snap });
    for (const ws of this.subscribers) {
      if (ws.readyState === 1) { try { ws.send(msg); } catch {} }
    }
    this.dirty = false;
  }
}

const rooms = {
  room_15: new Room('room_15', 15),
  room_30: new Room('room_30', 30),
};

async function debit(uid, amount, roomId, round) {
  const r = await db.ref('users/' + uid).transaction(u => {
    if (!u) return u;
    u.playWallet = Number(u.playWallet || 0);
    if (u.playWallet < amount) return;
    u.playWallet -= amount;
    const pw = u.pendingWager || {};
    const same = pw.room === roomId && Number(pw.round) === Number(round);
    u.pendingWager = { room: roomId, round, amount: (same ? Number(pw.amount || 0) : 0) + amount, ts: Date.now() };
    return u;
  });
  return !!(r && r.committed);
}

async function refundWager(uid, amount, roomId, round) {
  const r = await db.ref('users/' + uid).transaction(u => {
    if (!u) return u;
    u.playWallet = Number(u.playWallet || 0) + amount;
    const pw = u.pendingWager || {};
    if (pw.room === roomId && Number(pw.round) === Number(round)) {
      const left = Number(pw.amount || 0) - amount;
      if (left <= 0) delete u.pendingWager;
      else u.pendingWager = { ...pw, amount: left };
    }
    return u;
  });
  return !!(r && r.committed);
}

async function credit(uid, amount, roomId, round, kind) {
  const key = `${roomId}_${round}_${uid}_${kind}`.replace(/[.#$/\[\]]/g, '_');
  const r = await db.ref('users/' + uid).transaction(u => {
    if (!u) return u;
    u.mainWallet = Number(u.mainWallet || 0);
    u.gameWinPayouts = u.gameWinPayouts || {};
    if (u.gameWinPayouts[key]) return u;
    u.mainWallet += amount;
    u.gameWinPayouts[key] = { room: roomId, round, amount, kind, ts: Date.now() };
    return u;
  });
  return !!(r && r.committed);
}

async function transferMainToPlay(uid, amount) {
  const r = await db.ref('users/' + uid).transaction(u => {
    if (!u) return u;
    const main = Number(u.mainWallet || 0);
    const pending = Number(u.pendingWithdrawal || 0);
    if (main - pending < amount) return;
    u.mainWallet = main - amount;
    u.playWallet = Number(u.playWallet || 0) + amount;
    return u;
  });
  return !!(r && r.committed);
}

async function pickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok: false, reason: 'not-selection' };
    if (!(num >= 1 && num <= 200))  return { ok: false, reason: 'range' };
    let p = room.players.get(uid);
    if (!p) { p = { uid, picks: [], isBot: false }; room.players.set(uid, p); }
    if (p.picks.length >= MAX_PICKS) return { ok: false, reason: 'max-picks' };
    if (p.picks.includes(num))       return { ok: false, reason: 'duplicate' };
    if (room.taken.has(num))         return { ok: false, reason: 'taken' };
    room.taken.set(num, uid);
    const ok = await debit(uid, room.bet, room.id, room.round);
    if (!ok) { room.taken.delete(num); return { ok: false, reason: 'insufficient' }; }
    p.picks.push(num);
    room.dirty = true;
    return { ok: true };
  });
}

async function unpickIntent(room, uid, num) {
  return room.mutex.run(async () => {
    if (room.phase !== 'selection') return { ok: false, reason: 'not-selection' };
    const p = room.players.get(uid);
    if (!p || !p.picks.includes(num)) return { ok: false, reason: 'not-picked' };
    if (room.taken.get(num) !== uid)  return { ok: false, reason: 'not-owner' };
    const ok = await refundWager(uid, room.bet, room.id, room.round);
    if (!ok) return { ok: false, reason: 'refund-failed' };
    p.picks = p.picks.filter(n => n !== num);
    room.taken.delete(num);
    room.dirty = true;
    return { ok: true };
  });
}

async function toSpinning(room) {
  if (room.phase !== 'selection') return;
  const withPicks = [...room.players.values()].filter(p => p.picks.length > 0);
  const humans = withPicks.filter(p => !p.isBot);
  if (!humans.length) return resetRound(room, 'no-humans');
  const nums = [...room.taken.keys()];
  if (!nums.length) return resetRound(room, 'no-picks');
  const winningNumber = nums[Math.floor(Math.random() * nums.length)];
  const winners = withPicks.filter(p => p.picks.includes(winningNumber)).map(p => p.uid);
  const totalPicks = withPicks.reduce((s, p) => s + p.picks.length, 0);
  const prizePool = Math.floor(totalPicks * room.bet * PAYOUT_RATE);
  room.winningNumber = winningNumber;
  room.winners = winners;
  room.prizePool = prizePool;
  room.phase = 'spinning';
  room.endsAt = Date.now() + SPINNING_SECONDS * 1000;
  room.dirty = true;
  for (const p of withPicks) {
    if (!p.isBot) db.ref('users/' + p.uid + '/pendingWager').remove().catch(() => {});
  }
  if (winners.length) {
    const share = Math.floor(prizePool / winners.length);
    await Promise.all(winners.map(async uid => {
      if (!await credit(uid, share, room.id, room.round, 'win')) return;
      db.ref('notifications/' + uid).push({
        message: `🏆 You won ${share} Birr in round ${room.round}!`,
        timestamp: Date.now(), read: false,
      }).catch(() => {});
      const snap = await db.ref('users/' + uid + '/referredBy').once('value');
      const ref = snap.val();
      if (ref && ref !== uid) {
        const comm = Math.floor(share * REFERRAL_RATE);
        if (comm > 0 && await credit(ref, comm, room.id, room.round, 'ref_' + uid)) {
          db.ref('notifications/' + ref).push({
            message: `🎁 You earned ${comm} Birr commission from ${uid}'s win.`,
            timestamp: Date.now(), read: false,
          }).catch(() => {});
        }
      }
    }));
  }
  room.lastResult = {
    round: room.round,
    winningNumber,
    winners,
    winAmount: winners.length ? Math.floor(prizePool / winners.length) : 0,
    setAt: Date.now(),
  };
  await db.ref(`rooms/${room.id}/lastResult`).set(room.lastResult).catch(() => {});
  console.log(`[${room.id}] r${room.round} → spin num=${winningNumber} winners=${winners.join(',') || '—'} pool=${prizePool}`);
}

async function toResults(room) {
  if (room.phase !== 'spinning') return;
  room.phase = 'results';
  room.endsAt = Date.now() + RESULTS_SECONDS * 1000;
  room.dirty = true;
}

async function resetRound(room, why) {
  for (const p of room.players.values()) {
    if (p.isBot || !p.picks.length) continue;
    await refundWager(p.uid, room.bet * p.picks.length, room.id, room.round).catch(() => {});
  }
  room.round += 1;
  room.phase = 'selection';
  room.endsAt = Date.now() + SELECTION_SECONDS * 1000;
  room.taken.clear();
  room.winningNumber = null;
  room.winners = [];
  room.prizePool = 0;
  for (const p of room.players.values()) p.picks = [];
  room.dirty = true;
  console.log(`[${room.id}] reset (${why}) → round ${room.round}`);
  scheduleBots(room);
}

function botRoster(count, round) {
  const pool = [...BOT_NAMES];
  const off = ((round - 1) * 7) % pool.length;
  const rot = pool.slice(off).concat(pool.slice(0, off));
  return rot.slice(0, Math.min(count, pool.length));
}

function clearBotTimers(room) {
  room.botTimers.forEach(t => clearTimeout(t));
  room.botTimers = [];
}

function scheduleBots(room) {
  clearBotTimers(room);
  const count = BOT_MIN + (room.round % (BOT_MAX - BOT_MIN + 1));
  const roster = botRoster(count, room.round);
  const windowMs = SELECTION_SECONDS * 1000 - 8000;
  roster.forEach((name, i) => {
    const delay = 2000 + (windowMs / roster.length) * i + Math.random() * 1500;
    const t = setTimeout(async () => {
      await room.mutex.run(async () => {
        if (room.phase !== 'selection') return;
        const ref = db.ref('users/' + name);
        if (!(await ref.once('value')).exists()) {
          await ref.set({
            playWallet: BOT_START_WALLET, mainWallet: 0, pendingWithdrawal: 0,
            isBot: true, bonusClaimed: true,
          }).catch(() => {});
        }
        let p = room.players.get(name);
        if (!p) { p = { uid: name, picks: [], isBot: true }; room.players.set(name, p); }
        for (let k = 0; k < MAX_PICKS; k++) {
          if (room.phase !== 'selection') return;
          const free = [];
          for (let n = 1; n <= 200; n++) if (!room.taken.has(n)) free.push(n);
          if (!free.length) return;
          const num = free[Math.floor(Math.random() * free.length)];
          room.taken.set(num, name);
          p.picks.push(num);
          room.dirty = true;
          await new Promise(r => setTimeout(r, 250 + Math.random() * 700));
        }
      });
    }, delay);
    room.botTimers.push(t);
  });
}

setInterval(async () => {
  for (const room of Object.values(rooms)) {
    await room.mutex.run(async () => {
      const now = Date.now();
      if (room.phase === 'selection' && now >= room.endsAt)      await toSpinning(room);
      else if (room.phase === 'spinning' && now >= room.endsAt)  await toResults(room);
      else if (room.phase === 'results'  && now >= room.endsAt)  await resetRound(room, 'results-done');
    });
    if (room.dirty) await room.push();
  }
}, 500);

const conns = new Map();
const wss = new WebSocketServer({ noServer: true });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.uid = null;
  ws.roomId = null;
  ws.authed = false;
  ws.isAdmin = false;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', async raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    try { await handle(ws, m); } catch (e) { console.error('[WS-MSG]', e); }
  });
  ws.on('close', () => {
    if (ws.uid) {
      const s = conns.get(ws.uid);
      if (s) { s.delete(ws); if (!s.size) conns.delete(ws.uid); }
    }
    if (ws.roomId && rooms[ws.roomId]) rooms[ws.roomId].subscribers.delete(ws);
  });
  ws.on('error', () => {});
});

async function handle(ws, m) {
  if (m.type === 'ping') return ws.send(JSON.stringify({ type: 'pong', t: Date.now() }));

  if (m.type === 'auth') {
    let uid = null;
    const user = validateInitData(m.initData || '');
    if (user && user.username) uid = user.username;
    else if (DEV_ALLOW_ANY && m.devUsername && /^[A-Za-z0-9_]{1,32}$/.test(m.devUsername)) uid = m.devUsername;
    if (!uid) return ws.send(JSON.stringify({ type: 'error', message: 'auth-failed' }));

    const ref = db.ref('users/' + uid);
    if (!(await ref.once('value')).exists()) {
      const u = { mainWallet: 0, playWallet: 30, pendingWithdrawal: 0, bonusClaimed: true };
      if (m.ref && /^[A-Za-z0-9_]{1,32}$/.test(m.ref) && m.ref !== uid) {
        u.referredBy = m.ref; u.referralJoinedAt = SV.TIMESTAMP;
      }
      await ref.set(u).catch(() => {});
    }

    ws.uid = uid;
    ws.authed = true;
    let set = conns.get(uid);
    if (!set) { set = new Set(); conns.set(uid, set); }
    set.add(ws);
    ws.send(JSON.stringify({ type: 'authed', uid }));

    const snap = await db.ref('users/' + uid).once('value');
    const u = snap.val() || {};
    ws.send(JSON.stringify({
      type: 'wallet',
      main: Number(u.mainWallet || 0),
      play: Number(u.playWallet || 0),
      pending: Number(u.pendingWithdrawal || 0),
    }));
    return;
  }

  if (m.type === 'admin-auth') {
    const key = String(m.key || '');
    if (!ADMIN_KEY || key !== ADMIN_KEY) return ws.send(JSON.stringify({ type: 'admin-auth-fail' }));
    ws.isAdmin = true;
    ws.send(JSON.stringify({ type: 'admin-authed' }));
    return;
  }

  if (!ws.authed) return ws.send(JSON.stringify({ type: 'error', message: 'not-authed' }));

  if (m.type === 'join') {
    const room = rooms[m.room];
    if (!room) return ws.send(JSON.stringify({ type: 'error', message: 'bad-room' }));
    if (ws.roomId && rooms[ws.roomId] && ws.roomId !== m.room) {
      rooms[ws.roomId].subscribers.delete(ws);
      rooms[ws.roomId].players.delete(ws.uid);
      rooms[ws.roomId].dirty = true;
    }
    ws.roomId = m.room;
    room.subscribers.add(ws);
    if (!room.players.has(ws.uid)) {
      room.players.set(ws.uid, { uid: ws.uid, picks: [], isBot: false });
    }
    ws.send(JSON.stringify({ type: 'state', room: room.id, state: room.snapshot() }));
    room.dirty = true;
    return;
  }

  if (m.type === 'leave') {
    if (ws.roomId && rooms[ws.roomId]) {
      const room = rooms[ws.roomId];
      room.subscribers.delete(ws);
      room.players.delete(ws.uid);
      room.dirty = true;
    }
    ws.roomId = null;
    return;
  }

  if (m.type === 'pick') {
    const room = rooms[ws.roomId];
    if (!room) return;
    const r = await pickIntent(room, ws.uid, Number(m.number));
    ws.send(JSON.stringify(r.ok
      ? { type: 'pick-ok', number: m.number }
      : { type: 'pick-fail', number: m.number, reason: r.reason }));
    const snap = await db.ref('users/' + ws.uid).once('value');
    const u = snap.val() || {};
    ws.send(JSON.stringify({ type: 'wallet', main: Number(u.mainWallet || 0), play: Number(u.playWallet || 0), pending: Number(u.pendingWithdrawal || 0) }));
    return;
  }

  if (m.type === 'unpick') {
    const room = rooms[ws.roomId];
    if (!room) return;
    const r = await unpickIntent(room, ws.uid, Number(m.number));
    ws.send(JSON.stringify(r.ok
      ? { type: 'unpick-ok', number: m.number }
      : { type: 'unpick-fail', number: m.number, reason: r.reason }));
    const snap = await db.ref('users/' + ws.uid).once('value');
    const u = snap.val() || {};
    ws.send(JSON.stringify({ type: 'wallet', main: Number(u.mainWallet || 0), play: Number(u.playWallet || 0), pending: Number(u.pendingWithdrawal || 0) }));
    return;
  }

  if (m.type === 'transfer') {
    const amount = Number(m.amount);
    if (!Number.isFinite(amount) || amount <= 0) return;
    const ok = await transferMainToPlay(ws.uid, amount);
    const snap = await db.ref('users/' + ws.uid).once('value');
    const u = snap.val() || {};
    ws.send(JSON.stringify({ type: 'wallet', main: Number(u.mainWallet || 0), play: Number(u.playWallet || 0), pending: Number(u.pendingWithdrawal || 0) }));
    if (!ok) ws.send(JSON.stringify({ type: 'error', message: 'transfer-failed' }));
    return;
  }

  if (m.type === 'transaction-request') {
    const type = String(m.txType || '');
    const amount = Number(m.amount);
    if (!['deposit', 'withdraw'].includes(type) || !Number.isFinite(amount) || amount <= 0) {
      return ws.send(JSON.stringify({ type: 'transaction-fail', message: 'invalid-transaction' }));
    }
    const id = String(m.id || crypto.randomUUID());
    const ref = db.ref('transactions/' + id);
    const base = { id, username: ws.uid, type, amount, status: 'pending', timestamp: SV.TIMESTAMP, requestVersion: 4, createdBy: 'server' };
    if (type === 'deposit') { if (m.reference) base.reference = String(m.reference).slice(0, 120); }
    else { base.fullName = String(m.fullName || '').slice(0, 120); base.phoneNumber = String(m.phoneNumber || '').slice(0, 40); }
    if (type === 'withdraw') {
      const reserved = await db.ref('users/' + ws.uid).transaction(u => {
        if (!u) return u;
        const main = Number(u.mainWallet || 0);
        const pending = Number(u.pendingWithdrawal || 0);
        if (main - pending < amount) return;
        u.pendingWithdrawal = pending + amount;
        return u;
      });
      if (!reserved?.committed) return ws.send(JSON.stringify({ type: 'transaction-fail', id, message: 'insufficient-main-balance' }));
      base.reserved = true;
    }
    try {
      await ref.create(base);
    } catch (e) {
      if (type === 'withdraw' && base.reserved) {
        await db.ref('users/' + ws.uid).transaction(u => {
          if (!u) return u;
          u.pendingWithdrawal = Math.max(0, Number(u.pendingWithdrawal || 0) - amount);
          return u;
        }).catch(() => {});
      }
      return ws.send(JSON.stringify({ type: 'transaction-fail', id, message: 'transaction-create-failed' }));
    }
    ws.send(JSON.stringify({ type: 'transaction-created', id, txType: type, status: 'pending' }));
    return;
  }

  if (m.type === 'transaction-image') {
    const id = String(m.id || '');
    if (!id) return;
    const ref = db.ref('transactions/' + id);
    const snap = await ref.once('value');
    const tx = snap.val();
    if (!tx || tx.username !== ws.uid || tx.type !== 'deposit' || tx.status !== 'pending') return;
    const url = String(m.imageUrl || '').slice(0, 2000);
    if (url) await ref.update({ imageUrl: url });
    return;
  }

  if (m.type === 'admin-list-transactions') {
    if (!ws.isAdmin) return ws.send(JSON.stringify({ type: 'admin-error', message: 'not-admin' }));
    const snap = await db.ref('transactions').orderByChild('status').equalTo('pending').once('value');
    ws.send(JSON.stringify({ type: 'admin-transactions', transactions: snap.val() || {} }));
    return;
  }

  if (m.type === 'admin-decision') {
    if (!ws.isAdmin) return ws.send(JSON.stringify({ type: 'admin-error', message: 'not-admin' }));
    const id = String(m.id || '');
    const decision = String(m.decision || '');
    if (!id || !['approved', 'rejected'].includes(decision)) return;
    const txRef = db.ref('transactions/' + id);
    const claim = await txRef.transaction(tx => {
      if (!tx || tx.status !== 'pending') return;
      return { ...tx, status: 'processing', processingBy: 'admin', processingAt: Date.now() };
    });
    const tx = claim?.snapshot?.val();
    if (!claim?.committed || !tx || tx.status !== 'processing') return ws.send(JSON.stringify({ type: 'admin-error', message: 'transaction-not-pending' }));
    const userRef = db.ref('users/' + tx.username);
    const amount = Number(tx.amount || 0);
    if (tx.type === 'deposit') {
      if (decision === 'approved') {
        const applied = await userRef.transaction(u => {
          if (!u) return u;
          u.playWallet = Number(u.playWallet || 0);
          u.playWallet += amount;
          return u;
        });
        if (!applied?.committed) await txRef.update({ status: 'pending', processingBy: null, processingAt: null }).catch(() => {}); return ws.send(JSON.stringify({ type: 'admin-error', message: 'deposit-apply-failed' }));
      }
    } else if (tx.type === 'withdraw') {
      if (decision === 'approved') {
        const applied = await userRef.transaction(u => {
          if (!u) return u;
          const pending = Number(u.pendingWithdrawal || 0);
          if (pending < amount) return;
          u.mainWallet = Math.max(0, Number(u.mainWallet || 0) - amount);
          u.pendingWithdrawal = pending - amount;
          return u;
        });
        if (!applied?.committed) await txRef.update({ status: 'pending', processingBy: null, processingAt: null }).catch(() => {}); return ws.send(JSON.stringify({ type: 'admin-error', message: 'withdraw-apply-failed' }));
      } else {
        const released = await userRef.transaction(u => {
          if (!u) return u;
          u.pendingWithdrawal = Math.max(0, Number(u.pendingWithdrawal || 0) - amount);
          return u;
        });
        if (!released?.committed) await txRef.update({ status: 'pending', processingBy: null, processingAt: null }).catch(() => {}); return ws.send(JSON.stringify({ type: 'admin-error', message: 'withdraw-release-failed' }));
      }
    }
    await txRef.update({ status: decision, walletApplied: decision === 'approved', decidedAt: SV.TIMESTAMP, decidedBy: 'admin' });
    const set = conns.get(tx.username) || new Set();
    for (const c of set) if (c.readyState === 1) { try { c.send(JSON.stringify({ type: 'transaction-status', id, status: decision, txType: tx.type })); } catch {} }
    ws.send(JSON.stringify({ type: 'admin-decision-ok', id, status: decision }));
    return;
  }

  if (m.type === 'wallet-refresh') {
    const snap = await db.ref('users/' + ws.uid).once('value');
    const u = snap.val() || {};
    ws.send(JSON.stringify({ type: 'wallet', main: Number(u.mainWallet || 0), play: Number(u.playWallet || 0), pending: Number(u.pendingWithdrawal || 0) }));
    return;
  }
}

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 25000);

const app = express();
app.get('/health', (_, res) => res.json({
  ok: true,
  rooms: Object.fromEntries(Object.values(rooms).map(r =>
    [r.id, { phase: r.phase, round: r.round, players: r.players.size, taken: r.taken.size }])),
}));

const server = http.createServer(app);
server.on('upgrade', (req, socket, head) => {
  const origin = req.headers.origin || '';
  const ok = ALLOWED_ORIGIN === '*'
    || origin === ALLOWED_ORIGIN
    || (DEV_ALLOW_ANY && origin.endsWith('.github.dev'));
  if (!ok) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});
server.listen(PORT, '0.0.0.0', () => console.log('Listening on :' + PORT));

for (const room of Object.values(rooms)) {
  room.endsAt = Date.now() + SELECTION_SECONDS * 1000;
  room.push();
  scheduleBots(room);
}

console.log(`Bot pool size: ${BOT_NAMES.length}`);
console.log(`Bots per round: ${BOT_MIN}–${BOT_MAX}`);
console.log(`Allowed origin: ${ALLOWED_ORIGIN} (dev: ${DEV_ALLOW_ANY ? 'any *.github.dev' : 'no'})`);