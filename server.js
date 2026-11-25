/**
 * selfbot-vc v3 — multi-bot server
 * Features: multi-bot, server join via invite, play local audio to all bots
 *
 * Install deps:
 *   npm install discord.js-selfbot-v13 express ws @discordjs/opus prism-media
 *
 * Also install ffmpeg in Termux:
 *   pkg install ffmpeg
 */

const { Client, StreamType } = require('discord.js-selfbot-v13');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { pipeline } = require('stream');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname)));

// Serve audio files from /audio folder
const AUDIO_DIR = path.join(__dirname, 'audio');
if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR);

app.use('/audio', express.static(AUDIO_DIR));

// Upload endpoint - saves audio file to /audio/
app.post('/upload', express.raw({ type: '*/*', limit: '50mb' }), (req, res) => {
  const name = req.headers['x-filename'] || 'audio_' + Date.now() + '.mp3';
  const safeName = name.replace(/[^a-zA-Z0-9._-]/g, '_');
  const dest = path.join(AUDIO_DIR, safeName);
  fs.writeFileSync(dest, req.body);
  log(0, `Uploaded: ${safeName} (${(req.body.length / 1024).toFixed(1)} KB)`, 'ok');
  broadcastAudioList();
  res.json({ ok: true, name: safeName });
});

app.get('/audio-list', (req, res) => {
  const files = fs.readdirSync(AUDIO_DIR)
    .filter(f => /\.(mp3|wav|ogg|flac|m4a|aac)$/i.test(f))
    .map(f => {
      const stat = fs.statSync(path.join(AUDIO_DIR, f));
      return { name: f, size: stat.size };
    });
  res.json(files);
});

// Delete audio file
app.delete('/audio/:name', (req, res) => {
  const safeName = req.params.name.replace(/[^a-zA-Z0-9._-]/g, '_');
  const fp = path.join(AUDIO_DIR, safeName);
  if (fs.existsSync(fp)) { fs.unlinkSync(fp); broadcastAudioList(); }
  res.json({ ok: true });
});

const bots = new Map();
let nextId = 1;
let playState = { playing: false, file: null, loop: false };
let ffmpegProcs = new Map(); // botId -> ffmpeg process

function broadcast(type, data) {
  const msg = JSON.stringify({ type, ...data });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

function log(botId, text, level = 'info') {
  const bot = bots.get(botId);
  const tag = botId === 0 ? '[SERVER]' : `[${bot ? bot.label : 'Bot ' + botId}]`;
  console.log(`${tag} ${text}`);
  broadcast('log', { botId, text: `${tag} ${text}`, level });
}

function broadcastBotList() {
  broadcast('botList', {
    bots: [...bots.values()].map(b => ({
      id: b.id, label: b.label, username: b.username,
      connected: b.connected, inVC: b.inVC,
      channelId: b.channelId, muted: b.muted, deafened: b.deafened,
      playing: b.playing,
    }))
  });
}

function broadcastAudioList() {
  const files = fs.existsSync(AUDIO_DIR)
    ? fs.readdirSync(AUDIO_DIR).filter(f => /\.(mp3|wav|ogg|flac|m4a|aac)$/i.test(f))
        .map(f => ({ name: f, size: fs.statSync(path.join(AUDIO_DIR, f)).size }))
    : [];
  broadcast('audioList', { files });
}

function broadcastPlayState() {
  broadcast('playState', playState);
}

wss.on('connection', socket => {
  console.log('[SERVER] UI connected');
  socket.send(JSON.stringify({
    type: 'init',
    bots: [...bots.values()].map(b => ({
      id: b.id, label: b.label, username: b.username,
      connected: b.connected, inVC: b.inVC,
      channelId: b.channelId, muted: b.muted, deafened: b.deafened,
      playing: b.playing,
    })),
    audioFiles: fs.readdirSync(AUDIO_DIR)
      .filter(f => /\.(mp3|wav|ogg|flac|m4a|aac)$/i.test(f))
      .map(f => ({ name: f, size: fs.statSync(path.join(AUDIO_DIR, f)).size })),
    playState,
  }));

  socket.on('message', async raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const { action, botId, token, channelId, value, label, inviteCode, file, loop } = msg;
    switch (action) {
      case 'addBot':    await addBot(token, label); break;
      case 'removeBot': await removeBot(botId); break;
      case 'join':      await joinVC(botId, channelId); break;
      case 'leave':     await leaveVC(botId); break;
      case 'rejoin':    await rejoinVC(botId); break;
      case 'move':      await moveVC(botId, channelId); break;
      case 'mute':      await setMute(botId, value); break;
      case 'deafen':    await setDeafen(botId, value); break;
      case 'joinAll':   await joinAll(channelId); break;
      case 'leaveAll':  await leaveAll(); break;
      case 'muteAll':   await muteAll(value); break;
      case 'joinServer':await joinServer(botId, inviteCode); break;
      case 'joinServerAll': await joinServerAll(inviteCode); break;
      case 'playAll':   await playAll(file, loop); break;
      case 'stopAll':   await stopAll(); break;
      case 'deleteAudio': deleteAudio(file); break;
    }
  });
});

// ── Bot management ──────────────────────────────────────────────────────────

async function addBot(token, label) {
  if (!token) return;
  const id = nextId++;
  const bot = {
    id, label: label || `Bot ${id}`, client: null,
    username: null, connected: false, inVC: false,
    channelId: null, muted: false, deafened: false,
    playing: false, connection: null,
  };
  bots.set(id, bot);

  const client = new Client({ checkUpdate: false });
  bot.client = client;

  client.on('ready', () => {
    bot.username = client.user.tag;
    bot.connected = true;
    log(id, `Logged in as ${client.user.tag}`, 'ok');
    broadcastBotList();
  });

  client.on('voiceStateUpdate', (o, n) => {
    if (!client.user || o.member?.id !== client.user.id) return;
    bot.inVC = !!n.channelId;
    bot.channelId = n.channelId || null;
    if (!bot.inVC) { bot.playing = false; bot.connection = null; }
    broadcastBotList();
  });

  client.on('error', err => log(id, `Error: ${err.message}`, 'error'));

  try { await client.login(token); }
  catch (err) {
    log(id, `Login failed: ${err.message}`, 'error');
    bots.delete(id); broadcastBotList();
  }
}

async function removeBot(botId) {
  const bot = bots.get(botId); if (!bot) return;
  try {
    stopBotAudio(botId);
    if (bot.inVC) await leaveVC(botId);
    bot.client?.destroy();
  } catch {}
  bots.delete(botId); broadcastBotList();
}

// ── Voice ───────────────────────────────────────────────────────────────────

async function joinVC(botId, channelId) {
  const bot = bots.get(botId);
  if (!bot?.connected) { log(botId, 'Not connected', 'error'); return; }
  try {
    const ch = await bot.client.channels.fetch(channelId);
    if (!ch?.isVoice()) { log(botId, 'Not a voice channel', 'error'); return; }
    const conn = await ch.join({ selfMute: bot.muted, selfDeaf: bot.deafened });
    bot.connection = conn;
    bot.channelId = channelId;
    bot.inVC = true;
    log(botId, `Joined ${ch.name}`, 'ok');
    broadcastBotList();
  } catch (err) { log(botId, `Join failed: ${err.message}`, 'error'); }
}

async function leaveVC(botId) {
  const bot = bots.get(botId); if (!bot?.connected) return;
  try {
    stopBotAudio(botId);
    bot.client.guilds.cache.forEach(g => {
      const m = g.members.cache.get(bot.client.user.id);
      if (m?.voice?.channelId) m.voice.disconnect();
    });
    bot.channelId = null; bot.inVC = false; bot.connection = null;
    log(botId, 'Left VC', 'info'); broadcastBotList();
  } catch (err) { log(botId, `Leave failed: ${err.message}`, 'error'); }
}

async function rejoinVC(botId) {
  const bot = bots.get(botId);
  if (!bot?.inVC) { log(botId, 'Not in VC', 'error'); return; }
  const ch = bot.channelId;
  await leaveVC(botId);
  setTimeout(() => joinVC(botId, ch), 800);
}

async function moveVC(botId, channelId) {
  const bot = bots.get(botId); if (!bot?.connected) return;
  try {
    const ch = await bot.client.channels.fetch(channelId);
    if (!ch?.isVoice()) { log(botId, 'Not a voice channel', 'error'); return; }
    const conn = await ch.join({ selfMute: bot.muted, selfDeaf: bot.deafened });
    bot.connection = conn;
    bot.channelId = channelId; bot.inVC = true;
    log(botId, `Moved to ${ch.name}`, 'ok'); broadcastBotList();
  } catch (err) { log(botId, `Move failed: ${err.message}`, 'error'); }
}

async function setMute(botId, value) {
  const bot = bots.get(botId); if (!bot) return;
  bot.muted = value;
  try { bot.client?.guilds.cache.forEach(g => { const m = g.members.cache.get(bot.client.user.id); if (m?.voice?.channelId) m.voice.setSelfMute(value); }); } catch {}
  log(botId, value ? 'Muted' : 'Unmuted', value ? 'warn' : 'ok');
  broadcastBotList();
}

async function setDeafen(botId, value) {
  const bot = bots.get(botId); if (!bot) return;
  bot.deafened = value;
  try { bot.client?.guilds.cache.forEach(g => { const m = g.members.cache.get(bot.client.user.id); if (m?.voice?.channelId) m.voice.setSelfDeaf(value); }); } catch {}
  log(botId, value ? 'Deafened' : 'Undeafened', value ? 'warn' : 'ok');
  broadcastBotList();
}

// ── Bulk ────────────────────────────────────────────────────────────────────

async function joinAll(channelId) {
  if (!channelId) return;
  for (const [id] of bots) await joinVC(id, channelId);
}

async function leaveAll() {
  for (const [id] of bots) await leaveVC(id);
}

async function muteAll(value) {
  for (const [id] of bots) await setMute(id, value);
}

// ── Server join via invite ──────────────────────────────────────────────────

async function joinServer(botId, inviteCode) {
  const bot = bots.get(botId);
  if (!bot?.connected) { log(botId, 'Not connected', 'error'); return; }
  try {
    // Strip full URL down to code
    const code = inviteCode.replace(/^(https?:\/\/)?(discord\.gg|discord\.com\/invite)\//i, '').trim();
    const invite = await bot.client.fetchInvite(code);
    log(botId, `Joining server: ${invite.guild?.name || code}…`, 'info');
    await bot.client.acceptInvite(code);
    log(botId, `Joined server: ${invite.guild?.name || code}`, 'ok');
  } catch (err) {
    log(botId, `Server join failed: ${err.message}`, 'error');
  }
}

async function joinServerAll(inviteCode) {
  if (!inviteCode) return;
  for (const [id] of bots) await joinServer(id, inviteCode);
}

// ── Audio playback ──────────────────────────────────────────────────────────

function stopBotAudio(botId) {
  const proc = ffmpegProcs.get(botId);
  if (proc) { try { proc.kill('SIGKILL'); } catch {} ffmpegProcs.delete(botId); }
  const bot = bots.get(botId);
  if (bot) bot.playing = false;
}

async function playBotAudio(botId, filePath, loop) {
  const bot = bots.get(botId);
  if (!bot?.inVC) { log(botId, 'Not in VC — skipping audio', 'warn'); return; }

  stopBotAudio(botId);

  // Re-fetch connection if needed
  if (!bot.connection) {
    bot.client.guilds.cache.forEach(g => {
      const vc = g.voice?.connection;
      if (vc) bot.connection = vc;
    });
  }

  if (!bot.connection) { log(botId, 'No voice connection', 'error'); return; }

  try {
    // Use ffmpeg to convert to PCM s16le, then stream as Opus via dispatcher
    const dispatcher = bot.connection.play(filePath, {
      type: 'unknown',   // let discord.js-selfbot handle transcoding
      volume: 1,
    });

    bot.playing = true;
    broadcastBotList();

    dispatcher.on('finish', () => {
      bot.playing = false;
      ffmpegProcs.delete(botId);
      if (loop && playState.playing) {
        setTimeout(() => playBotAudio(botId, filePath, loop), 500);
      } else {
        broadcastBotList();
      }
    });

    dispatcher.on('error', err => {
      log(botId, `Playback error: ${err.message}`, 'error');
      bot.playing = false; broadcastBotList();
    });

  } catch (err) {
    log(botId, `Play failed: ${err.message}`, 'error');
    bot.playing = false;
  }
}

async function playAll(file, loop = false) {
  if (!file) { log(0, 'No file selected', 'error'); return; }
  const filePath = path.join(AUDIO_DIR, file);
  if (!fs.existsSync(filePath)) { log(0, `File not found: ${file}`, 'error'); return; }

  playState = { playing: true, file, loop };
  broadcastPlayState();
  log(0, `Playing ${file} on all bots${loop ? ' (loop)' : ''}`, 'info');

  const botsInVC = [...bots.values()].filter(b => b.inVC && b.connected);
  if (botsInVC.length === 0) { log(0, 'No bots are in a VC', 'warn'); return; }

  for (const bot of botsInVC) {
    await playBotAudio(bot.id, filePath, loop);
  }
}

async function stopAll() {
  playState = { playing: false, file: playState.file, loop: false };
  broadcastPlayState();
  log(0, 'Stopped playback on all bots', 'info');
  for (const [id] of bots) stopBotAudio(id);
  broadcastBotList();
}

function deleteAudio(file) {
  const safeName = (file || '').replace(/[^a-zA-Z0-9._-]/g, '_');
  const fp = path.join(AUDIO_DIR, safeName);
  if (fs.existsSync(fp)) { fs.unlinkSync(fp); log(0, `Deleted ${safeName}`, 'info'); }
  broadcastAudioList();
}

// ── Start ───────────────────────────────────────────────────────────────────

server.listen(3000, '0.0.0.0', () => {
  console.log('\n╔══════════════════════════════════════╗');
  console.log('║  selfbot-vc v3 — http://localhost:3000  ║');
  console.log('╚══════════════════════════════════════╝\n');
  console.log('Make sure ffmpeg is installed: pkg install ffmpeg\n');
});
