// index.js - Single-file Discord timeclock bot with Coin Card payments
// Requirements: node 18+, discord.js v14, sqlite3
// Put in the same project folder as your api.js (or ensure API at API_BASE).
require('dotenv').config();
const { Client, GatewayIntentBits, Partials, ActionRowBuilder, ButtonBuilder, ButtonStyle, EmbedBuilder, PermissionsBitField } = require('discord.js');
const { REST } = require('@discordjs/rest');
const { Routes } = require('discord-api-types/v10');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
if (!DISCORD_TOKEN) {
  console.error('Missing DISCORD_TOKEN in .env');
  process.exit(1);
}
const API_BASE = (process.env.API_BASE || 'http://coin.foxsrv.net:26450').replace(/\/+$/, ''); // remove trailing slash

const DB_PATH = path.join(__dirname, process.env.DATABASE_PATH && process.env.DATABASE_PATH.trim() !== '' ? process.env.DATABASE_PATH : 'database.db');
const db = new sqlite3.Database(DB_PATH);

// Boot DB
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS guilds (
    guildId TEXT PRIMARY KEY,
    coinCard TEXT,
    logChannelId TEXT,
    panelChannelId TEXT
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS staffRoles (
    guildId TEXT,
    roleId TEXT,
    PRIMARY KEY (guildId, roleId)
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS salaries (
    guildId TEXT,
    roleId TEXT,
    amount TEXT,
    PRIMARY KEY (guildId, roleId)
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS user_guild (
    userId TEXT,
    guildId TEXT,
    totalSeconds INTEGER DEFAULT 0,
    currentSeconds INTEGER DEFAULT 0,
    pendingCoins TEXT DEFAULT '0.0',
    totalReceived TEXT DEFAULT '0.0',
    PRIMARY KEY (userId, guildId)
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS user_global (
    userId TEXT PRIMARY KEY,
    totalSeconds INTEGER DEFAULT 0,
    totalReceived TEXT DEFAULT '0.0',
    totalPending TEXT DEFAULT '0.0'
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS opens (
    userId TEXT,
    guildId TEXT,
    openTs INTEGER,
    PRIMARY KEY (userId)
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    userId TEXT,
    guildId TEXT,
    startTs INTEGER,
    endTs INTEGER,
    durationSecs INTEGER,
    coinsPaid TEXT,
    txId TEXT
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS txlog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    userId TEXT,
    guildId TEXT,
    amount TEXT,
    txId TEXT,
    timestamp INTEGER
  )`);
});

// Utils
const toFloat8 = (num) => {
  const n = typeof num === 'string' ? parseFloat(num || '0') : (num || 0);
  if (!isFinite(n)) return '0.0';
  const truncated = Math.floor(n * 1e8) / 1e8;
  const s = truncated.toFixed(8);
  // remove trailing zeros but keep decimal if there was fractional part
  if (s.indexOf('.') >= 0) {
    return s.replace(/\.?0+$/, (truncated % 1 === 0 ? '' : ''));
  }
  return s;
};
const secsToDhms = (s) => {
  s = Math.max(0, Math.floor(s));
  const d = Math.floor(s / 86400); s %= 86400;
  const h = Math.floor(s / 3600); s %= 3600;
  const m = Math.floor(s / 60); s %= 60;
  return `${d}d ${h}h ${m}m ${s}s`;
};
const parseDDDHHMMSS = (str) => {
  const parts = (str || '').split(':').map(p => parseInt(p, 10));
  if (parts.length !== 4 || parts.some(x => isNaN(x) || x < 0)) return null;
  const [d,h,m,s] = parts;
  return ((d*86400) + (h*3600) + (m*60) + s);
};

// New parser: accepts ddd:hh:mm:ss OR shorthand like "1d", "2h", "30m", "15s", "1d2h30m", with or without spaces
function parseTimeInput(input) {
  if (!input || typeof input !== 'string') return null;
  input = input.trim();
  // first, allow the original format ddd:hh:mm:ss
  const dddhhmmss = parseDDDHHMMSS(input);
  if (dddhhmmss !== null) return dddhhmmss;

  // shorthand parser
  // accept sequences like "1d", "2h", "30m", "15s", optionally separated by spaces
  // regex global to capture all occurrences
  const regex = /(\d+)\s*(d|h|m|s)\b/gi;
  let match;
  let total = 0;
  let found = false;
  while ((match = regex.exec(input)) !== null) {
    found = true;
    const val = parseInt(match[1], 10);
    const unit = match[2].toLowerCase();
    if (isNaN(val) || val < 0) continue;
    if (unit === 'd') total += val * 86400;
    else if (unit === 'h') total += val * 3600;
    else if (unit === 'm') total += val * 60;
    else if (unit === 's') total += val;
  }
  if (found && total >= 0) return total;

  // fallback: if input is pure number, treat as seconds (safe fallback)
  if (/^\d+$/.test(input)) {
    return parseInt(input, 10);
  }

  return null;
}

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers, GatewayIntentBits.GuildMessages],
  partials: [Partials.Channel]
});

// Crash-resistant handlers
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
});
process.on('unhandledRejection', (err) => {
  console.error('Unhandled Rejection:', err);
});

// Commands - note: option types use numeric values accepted by Discord API
const commands = [
  { name: 'addtime', description: 'Add time to a user (ddd:hh:mm:ss or 1d/2h/30m/15s)', options: [
      { name: 'user', type: 6, description: 'User', required: true },
      { name: 'time', type: 3, description: 'ddd:hh:mm:ss or 1d2h30m', required: true }
    ]},
  { name: 'removetime', description: 'Remove time from a user (ddd:hh:mm:ss or 1d/2h/30m/15s)', options: [
      { name: 'user', type: 6, description: 'User', required: true },
      { name: 'time', type: 3, description: 'ddd:hh:mm:ss or 1d2h30m', required: true }
    ]},
  { name: 'reset', description: 'Reset current unpaid time for a user', options: [
      { name: 'user', type: 6, description: 'User', required: true }
    ]},
  { name: 'settime', description: 'Set current unpaid time for a user (ddd:hh:mm:ss or 1d2h)', options: [
      { name: 'user', type: 6, description: 'User', required: true },
      { name: 'time', type: 3, description: 'ddd:hh:mm:ss or 1d2h', required: true }
    ]},
  { name: 'time', description: 'Show the accumulated time for a user', options: [
      { name: 'user', type: 6, description: 'User', required: true }
    ]},
  { name: 'addstaff', description: 'Add a staff role', options: [
      { name: 'role', type: 8, description: 'Role', required: true }
    ]},
  { name: 'removestaff', description: 'Remove a staff role', options: [
      { name: 'role', type: 8, description: 'Role', required: true }
    ]},
  { name: 'channel', description: 'Post the timeclock panel to a channel', options: [
      { name: 'channel', type: 7, description: 'Channel', required: true }
    ]},
  { name: 'log', description: 'Set log channel', options: [
      { name: 'channel', type: 7, description: 'Channel', required: true }
    ]},
  { name: 'salary', description: 'Set salary per hour for a role (coins)', options: [
      { name: 'role', type: 8, description: 'Role', required: true },
      { name: 'amount', type: 3, description: 'Coins per hour (ex: 0.01234567)', required: true }
    ]},
  { name: 'coincard', description: 'Set Coin Card (card code) for this guild', options: [
      { name: 'card', type: 3, description: 'Card code (will be stored as-is)', required: true }
    ]},
  { name: 'top', description: 'Show top 10 users by current unpaid time' },
  { name: 'global', description: 'Show global stats of a user', options: [
      { name: 'user', type: 6, description: 'User', required: true }
    ]},
  { name: 'clear', description: 'Clear current unpaid time and pending coins for a user in this guild', options: [
      { name: 'user', type: 6, description: 'User', required: true }
    ]},
  { name: 'salaries', description: 'List all roles in this guild that have a configured salary (> 0)' }
];

// Register commands
client.once('ready', async () => {
  console.log(`Logged in as ${client.user.tag}`);

  try {
    const rest = new REST({ version: '10' }).setToken(DISCORD_TOKEN);
    await rest.put(Routes.applicationCommands(client.user.id), { body: commands });
    console.log('Global commands pushed (may take time).');

    const guilds = client.guilds.cache.map(g => g.id);
    for (const gid of guilds) {
      try {
        await rest.put(Routes.applicationGuildCommands(client.user.id, gid), { body: commands });
      } catch (e) {
        console.warn('Could not register commands for guild', gid, e?.message?.slice(0,200));
      }
    }
  } catch (e) {
    console.error('Command registration failed:', e);
  }

  startPeriodicChecker();
});

// UI builders
function buildPanelEmbed() {
  return new EmbedBuilder()
    .setTitle('⏱️ Timeclock Panel')
    .setDescription('Click **Open** to start a work session, **Close** to stop it, **View** to see your current time, or **Claim** to receive your pending coins.')
    .setColor(0x2f3136)
    .setTimestamp();
}
function buildPanelRow() {
  const open = new ButtonBuilder().setCustomId('open_point').setLabel('Open').setStyle(ButtonStyle.Primary).setEmoji('🟦');
  const close = new ButtonBuilder().setCustomId('close_point').setLabel('Close').setStyle(ButtonStyle.Danger).setEmoji('🔴');
  const view = new ButtonBuilder().setCustomId('view_point').setLabel('View').setStyle(ButtonStyle.Secondary).setEmoji('📄');
  const claim = new ButtonBuilder().setCustomId('claim_point').setLabel('Claim').setStyle(ButtonStyle.Success).setEmoji('💸');
  return new ActionRowBuilder().addComponents(open, close, view, claim);
}

// Safe helper to fetch channel and check sendability
async function fetchSendableChannel(channelResolvable) {
  try {
    // channelResolvable may be an object with id or a Channel object - normalize
    const id = (channelResolvable && channelResolvable.id) ? channelResolvable.id : channelResolvable;
    if (!id) return null;
    const ch = await client.channels.fetch(id).catch(()=>null);
    if (!ch) return null;
    // check for send function (robust for multiple versions)
    if (typeof ch.send === 'function') return ch;
    return null;
  } catch (e) {
    return null;
  }
}

// Compute salary per hour by summing all matching role salaries for the user in the guild.
// Returns Promise<number>
async function computeSalaryForUser(guildId, userId) {
  try {
    const guild = client.guilds.cache.get(guildId) || await client.guilds.fetch(guildId).catch(()=>null);
    if (!guild) return 0;
    const member = await guild.members.fetch(userId).catch(()=>null);
    if (!member) return 0;
    const roleIds = Array.from(member.roles.cache.keys()).filter(id => id && id !== guildId);
    if (!roleIds || roleIds.length === 0) return 0;
    const placeholders = roleIds.map(()=>'?').join(',');
    return new Promise((resolve) => {
      db.all(`SELECT amount FROM salaries WHERE guildId = ? AND roleId IN (${placeholders})`, [guildId, ...roleIds], (err, rows) => {
        if (err || !rows || rows.length === 0) return resolve(0);
        const vals = rows.map(r => parseFloat(r.amount || '0')).filter(x => !isNaN(x));
        const sum = vals.reduce((a,b) => a + b, 0);
        resolve(Math.max(0, sum));
      });
    });
  } catch (e) {
    console.error('computeSalaryForUser error', e);
    return 0;
  }
}

// Adjust global pending for a user by deltaCoins (string or number). deltaCoins can be negative.
function adjustGlobalPending(userId, deltaCoins) {
  return new Promise((resolve) => {
    const delta = parseFloat(deltaCoins || 0) || 0;
    db.get('SELECT totalPending, totalSeconds, totalReceived FROM user_global WHERE userId = ?', [userId], (err, g) => {
      if (err) { console.error('adjustGlobalPending select error', err); return resolve(); }
      const current = g ? parseFloat(g.totalPending||'0') : 0;
      const newVal = Math.max(0, current + delta);
      const totalSeconds = g ? (g.totalSeconds||0) : 0;
      const totalReceived = g ? (g.totalReceived||'0.0') : '0.0';
      db.run('INSERT OR REPLACE INTO user_global(userId,totalSeconds,totalReceived,totalPending) VALUES (?, ?, ?, ?)', [userId, totalSeconds, totalReceived, toFloat8(newVal)], (err2) => {
        if (err2) console.error('adjustGlobalPending update error', err2);
        resolve();
      });
    });
  });
}

// interaction handling
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isButton && interaction.isButton()) return handleButton(interaction);
    if (!interaction.isChatInputCommand || !interaction.isChatInputCommand()) return;

    const { commandName, guild, member } = interaction;
    if (!guild) return interaction.reply({ content: 'Commands must be used inside a guild.', ephemeral: true });

    const isAdmin = member?.permissions?.has ? member.permissions.has(PermissionsBitField.Flags.Administrator) : false;

    const checkStaff = async () => {
      if (isAdmin) return true;
      if (!member) return false;
      const guildId = guild.id;
      const roleIds = Array.from(member.roles.cache.keys());
      if (!roleIds || roleIds.length === 0) return false;
      const placeholders = roleIds.map(()=>'?').join(',');
      return new Promise((resolve, reject) => {
        db.get(`SELECT 1 FROM staffRoles WHERE guildId = ? AND roleId IN (${placeholders}) LIMIT 1`, [guildId, ...roleIds], (err,row) => {
          if (err) return reject(err);
          resolve(!!row);
        });
      });
    };

    if (commandName === 'channel') {
      if (!isAdmin) return interaction.reply({ content: 'You need Administrator to set panel channel.', ephemeral: true });
      const target = interaction.options.getChannel('channel');
      if (!target) return interaction.reply({ content: 'Invalid channel.', ephemeral: true });

      // store panel channel id as string
      db.run('INSERT OR REPLACE INTO guilds(guildId, coinCard, logChannelId, panelChannelId) VALUES (?, COALESCE((SELECT coinCard FROM guilds WHERE guildId=?), NULL), COALESCE((SELECT logChannelId FROM guilds WHERE guildId=?), NULL), ?)', [guild.id, guild.id, guild.id, target.id], (err) => {
        if (err) console.error('DB error setting panel:', err);
      });

      const embed = buildPanelEmbed();
      const row = buildPanelRow();

      // fetch a sendable channel object to send properly
      const sendCh = await fetchSendableChannel(target).catch(()=>null);
      if (!sendCh) {
        await interaction.reply({ content: `Panel saved but I couldn't send to that channel (missing permission or invalid channel). Panel ID saved: ${target.id}`, ephemeral: true });
        return;
      }

      try {
        await sendCh.send({ embeds: [embed], components: [row] });
      } catch (e) {
        console.warn('Could not send panel to channel (missing perms?)', e && e.message || e);
        await interaction.reply({ content: `Panel saved but sending failed (missing perms?). Panel saved to ${target.id}`, ephemeral: true });
        return;
      }
      await interaction.reply({ content: `Panel posted to <#${target.id}>`, ephemeral: true });
      return;
    }

    if (commandName === 'log') {
      if (!isAdmin) return interaction.reply({ content: 'You need Administrator to set log channel.', ephemeral: true });
      const ch = interaction.options.getChannel('channel');
      if (!ch) return interaction.reply({ content: 'Invalid channel.', ephemeral: true });
      db.run('INSERT OR REPLACE INTO guilds(guildId, coinCard, logChannelId, panelChannelId) VALUES (?, COALESCE((SELECT coinCard FROM guilds WHERE guildId=?), NULL), ?, COALESCE((SELECT panelChannelId FROM guilds WHERE guildId=?), NULL))', [guild.id, guild.id, ch.id, guild.id], (err) => {
        if (err) console.error('DB error setting log channel', err);
      });
      return interaction.reply({ content: `Log channel set to <#${ch.id}>`, ephemeral: true });
    }

    if (commandName === 'coincard') {
      if (!isAdmin) return interaction.reply({ content: 'You need Administrator to set the Coin Card for this guild.', ephemeral: true });
      const card = interaction.options.getString('card', true);
      db.run('INSERT OR REPLACE INTO guilds(guildId, coinCard, logChannelId, panelChannelId) VALUES (?, ?, COALESCE((SELECT logChannelId FROM guilds WHERE guildId=?), NULL), COALESCE((SELECT panelChannelId FROM guilds WHERE guildId=?), NULL))', [guild.id, card, guild.id, guild.id], (err) => {
        if (err) console.error('DB error saving card', err);
      });
      return interaction.reply({ content: `Coin Card saved ✅ (hidden)`, ephemeral: true });
    }

    if (commandName === 'addstaff' || commandName === 'removestaff') {
      if (!isAdmin) return interaction.reply({ content: 'You need Administrator to manage staff roles.', ephemeral: true });
      const role = interaction.options.getRole('role');
      if (!role) return interaction.reply({ content: 'Invalid role.', ephemeral: true });
      if (commandName === 'addstaff') {
        db.run('INSERT OR IGNORE INTO staffRoles(guildId, roleId) VALUES (?, ?)', [guild.id, role.id], (err) => { if (err) console.error(err); });
        return interaction.reply({ content: `Role **${role.name}** added as staff ✅`, ephemeral: true });
      } else {
        db.run('DELETE FROM staffRoles WHERE guildId = ? AND roleId = ?', [guild.id, role.id], (err) => { if (err) console.error(err); });
        return interaction.reply({ content: `Role **${role.name}** removed from staff ✅`, ephemeral: true });
      }
    }

    if (commandName === 'salary') {
      if (!isAdmin) return interaction.reply({ content: 'Administrator required.', ephemeral: true });
      const role = interaction.options.getRole('role');
      const amount = interaction.options.getString('amount', true);
      const parsed = parseFloat(amount);
      if (isNaN(parsed) || parsed < 0) return interaction.reply({ content: 'Invalid amount.', ephemeral: true });
      const truncated = toFloat8(parsed);
      db.run('INSERT OR REPLACE INTO salaries(guildId, roleId, amount) VALUES (?, ?, ?)', [guild.id, role.id, truncated], (err) => { if (err) console.error(err); });
      return interaction.reply({ content: `Salary ${truncated} coins/hour set for role **${role.name}**`, ephemeral: true });
    }

    if (['addtime','removetime','settime','reset'].includes(commandName)) {
      const isStaff = await checkStaff();
      if (!isStaff) return interaction.reply({ content: 'You need admin or configured staff role to use this.', ephemeral: true });
      const user = interaction.options.getUser('user', true);
      const guildId = guild.id;

      if (commandName === 'reset') {
        db.run('INSERT OR REPLACE INTO user_guild(userId, guildId, totalSeconds, currentSeconds, pendingCoins, totalReceived) VALUES (?, ?, COALESCE((SELECT totalSeconds FROM user_guild WHERE userId=? AND guildId=?),0), 0, COALESCE((SELECT pendingCoins FROM user_guild WHERE userId=? AND guildId=?), "0.0"), COALESCE((SELECT totalReceived FROM user_guild WHERE userId=? AND guildId=?), "0.0"))', [user.id, guildId, user.id, guildId, user.id, guildId, user.id, guildId], (err) => { if (err) console.error(err); });
        db.run('UPDATE user_guild SET currentSeconds = 0 WHERE userId = ? AND guildId = ?', [user.id, guildId], (err) => { if (err) console.error(err); });
        return interaction.reply({ content: `Reset current unpaid time for ${user.tag}`, ephemeral: false });
      }

      // parse time for add/removetime/settime using new flexible parser
      const timeStr = interaction.options.getString('time', true);
      const secs = parseTimeInput(timeStr);
      if (secs === null) return interaction.reply({ content: 'Invalid time format. Use ddd:hh:mm:ss or shorthand like 1d2h30m', ephemeral: true });

      // compute salary for target user in this guild (sum of roles)
      const salaryPerHour = await computeSalaryForUser(guildId, user.id);

      if (commandName === 'addtime') {
        // add seconds and proportional pending coins
        db.run('INSERT OR IGNORE INTO user_guild(userId,guildId,totalSeconds,currentSeconds,pendingCoins,totalReceived) VALUES (?, ?, 0, 0, "0.0", "0.0")', [user.id, guildId], (err) => { if (err) console.error(err); });
        const coins = toFloat8(salaryPerHour * (secs / 3600));
        db.get('SELECT currentSeconds,pendingCoins,totalSeconds FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, guildId], (err, row) => {
          if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
          const prevCur = row ? (row.currentSeconds||0) : 0;
          const prevTot = row ? (row.totalSeconds||0) : 0;
          const prevPending = row ? parseFloat(row.pendingCoins||'0') : 0;
          const newCur = prevCur + secs;
          const newTot = prevTot + secs;
          const newPending = toFloat8((prevPending || 0) + parseFloat(coins || '0'));
          db.run('UPDATE user_guild SET currentSeconds = ?, totalSeconds = ?, pendingCoins = ? WHERE userId = ? AND guildId = ?', [newCur, newTot, newPending, user.id, guildId], (err2) => {
            if (err2) console.error('Error updating user_guild on addtime', err2);
            // update global pending
            adjustGlobalPending(user.id, parseFloat(coins || 0)).then(() => {
              interaction.reply({ content: `Added ${secsToDhms(secs)} to ${user.tag} — +${coins} coins pending (based on roles)`, ephemeral: false });
            });
          });
        });
        return;
      }

      if (commandName === 'removetime') {
        // remove seconds and proportional pending coins
        db.get('SELECT currentSeconds,pendingCoins,totalSeconds FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, guildId], (err, row) => {
          if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
          const prevCur = row ? (row.currentSeconds||0) : 0;
          const prevTot = row ? (row.totalSeconds||0) : 0;
          const prevPending = row ? parseFloat(row.pendingCoins||'0') : 0;
          const removeSec = Math.min(prevCur, secs);
          // compute coins to remove proportional to time removed using current salaryPerHour
          const coinsToRemove = toFloat8(salaryPerHour * (removeSec / 3600));
          const newCur = Math.max(0, prevCur - removeSec);
          const newTot = Math.max(0, prevTot - removeSec); // note: if you don't want to touch totalSeconds, change accordingly
          const newPendingFloat = Math.max(0, (prevPending || 0) - parseFloat(coinsToRemove || '0'));
          const newPending = toFloat8(newPendingFloat);
          db.run('INSERT OR REPLACE INTO user_guild(userId,guildId,totalSeconds,currentSeconds,pendingCoins,totalReceived) VALUES (?, ?, ?, ?, ?, COALESCE((SELECT totalReceived FROM user_guild WHERE userId=? AND guildId=?),"0.0"))', [user.id, guildId, newTot, newCur, newPending, user.id, guildId], (err2) => {
            if (err2) console.error('Error updating user_guild on removetime', err2);
            // update global pending (subtract coinsToRemove)
            adjustGlobalPending(user.id, -parseFloat(coinsToRemove || 0)).then(() => {
              interaction.reply({ content: `Removed ${secsToDhms(removeSec)} from ${user.tag} — -${coinsToRemove} coins pending (based on roles)`, ephemeral: false });
            });
          });
        });
        return;
      }

      if (commandName === 'settime') {
        // set current unpaid time to secs, adjust pending coins proportionally (delta)
        db.run('INSERT OR IGNORE INTO user_guild(userId,guildId,totalSeconds,currentSeconds,pendingCoins,totalReceived) VALUES (?, ?, 0, 0, "0.0", "0.0")', [user.id, guildId], (err) => { if (err) console.error(err); });
        db.get('SELECT currentSeconds,pendingCoins,totalSeconds FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, guildId], (err,row) => {
          if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
          const prevCur = row ? (row.currentSeconds||0) : 0;
          const prevTot = row ? (row.totalSeconds||0) : 0;
          const prevPending = row ? parseFloat(row.pendingCoins||'0') : 0;
          const deltaSec = secs - prevCur;
          const deltaCoins = toFloat8(salaryPerHour * (deltaSec / 3600));
          const newCur = Math.max(0, secs);
          const newTot = Math.max(0, prevTot + Math.max(0, deltaSec)); // only increase totalSeconds if delta > 0
          const newPendingFloat = Math.max(0, (prevPending || 0) + parseFloat(deltaCoins || '0'));
          const newPending = toFloat8(newPendingFloat);
          db.run('UPDATE user_guild SET currentSeconds = ?, totalSeconds = ?, pendingCoins = ? WHERE userId = ? AND guildId = ?', [newCur, newTot, newPending, user.id, guildId], (err2) => {
            if (err2) console.error('Error updating user_guild on settime', err2);
            // adjust global pending by deltaCoins
            adjustGlobalPending(user.id, parseFloat(deltaCoins || 0)).then(() => {
              interaction.reply({ content: `Set current unpaid time of ${user.tag} to ${secsToDhms(secs)} — pending coins adjusted by ${deltaCoins}`, ephemeral: false });
            });
          });
        });
        return;
      }
    }

    if (commandName === 'clear') {
      // new command: clear pending time and pending coins for a user in this guild
      const isStaff = await checkStaff();
      if (!isStaff) return interaction.reply({ content: 'You need admin or configured staff role to use this.', ephemeral: true });
      const target = interaction.options.getUser('user', true);
      const guildId = guild.id;
      db.get('SELECT currentSeconds, pendingCoins FROM user_guild WHERE userId = ? AND guildId = ?', [target.id, guildId], (err,row) => {
        if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
        const prevPending = row ? parseFloat(row.pendingCoins||'0') : 0;
        // set currentSeconds=0 and pendingCoins=0.0
        db.run('INSERT OR REPLACE INTO user_guild(userId,guildId,totalSeconds,currentSeconds,pendingCoins,totalReceived) VALUES (?, ?, COALESCE((SELECT totalSeconds FROM user_guild WHERE userId=? AND guildId=?),0), 0, "0.0", COALESCE((SELECT totalReceived FROM user_guild WHERE userId=? AND guildId=?), "0.0"))', [target.id, guildId, target.id, guildId, target.id, guildId], (err2) => {
          if (err2) console.error('Error clearing user_guild', err2);
          // update user_global totalPending (subtract prevPending)
          if (prevPending && prevPending > 0) {
            adjustGlobalPending(target.id, -prevPending).then(() => {
              interaction.reply({ content: `Cleared pending time and pending coins (${toFloat8(prevPending)} coins) for ${target.tag}`, ephemeral: true });
              // safe log
              db.get('SELECT logChannelId FROM guilds WHERE guildId = ?', [guildId], (err5, gcfg) => {
                if (!err5 && gcfg && gcfg.logChannelId) {
                  fetchSendableChannel(gcfg.logChannelId).then(ch => { if (ch) ch.send({ embeds: [new EmbedBuilder().setDescription(`🧹 Cleared pending time and ${toFloat8(prevPending)} coins pending for <@${target.id}> (by ${interaction.user.tag}).`).setTimestamp()] }).catch(()=>{}); });
                }
              });
            });
          } else {
            interaction.reply({ content: `Cleared pending time (no pending coins) for ${target.tag}`, ephemeral: true });
            // safe log
            db.get('SELECT logChannelId FROM guilds WHERE guildId = ?', [guildId], (err5, gcfg) => {
              if (!err5 && gcfg && gcfg.logChannelId) {
                fetchSendableChannel(gcfg.logChannelId).then(ch => { if (ch) ch.send({ embeds: [new EmbedBuilder().setDescription(`🧹 Cleared pending time for <@${target.id}> (by ${interaction.user.tag}).`).setTimestamp()] }).catch(()=>{}); });
              }
            });
          }
        });
      });
      return;
    }

    if (commandName === 'salaries') {
      // list roles in this guild with salary > 0
      const guildId = guild.id;
      db.all('SELECT roleId, amount FROM salaries WHERE guildId = ? AND CAST(amount AS REAL) > 0 ORDER BY CAST(amount AS REAL) DESC', [guildId], async (err, rows) => {
        if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
        if (!rows || rows.length === 0) return interaction.reply({ content: 'No roles with configured salary (> 0) in this guild.', ephemeral: true });
        // build list with role mention if exists
        const lines = [];
        for (const r of rows) {
          const roleId = r.roleId;
          let roleMention = `Role ID: ${roleId}`;
          try {
            const role = await guild.roles.fetch(roleId).catch(()=>null);
            if (role) roleMention = `<@&${roleId}> (${role.name})`;
          } catch (_) {}
          lines.push(`${roleMention} — ${toFloat8(parseFloat(r.amount||'0'))} coins/hour`);
        }
        const emb = new EmbedBuilder().setTitle('💼 Salaries — configured roles').setDescription(lines.join('\n')).setColor(0x00AAFF).setTimestamp();
        return interaction.reply({ embeds: [emb], ephemeral: false });
      });
      return;
    }

    if (commandName === 'time') {
      const user = interaction.options.getUser('user', true);
      const guildId = guild.id;
      db.get('SELECT currentSeconds, totalSeconds FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, guildId], (err,row) => {
        if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
        const cur = row ? row.currentSeconds || 0 : 0;
        const tot = row ? row.totalSeconds || 0 : 0;
        const emb = new EmbedBuilder()
          .setTitle(`⏱️ Time for ${user.tag}`)
          .addFields(
            { name: 'Current unpaid time', value: String(secsToDhms(cur)), inline: true },
            { name: 'Total recorded (ever)', value: String(secsToDhms(tot)), inline: true }
          ).setColor(0x00AE86);
        return interaction.reply({ embeds: [emb], ephemeral: false });
      });
      return;
    }

    if (commandName === 'top') {
      db.all('SELECT userId, currentSeconds FROM user_guild WHERE currentSeconds > 0 ORDER BY currentSeconds DESC LIMIT 10', [], async (err, rows) => {
        if (err) { console.error(err); return interaction.reply({ content: 'Error fetching top', ephemeral: true }); }
        const lines = [];
        for (const r of rows) {
          const u = await client.users.fetch(r.userId).catch(()=>({tag:`Unknown (${r.userId})`}));
          lines.push(`${u.tag || u.username || r.userId} — ${secsToDhms(r.currentSeconds)}`);
        }
        const emb = new EmbedBuilder().setTitle('🏆 Top 10 — Current Unpaid Time').setDescription(lines.join('\n') || 'No data').setColor(0xFFD700);
        return interaction.reply({ embeds: [emb], ephemeral: false });
      });
      return;
    }

    if (commandName === 'global') {
      const user = interaction.options.getUser('user', true);
      db.get('SELECT totalSeconds,totalReceived FROM user_global WHERE userId = ?', [user.id], (err, g) => {
        if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
        const totSec = g ? (g.totalSeconds||0) : 0;
        const totRec = g ? (g.totalReceived||'0.0') : '0.0';
        db.all('SELECT guildId,currentSeconds,pendingCoins,totalSeconds,totalReceived FROM user_guild WHERE userId = ?', [user.id], async (err2, rows) => {
          if (err2) { console.error(err2); return interaction.reply({ content: 'DB error', ephemeral: true }); }
          const guildLines = rows.map(r => `Guild ${r.guildId}\n• Current: ${secsToDhms(r.currentSeconds||0)}\n• Pending coins: ${r.pendingCoins||'0.0'}\n• Total received (guild): ${r.totalReceived||'0.0'}\n`).join('\n') || 'No guild data';
          const emb = new EmbedBuilder()
            .setTitle(`🌐 Global stats for ${user.tag}`)
            .addFields(
              { name: 'Total time (all guilds, ever)', value: String(secsToDhms(totSec)), inline: true },
              { name: 'Total coins received (all guilds)', value: String(totRec || '0.0'), inline: true },
              { name: 'Per-guild breakdown', value: String(guildLines) }
            ).setColor(0x7289DA);
          return interaction.reply({ embeds: [emb], ephemeral: false });
        });
      });
      return;
    }

  } catch (err) {
    console.error('interactionCreate error', err);
    try {
      if (interaction && !interaction.replied) await interaction.reply({ content: 'Internal error', ephemeral: true });
      else if (interaction) await interaction.followUp({ content: 'Internal error', ephemeral: true });
    } catch (_) {}
  }
});

// Button handling (kept behavior, but safer channel operations)
async function handleButton(interaction) {
  const { customId, user, guildId } = interaction;
  if (!guildId) return interaction.reply({ content: 'Use in a guild.', ephemeral: true });

  // load guild config
  const guildConfig = await new Promise((res) => {
    db.get('SELECT coinCard, logChannelId FROM guilds WHERE guildId = ?', [guildId], (err,row) => res(row || {}));
  });

  const safeLog = async (textOrEmbed) => {
    try {
      if (!guildConfig || !guildConfig.logChannelId) return;
      const ch = await fetchSendableChannel(guildConfig.logChannelId);
      if (!ch) return;
      if (typeof textOrEmbed === 'string') await ch.send({ content: textOrEmbed }).catch(()=>{});
      else await ch.send({ embeds: [textOrEmbed] }).catch(()=>{});
    } catch (e) {
      console.warn('Could not log message', e && e.message || e);
    }
  };

  if (customId === 'open_point') {
    db.get('SELECT guildId, openTs FROM opens WHERE userId = ?', [user.id], (err, row) => {
      if (err) { console.error(err); return interaction.reply({ content: 'Internal error', ephemeral: true }); }
      if (row) {
        if (row.guildId === guildId) return interaction.reply({ content: 'You already have an open point in this server.', ephemeral: true });
        return interaction.reply({ content: `You already have an open point in another server (<#${row.guildId}>). Close it there first.`, ephemeral: true });
      }
      const openTs = Date.now();
      db.run('INSERT INTO opens(userId, guildId, openTs) VALUES (?, ?, ?)', [user.id, guildId, openTs], (err2) => {
        if (err2) { console.error(err2); return interaction.reply({ content: 'Internal error saving open.', ephemeral: true }); }
        safeLog(new EmbedBuilder().setDescription(`🟢 ${user.tag} opened a timepoint`).setTimestamp());
        return interaction.reply({ content: `🟢 Opened timepoint — start: ${new Date(openTs).toLocaleString()}`, ephemeral: true });
      });
    });
    return;
  }

  if (customId === 'close_point') {
    db.get('SELECT guildId, openTs FROM opens WHERE userId = ?', [user.id], (err,row) => {
      if (err) { console.error(err); return interaction.reply({ content: 'Internal error', ephemeral: true }); }
      if (!row) return interaction.reply({ content: 'You have no open point.', ephemeral: true });
      const startGuild = row.guildId;
      const startTs = row.openTs;
      const endTs = Date.now();
      const duration = Math.max(0, Math.floor((endTs - startTs) / 1000)); // seconds

      (async () => {
        try {
          // compute salary (sum of role salaries)
          const salaryPerHour = await computeSalaryForUser(startGuild, user.id);
          finalizeClose(salaryPerHour);
        } catch (e) {
          console.error('Error computing salary', e);
          finalizeClose(0);
        }
      })();

      function finalizeClose(salaryPerHour) {
        const coins = toFloat8((salaryPerHour * (duration / 3600)));
        db.serialize(() => {
          db.run('DELETE FROM opens WHERE userId = ?', [user.id], (err) => { if (err) console.error(err); });
          db.run('INSERT OR IGNORE INTO user_guild(userId,guildId,totalSeconds,currentSeconds,pendingCoins,totalReceived) VALUES (?, ?, 0, 0, "0.0", "0.0")', [user.id, startGuild], (err) => { if (err) console.error(err); });
          db.get('SELECT pendingCoins, currentSeconds, totalSeconds FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, startGuild], (err,row) => {
            if (err) { console.error(err); return; }
            const prevPending = row ? parseFloat(row.pendingCoins||'0') : 0;
            const prevCurSec = row ? (row.currentSeconds||0) : 0;
            const prevTot = row ? (row.totalSeconds||0) : 0;
            const newPending = toFloat8((prevPending || 0) + parseFloat(coins || '0'));
            const newCur = prevCurSec + duration;
            const newTot = prevTot + duration;
            db.run('UPDATE user_guild SET pendingCoins = ?, currentSeconds = ?, totalSeconds = ? WHERE userId = ? AND guildId = ?', [newPending, newCur, newTot, user.id, startGuild], (err2) => {
              if (err2) console.error('Error updating user_guild on close', err2);
              db.run('INSERT OR IGNORE INTO user_global(userId,totalSeconds,totalReceived,totalPending) VALUES (?,0,"0.0","0.0")', [user.id], (err3) => { if (err3) console.error(err3); });
              db.get('SELECT totalSeconds, totalPending FROM user_global WHERE userId = ?', [user.id], (err,grow) => {
                if (err) console.error(err);
                const gtot = (grow ? (grow.totalSeconds||0) : 0) + duration;
                const gpend = (grow ? parseFloat(grow.totalPending||'0') : 0) + parseFloat(coins||'0');
                db.run('UPDATE user_global SET totalSeconds = ?, totalPending = ? WHERE userId = ?', [gtot, toFloat8(gpend), user.id], (err4) => { if (err4) console.error(err4); });
                db.run('INSERT INTO history(userId,guildId,startTs,endTs,durationSecs,coinsPaid,txId) VALUES (?, ?, ?, ?, ?, ?, ?)', [user.id, startGuild, startTs, endTs, duration, coins, null], (err5) => { if (err5) console.error(err5); });
                const replyEmbed = new EmbedBuilder()
                  .setTitle('🔴 Timepoint closed')
                  .addFields(
                    { name: 'Duration', value: String(secsToDhms(duration)), inline: true },
                    { name: 'Added pending coins', value: String(`${coins} coins`), inline: true }
                  ).setColor(0xFF0000).setTimestamp();
                interaction.reply({ embeds: [replyEmbed], ephemeral: true }).catch(()=>{});
                safeLog(new EmbedBuilder().setDescription(`🔴 ${user.tag} closed a timepoint — ${secsToDhms(duration)} — +${coins} coins pending`).setTimestamp());
              });
            });
          });
        });
      }
    });
    return;
  }

  if (customId === 'view_point') {
    db.get('SELECT currentSeconds, pendingCoins FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, guildId], (err,row) => {
      if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
      const cur = row ? row.currentSeconds || 0 : 0;
      const pending = row ? (row.pendingCoins || '0.0') : '0.0';
      const emb = new EmbedBuilder()
        .setTitle(`📄 Your time — ${user.tag}`)
        .addFields({ name: 'Current unpaid time', value: String(secsToDhms(cur)), inline: true }, { name: 'Pending coins', value: String(pending), inline: true })
        .setColor(0x99AAB5);
      return interaction.reply({ embeds: [emb], ephemeral: true }).catch(()=>{});
    });
    return;
  }

  if (customId === 'claim_point') {
    const gId = guildId;
    db.get('SELECT pendingCoins FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, gId], async (err,row) => {
      if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
      const pending = row ? parseFloat(row.pendingCoins||'0') : 0;
      if (!pending || pending <= 0) return interaction.reply({ content: 'You have no pending coins to claim.', ephemeral: true });
      db.get('SELECT coinCard, logChannelId FROM guilds WHERE guildId = ?', [gId], async (err, gcfg) => {
        if (err) { console.error(err); return interaction.reply({ content: 'DB error', ephemeral: true }); }
        if (!gcfg || !gcfg.coinCard) return interaction.reply({ content: 'This server has no Coin Card configured. Contact an admin.', ephemeral: true });
        const payload = { cardCode: gcfg.coinCard, toId: user.id, amount: pending };
        try {
          const res = await fetch(`${API_BASE}/api/transfer/card`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          if (!res.ok) {
            const txt = await res.text().catch(()=>null);
            await interaction.reply({ content: `Claim failed: ${res.status} ${txt || ''}`, ephemeral: true }).catch(()=>{});
            await safeLog(new EmbedBuilder().setDescription(`❌ Claim failed for ${user.tag} — ${pending} coins — HTTP ${res.status} — ${txt}`).setTimestamp());
            return;
          }
          const j = await res.json().catch(()=>null);
          if (!j || j.success !== true) {
            await interaction.reply({ content: `Claim failed: API returned failure.`, ephemeral: true }).catch(()=>{});
            await safeLog(new EmbedBuilder().setDescription(`❌ Claim failed for ${user.tag} — ${pending} coins — API said fail`).setTimestamp());
            return;
          }
          const txId = j.txId || `tx:${Date.now()}`;
          db.get('SELECT currentSeconds, totalSeconds, totalReceived FROM user_guild WHERE userId = ? AND guildId = ?', [user.id, gId], (err, urow) => {
            if (err) { console.error(err); }
            const totalRecGuild = urow ? parseFloat(urow.totalReceived||'0') : 0;
            const newTotalRecGuild = toFloat8(totalRecGuild + pending);
            db.run('UPDATE user_guild SET currentSeconds = 0, pendingCoins = "0.0", totalReceived = ? WHERE userId = ? AND guildId = ?', [newTotalRecGuild, user.id, gId], (err2) => {
              if (err2) console.error('Error updating user_guild after claim', err2);
              db.get('SELECT totalReceived, totalPending FROM user_global WHERE userId = ?', [user.id], (err, grow) => {
                if (err) console.error(err);
                const grec = grow ? parseFloat(grow.totalReceived||'0') : 0;
                const gpend = grow ? parseFloat(grow.totalPending||'0') : 0;
                db.run('UPDATE user_global SET totalReceived = ?, totalPending = ? WHERE userId = ?', [toFloat8(grec + pending), toFloat8(Math.max(0, gpend - pending)), user.id], (err3) => { if (err3) console.error(err3); });
              });
              db.run('INSERT INTO txlog(userId,guildId,amount,txId,timestamp) VALUES (?, ?, ?, ?, ?)', [user.id, gId, toFloat8(pending), txId, Date.now()], (err4) => { if (err4) console.error(err4); });
              db.run('UPDATE history SET txId = ? WHERE userId = ? AND guildId = ? AND txId IS NULL', [txId, user.id, gId], (err5) => { if (err5) console.error(err5); });
              interaction.reply({ content: `💸 Claim successful — ${toFloat8(pending)} coins sent. TX: ${txId}`, ephemeral: true }).catch(()=>{});
              safeLog(new EmbedBuilder().setDescription(`✅ ${user.tag} claimed ${toFloat8(pending)} coins — TX: ${txId}`).setTimestamp());
            });
          });
        } catch (err) {
          console.error('Claim API error', err);
          await interaction.reply({ content: `Claim failed: internal error`, ephemeral: true }).catch(()=>{});
          await safeLog(new EmbedBuilder().setDescription(`❌ Claim failed for ${user.tag} — ${pending} coins — error: ${err.message}`).setTimestamp());
        }
      });
    });
    return;
  }
}

// Periodic checker - auto-close >24h
let periodicTimer = null;
function startPeriodicChecker() {
  if (periodicTimer) return;
  periodicTimer = setInterval(() => {
    try {
      const cutoff = Date.now() - 24*3600*1000;
      db.all('SELECT userId, guildId, openTs FROM opens WHERE openTs <= ?', [cutoff], (err, rows) => {
        if (err) return console.error('Checker db error', err);
        rows.forEach(r => {
          (async () => {
            try {
              const startTs = r.openTs;
              const endTs = Date.now();
              const duration = Math.floor((endTs - startTs)/1000);

              // compute salary for user (sum of salaries)
              const salaryPerHour = await computeSalaryForUser(r.guildId, r.userId);
              const coins = toFloat8((salaryPerHour * (duration / 3600)));

              db.get('SELECT coinCard, logChannelId FROM guilds WHERE guildId = ?', [r.guildId], (err,gcfg) => {
                if (err) console.error(err);
                db.run('DELETE FROM opens WHERE userId = ?', [r.userId], (err2) => { if (err2) console.error(err2); });
                db.run('INSERT OR IGNORE INTO user_guild(userId,guildId,totalSeconds,currentSeconds,pendingCoins,totalReceived) VALUES (?, ?, 0, 0, "0.0", "0.0")', [r.userId, r.guildId], (err3) => { if (err3) console.error(err3); });
                db.get('SELECT pendingCoins,currentSeconds,totalSeconds FROM user_guild WHERE userId = ? AND guildId = ?', [r.userId, r.guildId], (err4,row) => {
                  if (err4) { console.error(err4); return; }
                  const newCur = (row?row.currentSeconds||0:0) + duration;
                  const newTot = (row?row.totalSeconds||0:0) + duration;
                  const newPending = toFloat8((parseFloat(row ? (row.pendingCoins||'0') : '0') || 0) + parseFloat(coins||'0'));
                  db.run('UPDATE user_guild SET currentSeconds = ?, totalSeconds = ?, pendingCoins = ? WHERE userId = ? AND guildId = ?', [newCur, newTot, newPending, r.userId, r.guildId], (err5) => { if (err5) console.error(err5); });
                  db.run('INSERT OR IGNORE INTO user_global(userId,totalSeconds,totalReceived,totalPending) VALUES (?,0,"0.0","0.0")', [r.userId], (err6) => { if (err6) console.error(err6); });
                  db.get('SELECT totalSeconds,totalPending FROM user_global WHERE userId = ?', [r.userId], (err7,g) => {
                    if (err7) console.error(err7);
                    const gtot = (g? g.totalSeconds||0:0) + duration;
                    const gpend = (g? parseFloat(g.totalPending||'0'):0) + parseFloat(coins||'0');
                    db.run('UPDATE user_global SET totalSeconds = ?, totalPending = ? WHERE userId = ?', [gtot, toFloat8(gpend), r.userId], (err8) => { if (err8) console.error(err8); });
                    if (gcfg && gcfg.logChannelId) {
                      fetchSendableChannel(gcfg.logChannelId).then(ch => {
                        if (ch) ch.send({ embeds: [new EmbedBuilder().setDescription(`⏰ Auto-closed open point for <@${r.userId}> (older than 24h). Duration: ${secsToDhms(duration)} — +${coins} coins pending.`).setTimestamp()] }).catch(()=>{});
                      });
                    }
                    // store history row for auto-close
                    db.run('INSERT INTO history(userId,guildId,startTs,endTs,durationSecs,coinsPaid,txId) VALUES (?, ?, ?, ?, ?, ?, ?)', [r.userId, r.guildId, startTs, endTs, duration, coins, null], (err9) => { if (err9) console.error(err9); });
                  });
                });
              });
            } catch (e) {
              console.error('Error in periodic auto-close handling', e);
            }
          })();
        });
      });
    } catch (e) {
      console.error('Periodic checker error', e);
    }
  }, 5*60*1000);
}

// Login
client.login(DISCORD_TOKEN).catch(err => {
  console.error('Login failed', err);
  process.exit(1);
});
