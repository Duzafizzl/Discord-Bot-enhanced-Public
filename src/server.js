"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
const express_1 = __importDefault(require("express"));
const discord_js_1 = require("discord.js");
const messages_1 = require("./messages");
const attachmentForwarder_1 = require("./attachmentForwarder");
const letta_client_1 = require("@letta-ai/letta-client");
const taskScheduler_1 = require("./taskScheduler");
const youtubeTranscript_1 = require("./youtubeTranscript");
const fileChunking_1 = require("./fileChunking");
const lettaStatsMonitor_1 = require("./lettaStatsMonitor");
const dailyStatsSummary_1 = require("./dailyStatsSummary");
// 🔒 AUTONOMOUS BOT-LOOP PREVENTION SYSTEM
const autonomous_1 = require("./autonomous");
// 🛠️ ADMIN COMMAND SYSTEM (Oct 16, 2025)
const adminCommands_1 = require("./adminCommands");
// Import TTS functionality
// TTS imports removed - using ElevenLabs integration instead
// 📝 CONVERSATION LOGGER (for training data)
const conversationLogger_1 = require("./conversationLogger");
// 🤖 MCP HANDLER - Rider Pi Robot Control (Dec 2025)
const mcpHandler_1 = require("./mcpHandler");
// ============================================
// 🛡️ GLOBAL ERROR HANDLERS (Nov 2025)
// ============================================
// Catch unhandled promise rejections to prevent log spam
process.on('unhandledRejection', (reason, promise) => {
    // Log concisely without full stack trace spam
    const errorMsg = reason instanceof Error ? reason.message : String(reason);
    const errorName = reason instanceof Error ? reason.name : 'UnhandledRejection';
    // Known non-critical errors that we can safely ignore
    const ignorableErrors = [
        'Opening handshake has timed out', // YouTube/Discord WebSocket timeout
        'Connect Timeout Error', // YouTube API timeout
        'timeout of 10000ms exceeded' // Axios timeout (task scheduler)
    ];
    if (ignorableErrors.some(msg => errorMsg.includes(msg))) {
        console.log(`⚠️  [${errorName}] ${errorMsg} (non-critical, ignoring)`);
    }
    else {
        console.error(`❌ Unhandled Promise Rejection: ${errorName}: ${errorMsg}`);
    }
    // Prevent default Node.js behavior (writing to stderr)
    promise.catch(() => { });
});
process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught Exception:', error.message);
    // Don't exit - let PM2 handle restarts if necessary
});
// ============================================
// 🛡️ GRACEFUL SHUTDOWN (for conversation logs)
// ============================================
// Ensure conversation logs are flushed before shutdown
async function gracefulShutdown(signal) {
    console.log(`\n🛑 Received ${signal} - performing graceful shutdown...`);
    try {
        // Flush conversation logs before exit
        console.log('📝 Flushing conversation logs...');
        await (0, conversationLogger_1.forceFlush)();
        await (0, conversationLogger_1.stopAutoFlush)();
        console.log('✅ Conversation logs flushed successfully');
    }
    catch (error) {
        console.error('❌ Error flushing conversation logs:', error);
    }
    // Give a moment for logs to be written
    await new Promise(resolve => setTimeout(resolve, 1000));
    console.log('👋 Shutting down gracefully...');
    process.exit(0);
}
// Handle shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
// Handle PM2 shutdown (kill_timeout)
// PM2 sends SIGTERM first, then SIGKILL after kill_timeout
// We have time to flush logs before SIGKILL
const app = (0, express_1.default)();
// Add JSON body parser for TTS API
app.use(express_1.default.json({ limit: '10mb' }));
const PORT = process.env.PORT || 3001;
const RESPOND_TO_DMS = process.env.RESPOND_TO_DMS === 'true';
// 🔒 DM RESTRICTION: Only allow DMs from specific user if configured
const ALLOWED_DM_USER_ID = process.env.ALLOWED_DM_USER_ID || '';
// 🔍 DEBUG: Log DM restriction status
if (ALLOWED_DM_USER_ID) {
    console.log(`🔒 DM RESTRICTION ACTIVE: Only allowing DMs to/from user ${ALLOWED_DM_USER_ID}`);
}
else {
    console.log(`⚠️  DM RESTRICTION DISABLED: ALLOWED_DM_USER_ID not set (all DMs allowed)`);
}
const RESPOND_TO_MENTIONS = process.env.RESPOND_TO_MENTIONS === 'true';
const RESPOND_TO_BOTS = process.env.RESPOND_TO_BOTS === 'true';
const RESPOND_TO_GENERIC = process.env.RESPOND_TO_GENERIC === 'true';
const ENABLE_AUTONOMOUS = process.env.ENABLE_AUTONOMOUS === 'true'; // 🔒 NEW!
const CHANNEL_ID = process.env.DISCORD_CHANNEL_ID;
const HEARTBEAT_LOG_CHANNEL_ID = process.env.HEARTBEAT_LOG_CHANNEL_ID;
const MESSAGE_REPLY_TRUNCATE_LENGTH = 100;
const ENABLE_TIMER = process.env.ENABLE_TIMER === 'true';
// 💰 TIME-BASED HEARTBEAT CONFIG (Oct 2025 - Credit-optimized)
// Different intervals and probabilities based on time of day
// Now properly saves credits because API is only called when probability succeeds!
function getHeartbeatConfigForTime() {
    const now = new Date();
    // Get configured timezone time
    const TIMEZONE = process.env.TIMEZONE || 'Europe/Berlin';
    const berlinFormatter = new Intl.DateTimeFormat('de-DE', {
        timeZone: TIMEZONE,
        hour: 'numeric',
        hour12: false
    });
    const parts = berlinFormatter.formatToParts(now);
    const hourPart = parts.find(p => p.type === 'hour');
    const hour = hourPart ? parseInt(hourPart.value, 10) : now.getUTCHours();
    console.log(`🕐 Current Berlin time: ${hour}:00`);
    if (hour >= 7 && hour < 9) {
        // Morgen (7:00-9:00): Alle 30min, 50% Chance
        return { intervalMinutes: 30, firingProbability: 0.50, description: 'Morgen (Aufwach-Check)' };
    }
    else if (hour >= 9 && hour < 12) {
        // Vormittag (9:00-12:00): Alle 45min, 33% Chance
        return { intervalMinutes: 45, firingProbability: 0.33, description: 'Vormittag (Ruhig)' };
    }
    else if (hour >= 12 && hour < 14) {
        // Mittag (12:00-14:00): Alle 15min, 33% Chance - Lunch together vibes!
        return { intervalMinutes: 15, firingProbability: 0.33, description: 'Mittag (Lunch Together)' };
    }
    else if (hour >= 14 && hour < 17) {
        // Nachmittag (14:00-17:00): Alle 30min, 40% Chance
        return { intervalMinutes: 30, firingProbability: 0.40, description: 'Nachmittag (Aktiv)' };
    }
    else if (hour >= 18 && hour < 22) {
        // Abend (18:00-22:00): Alle 20min, 50% Chance
        return { intervalMinutes: 20, firingProbability: 0.50, description: 'Abend (Prime Time)' };
    }
    else if (hour >= 22 || hour < 1) {
        // Nacht (22:00-1:00): Alle 45min, 25% Chance
        return { intervalMinutes: 45, firingProbability: 0.25, description: 'Nacht (Winddown)' };
    }
    else {
        // Deep Night (1:00-7:00): Alle 90min, 20% Chance - Max. Credit-Saving!
        return { intervalMinutes: 90, firingProbability: 0.20, description: 'Deep Night (Schlafzeit)' };
    }
}
// TTS Configuration - Removed (using ElevenLabs integration instead)
function truncateMessage(message, maxLength) {
    if (message.length > maxLength) {
        return message.substring(0, maxLength - 3) + '...';
    }
    return message;
}
function chunkText(text, limit) {
    const chunks = [];
    let i = 0;
    while (i < text.length) {
        let end = Math.min(i + limit, text.length);
        let slice = text.slice(i, end);
        if (end < text.length) {
            const lastNewline = slice.lastIndexOf('\n');
            if (lastNewline > Math.floor(limit * 0.6)) {
                end = i + lastNewline + 1;
                slice = text.slice(i, end);
            }
        }
        chunks.push(slice);
        i = end;
    }
    return chunks;
}
const client = new discord_js_1.Client({
    intents: [
        discord_js_1.GatewayIntentBits.Guilds,
        discord_js_1.GatewayIntentBits.GuildMessages,
        discord_js_1.GatewayIntentBits.MessageContent,
        discord_js_1.GatewayIntentBits.DirectMessages,
    ],
    partials: [discord_js_1.Partials.Channel]
});
// ============================================
// 🛡️ DISCORD.JS ERROR HANDLERS
// ============================================
// Catch Discord.js WebSocket and API errors to prevent crashes
client.on('error', (error) => {
    const ignorableErrors = [
        'Connect Timeout Error',
        'Opening handshake has timed out',
        'WebSocket was closed before the connection was established'
    ];
    if (ignorableErrors.some(msg => error.message.includes(msg))) {
        console.log(`⚠️  [Discord.js] ${error.message} (non-critical, ignoring)`);
    }
    else {
        console.error(`❌ [Discord.js Error] ${error.name}: ${error.message}`);
    }
});
client.on('warn', (info) => {
    console.log(`⚠️  [Discord.js Warning] ${info}`);
});
// Register attachment forwarder
(0, attachmentForwarder_1.registerAttachmentForwarder)(client);
// Discord Bot Ready Event
client.once('ready', async () => {
    console.log(`🤖 Logged in as ${client.user?.tag}!`);
    console.log(`🔒 Bot-Loop Prevention: ${ENABLE_AUTONOMOUS ? 'ENABLED ✅' : 'DISABLED ⚠️'}`);
    console.log(`🔒 Self-Spam Prevention: ${ENABLE_AUTONOMOUS ? 'Active (Max 3 consecutive) ✅' : 'DISABLED ⚠️'}`);
    // 📝 Initialize conversation logger (for training data)
    (0, conversationLogger_1.initializeLogger)();
    // 🤖 Initialize MCP Handler (Rider Pi Robot Control)
    (0, mcpHandler_1.initMCPHandler)();
    // Start background task scheduler
    (0, taskScheduler_1.startTaskCheckerLoop)(client);
    // Start Letta stats monitoring (daily summary + threshold alerts)
    (0, lettaStatsMonitor_1.startDailySummaryScheduler)(client);
    await (0, lettaStatsMonitor_1.startThresholdMonitoring)(client);
    // Start daily stats summary (Letta + Chat Stats) - runs at 0:00 Berlin time
    (0, dailyStatsSummary_1.startDailyStatsSummaryScheduler)(client);
    // TTS initialization removed - using ElevenLabs integration instead
});
// Helper function to send a message and receive a response
async function processAndSendMessage(message, messageType, conversationContext = null, customContent = null) {
    try {
        const msg = await (0, messages_1.sendMessage)(message, messageType, conversationContext, customContent);
        if (msg !== "") {
            // 🔒 Record that bot replied (for pingpong tracking)
            if (ENABLE_AUTONOMOUS && client.user?.id) {
                const wasFarewell = msg.toLowerCase().includes('gotta go') ||
                    msg.toLowerCase().includes('catch you later') ||
                    msg.toLowerCase().includes('step away');
                (0, autonomous_1.recordBotReply)(message.channel.id, client.user.id, wasFarewell);
            }
            if (msg.length <= 1900) {
                await message.reply(msg);
                console.log(`Message sent: ${msg}`);
            }
            else {
                const chunks = chunkText(msg, 1900);
                await message.reply(chunks[0]);
                for (let i = 1; i < chunks.length; i++) {
                    await new Promise(r => setTimeout(r, 200));
                    await message.channel.send(chunks[i]);
                }
                console.log(`Message sent in ${chunks.length} chunks.`);
            }
        }
    }
    catch (error) {
        console.error("🛑 Error processing and sending message:", error);
    }
}
// Function to start randomized event timer
async function startRandomEventTimer() {
    if (!ENABLE_TIMER) {
        console.log("🜂 Heartbeat feature is disabled.");
        return;
    }
    // Get time-based config
    const config = getHeartbeatConfigForTime();
    // Random interval between 50-100% of the configured interval
    const minMinutes = Math.floor(config.intervalMinutes * 0.5);
    const randomMinutes = minMinutes + Math.floor(Math.random() * (config.intervalMinutes - minMinutes));
    console.log(`🜂 💰 Heartbeat scheduled to fire in ${randomMinutes} minutes [${config.description}]`);
    const delay = randomMinutes * 60 * 1000;
    setTimeout(async () => {
        console.log(`🜂 💰 Heartbeat fired after ${randomMinutes} minutes - checking probability...`);
        // Get fresh config in case time period changed
        const currentConfig = getHeartbeatConfigForTime();
        // 💰 CREDIT SAVING: Check probability BEFORE making API call!
        const shouldFire = Math.random() < currentConfig.firingProbability;
        if (shouldFire) {
            console.log(`🜂 💰 Heartbeat triggered (${currentConfig.firingProbability * 100}% chance) [${currentConfig.description}] - API CALL WILL BE MADE`);
            // Try to fetch heartbeat log channel first, fallback to default channel
            const channelToFetch = HEARTBEAT_LOG_CHANNEL_ID || CHANNEL_ID;
            let channel = undefined;
            console.log(`🜂 [DEBUG] Channel fetch - HEARTBEAT_LOG_CHANNEL_ID=${HEARTBEAT_LOG_CHANNEL_ID}, CHANNEL_ID=${CHANNEL_ID}, channelToFetch=${channelToFetch}`);
            if (channelToFetch) {
                try {
                    console.log(`🜂 [DEBUG] Attempting to fetch channel: ${channelToFetch}`);
                    const fetchedChannel = await client.channels.fetch(channelToFetch);
                    console.log(`🜂 [DEBUG] Channel fetched successfully, has 'send': ${fetchedChannel && 'send' in fetchedChannel}`);
                    if (fetchedChannel && 'send' in fetchedChannel) {
                        channel = fetchedChannel;
                        if (HEARTBEAT_LOG_CHANNEL_ID) {
                            console.log('🜂 Using heartbeat log channel for heartbeat responses');
                        }
                        else {
                            console.log('🜂 Using default channel for heartbeat responses');
                        }
                    }
                    else {
                        console.log("⏰ Channel not found or is not a text channel.");
                    }
                }
                catch (error) {
                    console.error("⏰ Error fetching channel:", error);
                    console.error("⏰ Error details:", error instanceof Error ? error.message : String(error));
                }
            }
            else {
                console.log("⏰ No channel ID configured (HEARTBEAT_LOG_CHANNEL_ID and CHANNEL_ID both undefined)");
            }
            // 💰 ONLY make API call if probability check passed!
            const msg = await (0, messages_1.sendTimerMessage)(channel);
            if (msg !== "" && channel) {
                try {
                    await channel.send(msg);
                    console.log("🜂 Heartbeat message sent to channel");
                }
                catch (error) {
                    console.error("🜂 Error sending heartbeat message:", error);
                }
            }
            else if (!channel) {
                console.log("🜂 No CHANNEL_ID defined or channel not available; message not sent.");
            }
        }
        else {
            console.log(`🜂 💰 Heartbeat skipped - probability check failed (${(1 - currentConfig.firingProbability) * 100}% chance to skip) [${currentConfig.description}] - NO API CALL MADE`);
        }
        setTimeout(() => {
            startRandomEventTimer();
        }, 1000);
    }, delay);
}
// Handle messages
client.on('messageCreate', async (message) => {
    // 🔒 AUTONOMOUS: Track ALL messages for context (EXCEPT our own bot messages to save credits!)
    if (ENABLE_AUTONOMOUS && client.user?.id && message.author.id !== client.user.id) {
        (0, autonomous_1.trackMessage)(message, client.user.id);
    }
    // Let the attachment forwarder handle image attachments
    if (message.attachments?.size) {
        for (const [, att] of message.attachments) {
            const ct = att.contentType || att.content_type || '';
            if (typeof ct === 'string' && ct.startsWith('image/')) {
                return;
            }
        }
    }
    // 🤖 MCP COMMAND HANDLER - Rider Pi Robot Control (Dec 2025)
    // Process MCP commands from the dedicated channel BEFORE other filters
    // This allows Letta to control the robot via Discord messages
    if (await (0, mcpHandler_1.handleMCPCommand)(message, client)) {
        return; // MCP command was handled
    }
    // Filter channels if CHANNEL_ID is set, but ALWAYS allow DMs through
    if (CHANNEL_ID && message.guild && message.channel.id !== CHANNEL_ID) {
        console.log(`📩 Ignoring message from other channels (only listening on channel=${CHANNEL_ID})...`);
        return;
    }
    if (message.author.id === client.user?.id) {
        console.log(`📩 Ignoring message from myself (NOT sending to Letta - saves credits!)...`);
        return;
    }
    // 🛠️ ADMIN COMMAND HANDLER (Oct 16, 2025)
    // CRITICAL: Check BEFORE autonomous mode to prevent blocking!
    // Admin commands should ALWAYS work, even with autonomous mode enabled
    if (message.content.startsWith('!') && client.user?.id) {
        const adminResponse = await (0, adminCommands_1.handleAdminCommand)(message, client.user.id);
        if (adminResponse) {
            // Admin command was handled
            await message.reply(adminResponse);
            return;
        }
        // Not an admin command, continue to autonomous check
        // (autonomous will ignore it anyway)
    }
    // 🔒 AUTONOMOUS: Check if we should respond (bot-loop prevention)
    let conversationContext = null;
    if (ENABLE_AUTONOMOUS && client.user?.id) {
        const decision = await (0, autonomous_1.shouldRespondAutonomously)(message, client.user.id, {
            respondToDMs: RESPOND_TO_DMS,
            respondToMentions: RESPOND_TO_MENTIONS,
            respondToBots: RESPOND_TO_BOTS,
            enableAutonomous: ENABLE_AUTONOMOUS
        });
        if (!decision.shouldRespond) {
            console.log(`🔒 Not responding: ${decision.reason}`);
            return;
        }
        // Save context to pass to Letta (only for Channels, NOT for DMs!)
        const isDM = message.guild === null;
        conversationContext = (!isDM && decision.context) ? decision.context : null;
        console.log(`🔒 Responding: ${decision.reason}`);
    }
    else {
        // Legacy behavior (no autonomous mode)
        if (message.author.bot && !RESPOND_TO_BOTS) {
            console.log(`📩 Ignoring other bot...`);
            return;
        }
    }
    // 📄 FILE CHUNK REQUEST HANDLER (Nov 20, 2025)
    // Check for file chunk requests BEFORE YouTube chunk requests
    console.log('📄 Checking for file chunk requests...');
    const fileChunkResponse = (0, fileChunking_1.handleFileChunkRequest)(message.content);
    if (fileChunkResponse) {
        console.log('📖 File chunk request detected - processing');
        console.log(`📖 Request content: ${message.content.substring(0, 100)}...`);
        console.log('📖 Sending file chunk response to Letta');
        // Determine message type
        let messageType = messages_1.MessageType.GENERIC;
        if (message.guild === null) {
            messageType = messages_1.MessageType.DM;
        }
        else if (message.mentions.has(client.user || '') || message.reference) {
            messageType = messages_1.MessageType.MENTION;
        }
        const msg = await (0, messages_1.sendMessage)(message, messageType, conversationContext, fileChunkResponse);
        if (msg !== "") {
            // 🔒 Record that bot replied (for pingpong tracking)
            if (ENABLE_AUTONOMOUS && client.user?.id) {
                const wasFarewell = msg.toLowerCase().includes('gotta go') ||
                    msg.toLowerCase().includes('catch you later') ||
                    msg.toLowerCase().includes('step away');
                (0, autonomous_1.recordBotReply)(message.channel.id, client.user.id, wasFarewell);
            }
            if (msg.length <= 1900) {
                await message.reply(msg);
                console.log(`Message sent: ${msg}`);
            }
            else {
                const chunks = chunkText(msg, 1900);
                await message.reply(chunks[0]);
                for (let i = 1; i < chunks.length; i++) {
                    await new Promise(r => setTimeout(r, 200));
                    await message.channel.send(chunks[i]);
                }
                console.log(`Message sent in ${chunks.length} chunks.`);
            }
        }
        return;
    }
    // 🎥 YOUTUBE CHUNK/INFO REQUEST HANDLER (Oct 26, 2025)
    // Check for chunk/info requests BEFORE processing YouTube links
    console.log(`🎥 Checking for YouTube chunk/info requests in: "${message.content.substring(0, 200)}"`);
    const chunkResponse = (0, youtubeTranscript_1.handleChunkRequest)(message.content);
    if (chunkResponse) {
        console.log('✅ YouTube chunk/info request detected - processing');
        console.log(`📖 Request content: ${message.content.substring(0, 100)}...`);
        console.log(`📖 Chunk response length: ${chunkResponse.length} characters`);
        console.log('📖 Sending chunk/info response to Letta');
        // Determine message type
        let messageType = messages_1.MessageType.GENERIC;
        if (message.guild === null) {
            messageType = messages_1.MessageType.DM;
        }
        else if (message.mentions.has(client.user || '') || message.reference) {
            messageType = messages_1.MessageType.MENTION;
        }
        const msg = await (0, messages_1.sendMessage)(message, messageType, conversationContext, chunkResponse);
        if (msg !== "") {
            // 🔒 Record that bot replied (for pingpong tracking)
            if (ENABLE_AUTONOMOUS && client.user?.id) {
                const wasFarewell = msg.toLowerCase().includes('gotta go') ||
                    msg.toLowerCase().includes('catch you later') ||
                    msg.toLowerCase().includes('step away');
                (0, autonomous_1.recordBotReply)(message.channel.id, client.user.id, wasFarewell);
            }
            if (msg.length <= 1900) {
                await message.reply(msg);
                console.log(`Message sent: ${msg}`);
            }
            else {
                const chunks = chunkText(msg, 1900);
                await message.reply(chunks[0]);
                for (let i = 1; i < chunks.length; i++) {
                    await new Promise(r => setTimeout(r, 200));
                    await message.channel.send(chunks[i]);
                }
                console.log(`Message sent in ${chunks.length} chunks.`);
            }
        }
        return;
    }
    // 🎥 PREPROCESS YOUTUBE LINKS (Oct 26, 2025)
    // Automatically fetch and attach transcripts to messages
    console.log('🎥 Checking message for YouTube links...');
    let statusMessage = null;
    // Check if message contains YouTube links
    const youtubeRegex = /https?:\/\/(?:www\.)?(?:youtube\.com\/watch\?v=|youtu\.be\/)([a-zA-Z0-9_-]{11})/g;
    const hasYouTubeLinks = youtubeRegex.test(message.content);
    if (hasYouTubeLinks) {
        console.log('🎥 YouTube link(s) detected in message!');
        // Send status message to user
        statusMessage = await message.reply('🎥 Fetching video transcript(s)...').catch(() => null);
        console.log('📺 User notified: Fetching YouTube transcript(s)');
    }
    else {
        console.log('🎥 No YouTube links found - skipping transcript processing');
    }
    const youtubeResult = await (0, youtubeTranscript_1.preprocessYouTubeLinks)(message.content, async () => await message.channel.sendTyping());
    // Delete status message and send completion info
    if (statusMessage) {
        await statusMessage.delete().catch(() => console.log('⚠️ Could not delete status message'));
        if (youtubeResult.videosProcessed > 0) {
            const statusText = youtubeResult.videosFailed > 0
                ? `✅ Processed ${youtubeResult.videosProcessed} video(s) | ⚠️ ${youtubeResult.videosFailed} failed (no transcript)`
                : `✅ Processed ${youtubeResult.videosProcessed} video transcript(s) - sending to Letta...`;
            const completionMsg = await message.reply(statusText).catch(() => null);
            console.log(`📺 ${statusText}`);
            // Delete completion message after 3 seconds
            if (completionMsg) {
                setTimeout(async () => {
                    await completionMsg.delete().catch(() => { });
                }, 3000);
            }
        }
    }
    // Store processed content for use in message handlers
    const processedContent = youtubeResult.content !== message.content ? youtubeResult.content : null;
    // Handle DMs
    if (message.guild === null) {
        console.log(`📩 Received DM from ${message.author.username} (${message.author.id}): ${message.content}`);
        // 🔒 DM RESTRICTION: Check if DM is from allowed user (if configured)
        if (ALLOWED_DM_USER_ID && message.author.id !== ALLOWED_DM_USER_ID) {
            console.log(`🔒 DM restriction: Ignoring DM from ${message.author.id} (not ${ALLOWED_DM_USER_ID})`);
            await message.reply(`❌ Sorry, I can only receive DMs from the authorized user.`);
            return;
        }
        if (RESPOND_TO_DMS) {
            // If content was modified (transcript added), send with custom content
            if (processedContent) {
                console.log('📺 Transcript(s) attached to message - sending to Letta');
            }
            processAndSendMessage(message, messages_1.MessageType.DM, conversationContext, processedContent);
        }
        else {
            console.log(`📩 Ignoring DM...`);
        }
        return;
    }
    // Handle mentions and replies
    if (RESPOND_TO_MENTIONS && (message.mentions.has(client.user || '') || message.reference)) {
        console.log(`📩 Received message from ${message.author.username}: ${message.content}`);
        await message.channel.sendTyping();
        let messageType = messages_1.MessageType.MENTION;
        if (message.reference && message.reference.messageId) {
            const originalMessage = await message.channel.messages.fetch(message.reference.messageId);
            if (originalMessage.author.id === client.user?.id) {
                messageType = messages_1.MessageType.REPLY;
            }
            else {
                messageType = message.mentions.has(client.user || '') ? messages_1.MessageType.MENTION : messages_1.MessageType.GENERIC;
            }
        }
        // If content was modified (transcript added), send with custom content
        if (processedContent) {
            console.log('📺 Transcript(s) attached to message - sending to Letta');
        }
        const msg = await (0, messages_1.sendMessage)(message, messageType, conversationContext, processedContent);
        if (msg !== "") {
            // 🔒 Record bot reply
            if (ENABLE_AUTONOMOUS && client.user?.id) {
                const wasFarewell = msg.toLowerCase().includes('gotta go') ||
                    msg.toLowerCase().includes('catch you later') ||
                    msg.toLowerCase().includes('step away');
                (0, autonomous_1.recordBotReply)(message.channel.id, client.user.id, wasFarewell);
            }
            await message.reply(msg);
        }
        return;
    }
    // Generic messages
    if (RESPOND_TO_GENERIC) {
        console.log(`📩 Received (non-mention) message from ${message.author.username}: ${message.content}`);
        // If content was modified (transcript added), send with custom content
        if (processedContent) {
            console.log('📺 Transcript(s) attached to message - sending to Letta');
        }
        processAndSendMessage(message, messages_1.MessageType.GENERIC, conversationContext, processedContent);
        return;
    }
});
// ============================================
// TTS API Routes - REMOVED
// ============================================
// Local Piper TTS system removed in favor of ElevenLabs integration
// See tools/send_voice_message.py for voice message functionality
// ============================================
// Midjourney Proxy API
// ============================================
const MIDJOURNEY_CHANNEL_ID = process.env.MIDJOURNEY_CHANNEL_ID;
const MIDJOURNEY_BOT_ID = '936929561302675456'; // Official Midjourney bot ID
app.post('/api/midjourney/generate', (req, res) => {
    (async () => {
        try {
            const { prompt, cref, sref, ar, v, cw, sw, style, chaos, quality } = req.body;
            if (!prompt) {
                return res.status(400).json({ error: 'Missing required parameter: prompt' });
            }
            if (!MIDJOURNEY_CHANNEL_ID) {
                return res.status(500).json({ error: 'MIDJOURNEY_CHANNEL_ID not configured' });
            }
            // Get Midjourney channel
            const channel = await client.channels.fetch(MIDJOURNEY_CHANNEL_ID);
            if (!channel || !('send' in channel)) {
                return res.status(500).json({ error: 'Midjourney channel not found or invalid' });
            }
            // Build Midjourney command
            let mjCommand = `/imagine prompt: ${prompt}`;
            // Add parameters
            if (ar && ar !== '1:1')
                mjCommand += ` --ar ${ar}`;
            if (v)
                mjCommand += ` --v ${v}`;
            if (style && style !== 'default')
                mjCommand += ` --style ${style}`;
            if (chaos && chaos > 0)
                mjCommand += ` --chaos ${chaos}`;
            if (quality && quality !== 1)
                mjCommand += ` --q ${quality}`;
            // Add character reference
            if (cref) {
                mjCommand += ` --cref ${cref}`;
                if (cw && cw !== 100)
                    mjCommand += ` --cw ${cw}`;
            }
            // Add style reference
            if (sref) {
                mjCommand += ` --sref ${sref}`;
                if (sw && sw !== 100)
                    mjCommand += ` --sw ${sw}`;
            }
            console.log(`🎨 [MJ Proxy] Sending command: ${mjCommand.substring(0, 100)}...`);
            // Send the command
            const sentMessage = await channel.send(mjCommand);
            const commandTimestamp = sentMessage.createdTimestamp;
            console.log(`⏳ [MJ Proxy] Waiting for Midjourney response...`);
            // Poll for Midjourney response
            const maxWaitTime = 120000; // 2 minutes
            const pollInterval = 3000; // 3 seconds
            const startTime = Date.now();
            while (Date.now() - startTime < maxWaitTime) {
                await new Promise(resolve => setTimeout(resolve, pollInterval));
                // Fetch recent messages
                const messages = await channel.messages.fetch({ limit: 10 });
                // Look for Midjourney's response
                for (const msg of messages.values()) {
                    // Check if from Midjourney bot
                    if (msg.author.id !== MIDJOURNEY_BOT_ID)
                        continue;
                    // Check if after our command
                    if (msg.createdTimestamp <= commandTimestamp)
                        continue;
                    // Check for attachments (completed image)
                    if (msg.attachments.size > 0) {
                        const attachment = msg.attachments.first();
                        if (attachment) {
                            const elapsed = Math.floor((Date.now() - startTime) / 1000);
                            console.log(`✅ [MJ Proxy] Image generated in ${elapsed}s`);
                            return res.json({
                                status: 'completed',
                                image_url: attachment.url,
                                filename: attachment.name,
                                width: attachment.width || 0,
                                height: attachment.height || 0,
                                generation_time: `${elapsed}s`,
                                command: mjCommand,
                                message_id: msg.id
                            });
                        }
                    }
                }
                console.log(`⏳ [MJ Proxy] Still waiting... (${Math.floor((Date.now() - startTime) / 1000)}s elapsed)`);
            }
            // Timeout
            return res.status(408).json({
                status: 'timeout',
                error: `Generation timed out after ${maxWaitTime / 1000}s`,
                note: 'Check Discord channel manually - generation might still complete'
            });
        }
        catch (error) {
            console.error('❌ [MJ Proxy] Error:', error);
            return res.status(500).json({
                status: 'error',
                error: error.message || String(error)
            });
        }
    })().catch((e) => {
        console.error('❌ [MJ Proxy] Uncaught error:', e);
        res.status(500).json({ status: 'error', error: String(e?.message || e) });
    });
});
// ============================================
// Health Check Endpoints
// ============================================
// Letta health check
app.get('/tool/letta-health', (req, res) => {
    (async () => {
        const baseUrl = (process.env.LETTA_BASE_URL || 'https://api.letta.com').replace(/\/$/, '');
        const agentId = process.env.LETTA_AGENT_ID;
        const token = process.env.LETTA_API_KEY;
        if (!agentId || !token) {
            res.status(400).json({ ok: false, error: 'Missing LETTA_AGENT_ID or LETTA_API_KEY' });
            return;
        }
        const imageUrl = typeof req.query.image_url === 'string' ? req.query.image_url : undefined;
        const lc = new letta_client_1.LettaClient({ token, baseUrl, timeout: 60000 });
        const content = imageUrl
            ? [
                { type: 'image', source: { type: 'url', url: imageUrl } },
                { type: 'text', text: 'Health check: describe this image.' }
            ]
            : [{ type: 'text', text: 'this is a Healthtest by Cursor' }];
        const t0 = Date.now();
        await lc.agents.messages.create(agentId, { messages: [{ role: 'user', content }] });
        const dt = Date.now() - t0;
        res.json({ ok: true, baseUrl, vision: !!imageUrl, latency_ms: dt });
    })().catch((e) => {
        res.status(500).json({ ok: false, error: String(e?.message || e) });
    });
});
// General health check
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        service: 'Discord-Letta Bot',
        uptime: process.uptime(),
        discord: client.isReady() ? 'connected' : 'disconnected',
        voice: 'elevenlabs',
        autonomous: ENABLE_AUTONOMOUS ? 'enabled' : 'disabled',
        timestamp: new Date().toISOString(),
    });
});
// ============================================
// Start Server
// ============================================
app.listen(PORT, () => {
    console.log('');
    console.log('🔥 ============================================');
    console.log(`🚀 Server listening on port ${PORT}`);
    console.log('🔥 ============================================');
    console.log('');
    console.log('Services:');
    console.log(`  - Discord Bot: ${RESPOND_TO_DMS || RESPOND_TO_MENTIONS || RESPOND_TO_GENERIC ? 'Enabled' : 'Disabled'}`);
    console.log(`  - Heartbeat: ${ENABLE_TIMER ? 'Enabled' : 'Disabled'}`);
    console.log(`  - TTS API: Disabled (using ElevenLabs integration instead)`);
    console.log(`  - Bot-Loop Prevention: ${ENABLE_AUTONOMOUS ? 'ENABLED 🔒' : 'DISABLED ⚠️'}`);
    // 🔒 DM RESTRICTION STATUS
    if (ALLOWED_DM_USER_ID) {
        console.log(`  - 🔒 DM Restriction: ACTIVE (only user ${ALLOWED_DM_USER_ID})`);
    }
    else {
        console.log(`  - ⚠️  DM Restriction: DISABLED (ALLOWED_DM_USER_ID not set - all DMs allowed)`);
    }
    console.log('');
    const token = String(process.env.DISCORD_TOKEN || '').trim();
    client.login(token);
    startRandomEventTimer();
});
