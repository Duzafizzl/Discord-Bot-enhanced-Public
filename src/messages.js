"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MessageType = void 0;
exports.sendMessage = sendMessage;
exports.sendTimerMessage = sendTimerMessage;
exports.sendTaskMessage = sendTaskMessage;
const grokClient_1 = require("./grokClient");
const fileChunking_1 = require("./fileChunking");
const conversationLogger_1 = require("./conversationLogger");
var MessageType;
(function (MessageType) {
    MessageType[MessageType["GENERIC"] = 0] = "GENERIC";
    MessageType[MessageType["MENTION"] = 1] = "MENTION";
    MessageType[MessageType["REPLY"] = 2] = "REPLY";
    MessageType[MessageType["DM"] = 3] = "DM";
})(MessageType || (exports.MessageType = MessageType = {}));
// Grok Client Configuration
const GROK_BASE_URL = process.env.GROK_BASE_URL || 'http://localhost:8091';
const GROK_SESSION_ID = process.env.GROK_SESSION_ID || 'discord-bot';
const GROK_MODEL = process.env.GROK_MODEL || 'grok-4-1-fast-reasoning';
const USE_SENDER_PREFIX = process.env.LETTA_USE_SENDER_PREFIX === 'true'; // Keep for compatibility
const SURFACE_ERRORS = process.env.SURFACE_ERRORS === 'true';
const GROK_API_TIMEOUT_MS = parseInt(process.env.GROK_API_TIMEOUT_MS || '300000', 10);
// Initialize Grok Client
const grokClient = new grokClient_1.GrokClient({
    baseUrl: GROK_BASE_URL,
    sessionId: GROK_SESSION_ID,
    model: GROK_MODEL,
    timeout: GROK_API_TIMEOUT_MS,
});
/**
 * Send a user message to Grok and get a response
 */
async function sendMessage(discordMessageObject, messageType, conversationContext = null, customContent = null) {
    const { author: { username: senderName, id: senderId }, content: originalMessage } = discordMessageObject;
    // Use custom content if provided (e.g. for file chunks or transcripts), otherwise use original message
    const message = customContent || originalMessage;
    // Generate current timestamp (configured timezone) for this message
    const TIMEZONE = process.env.TIMEZONE || 'Europe/Berlin';
    let timestampString = '';
    try {
        const now = new Date();
        if (isNaN(now.getTime())) {
            throw new Error('Invalid system time');
        }
        const dateFormatter = new Intl.DateTimeFormat('de-DE', {
            timeZone: TIMEZONE,
            weekday: 'short',
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        });
        const formatted = dateFormatter.format(now);
        const berlinTime = formatted.replace(/^(\w+)\./, '$1');
        timestampString = `, time=${berlinTime}`;
    }
    catch (err) {
        console.error('⚠️ Timestamp generation failed:', err instanceof Error ? err.message : err);
        timestampString = '';
    }
    // Sender receipt with Discord ID and timestamp
    const senderNameReceipt = `${senderName} (id=${senderId}${timestampString})`;
    // Extract channel context
    const channel = discordMessageObject.channel;
    const channelId = channel.id;
    const channelType = channel.type;
    const isDM = channelType === 1;
    const channelName = isDM ? "DM" : (channel.name || "unknown-channel");
    const channelContext = isDM
        ? `DM`
        : `#${channelName} (channel_id=${channelId})`;
    // Process file attachments
    let attachmentInfo = '';
    if (discordMessageObject.attachments && discordMessageObject.attachments.size > 0) {
        const nonImageAttachments = Array.from(discordMessageObject.attachments.values()).filter(att => {
            const ct = att.contentType || '';
            return ct && !ct.startsWith('image/');
        });
        if (nonImageAttachments.length > 0) {
            console.log(`📎 Processing ${nonImageAttachments.length} non-image attachment(s)...`);
            const attachmentPromises = nonImageAttachments.map(async (att) => {
                const name = att.name || 'unknown';
                const url = att.url || '';
                const type = att.contentType || 'unknown';
                const size = att.size || 0;
                try {
                    const processed = await (0, fileChunking_1.processFileAttachment)(name, url, type, size);
                    return processed;
                }
                catch (err) {
                    console.error(`⚠️ Failed to process attachment ${name}:`, err);
                    const sizeStr = size > 1024 * 1024 ? `${(size / 1024 / 1024).toFixed(1)}MB` : `${(size / 1024).toFixed(0)}KB`;
                    return `- \`${name}\` (${type}, ${sizeStr})\n  URL: ${url}\n  ⚠️ Auto-processing failed`;
                }
            });
            const processedAttachments = await Promise.all(attachmentPromises);
            attachmentInfo = '\n\n📎 **Attachments:**\n' + processedAttachments.join('\n');
            console.log(`✅ Processed ${processedAttachments.length} attachment(s)`);
        }
    }
    // Build message content with optional conversation context
    let messageContent;
    if (USE_SENDER_PREFIX) {
        const baseMessage = messageType === MessageType.MENTION
            ? `[${senderNameReceipt} sent a message mentioning you in ${channelContext}] ${message}${attachmentInfo}`
            : messageType === MessageType.REPLY
                ? `[${senderNameReceipt} replied to you in ${channelContext}] ${message}${attachmentInfo}`
                : messageType === MessageType.DM
                    ? `[${senderNameReceipt} sent you a direct message] ${message}${attachmentInfo}`
                    : `[${senderNameReceipt} sent a message in ${channelContext}] ${message}${attachmentInfo}`;
        messageContent = conversationContext
            ? `${conversationContext}\n\n${baseMessage}`
            : baseMessage;
    }
    else {
        messageContent = conversationContext
            ? `${conversationContext}\n\n${message}${attachmentInfo}`
            : message + attachmentInfo;
    }
    // Log user message
    const attachmentCount = discordMessageObject.attachments?.size || 0;
    (0, conversationLogger_1.logUserMessage)(message, channelId, channelName, senderId, senderName, discordMessageObject.id, isDM, attachmentCount);
    // Create Grok message
    const grokMessage = {
        role: "user",
        content: messageContent
    };
    // Log Letta input (kept for compatibility with logging system)
    (0, conversationLogger_1.logLettaInput)(messageContent, conversationContext, [grokMessage], channelId, channelName, senderId, senderName, GROK_SESSION_ID);
    // Send typing indicator
    void discordMessageObject.channel.sendTyping();
    const typingInterval = setInterval(() => {
        void discordMessageObject.channel
            .sendTyping()
            .catch(err => console.error('Error refreshing typing indicator:', err));
    }, 8000);
    try {
        console.log(`🛜 Sending message to Grok API with streaming (session=${GROK_SESSION_ID}): ${JSON.stringify(grokMessage)}`);
        // Send request to Grok API with streaming
        const request = {
            messages: [grokMessage],
            session_id: GROK_SESSION_ID,
            message_type: messageType === MessageType.DM ? 'inbox' : 'inbox',
        };
        let agentMessageResponse = '';
        let thinkingContent = '';
        const toolCalls = [];
        let tokens = null;
        // Process streaming response
        for await (const chunk of grokClient.chatStream(request)) {
            console.log(`📦 [STREAM CHUNK] Event: ${chunk.event}`);
            if (chunk.event === 'thinking' && chunk.data) {
                const content = typeof chunk.data === 'string' ? chunk.data : chunk.data.content || '';
                console.log(`💭 [THINKING] ${content.substring(0, 100)}...`);
                thinkingContent += content;
            }
            else if (chunk.event === 'content' && chunk.data) {
                const content = typeof chunk.data === 'string' ? chunk.data : chunk.data.content || '';
                console.log(`💬 [CONTENT] ${content.substring(0, 100)}...`);
                agentMessageResponse += content;
            }
            else if (chunk.event === 'tool_call' && chunk.data) {
                const toolName = chunk.data.name || 'unknown';
                const toolArgs = chunk.data.arguments || {};
                console.log(`🔧 [TOOL CALL] ${toolName}(${JSON.stringify(toolArgs).substring(0, 100)}...)`);
                toolCalls.push({
                    name: toolName,
                    arguments: toolArgs
                });
            }
            else if (chunk.event === 'done') {
                console.log(`✅ [STREAM DONE] Total content: ${agentMessageResponse.length} chars`);
                if (chunk.data && chunk.data.tokens) {
                    tokens = chunk.data.tokens;
                    const t = chunk.data.tokens;
                    console.log(`📊 Tokens: ${t.prompt} prompt + ${t.completion} completion = ${t.total} total`);
                }
            }
        }
        clearInterval(typingInterval);
        if (!agentMessageResponse || !agentMessageResponse.trim()) {
            console.warn('⚠️ Received empty response from Grok API');
            return SURFACE_ERRORS
                ? "Beep boop. I thought about your message but forgot to respond 🤔 - please send it again!"
                : "";
        }
        // Log bot response
        (0, conversationLogger_1.logBotResponse)(agentMessageResponse, channelId, channelName, senderId, senderName, undefined, isDM);
        // Log conversation turn
        (0, conversationLogger_1.logConversationTurn)(messageContent, agentMessageResponse, conversationContext, [grokMessage], channelId, channelName, senderId, senderName, GROK_SESSION_ID);
        // Log thinking/reasoning if available
        if (thinkingContent) {
            console.log(`💭 Thinking: ${thinkingContent.substring(0, 100)}...`);
        }
        // Log tool calls summary if available
        if (toolCalls.length > 0) {
            console.log(`🔧 Tool calls summary: ${toolCalls.length} total`);
            toolCalls.forEach((tool, idx) => {
                console.log(`  ${idx + 1}. ${tool.name}(${JSON.stringify(tool.arguments).substring(0, 100)}...)`);
            });
        }
        return agentMessageResponse;
    }
    catch (error) {
        clearInterval(typingInterval);
        console.error("❌ Error communicating with Grok API:", error);
        const errorMessage = error instanceof Error ? error.message : String(error);
        if (SURFACE_ERRORS) {
            if (errorMessage.includes('timeout')) {
                return "⚠️ **Timeout Error**\n> The AI service took too long to respond. Please try again!";
            }
            else if (errorMessage.includes('ECONNREFUSED') || errorMessage.includes('ENOTFOUND')) {
                return "⚠️ **Connection Error**\n> Cannot connect to the AI service. Please check if nate_api_substrate is running!";
            }
            else {
                return `⚠️ **Error**\n> ${errorMessage}\n\nPlease try again or contact support if the issue persists.`;
            }
        }
        return "";
    }
}
/**
 * Send a timer/heartbeat message to Grok
 */
async function sendTimerMessage(channel) {
    if (!channel) {
        console.log('⏰ No channel provided for timer message');
        return "";
    }
    console.log('🜂 Generating heartbeat message...');
    try {
        // Create heartbeat request
        const request = {
            messages: [{
                    role: "system",
                    content: "Generate a heartbeat message. Check in with the user, share what you're thinking about, or provide relevant updates. Keep it natural and conversational."
                }],
            session_id: GROK_SESSION_ID,
            message_type: 'heartbeat',
        };
        const response = await grokClient.chat(request);
        const heartbeatMessage = response.message?.content || '';
        if (heartbeatMessage && heartbeatMessage.trim()) {
            // Log heartbeat
            (0, conversationLogger_1.logHeartbeat)(heartbeatMessage, channel.id, channel.name || 'unknown');
            console.log(`🜂 Heartbeat generated: ${heartbeatMessage.substring(0, 100)}...`);
            return heartbeatMessage;
        }
        console.warn('⚠️ Empty heartbeat response');
        return "";
    }
    catch (error) {
        console.error("❌ Error generating heartbeat:", error);
        return "";
    }
}
/**
 * Send a scheduled task message to Grok
 */
async function sendTaskMessage(taskName, taskPrompt) {
    console.log(`📅 Executing scheduled task: ${taskName}`);
    try {
        // Log task execution
        (0, conversationLogger_1.logTask)(taskName, 'scheduled_task', taskPrompt, undefined, undefined);
        // Create task request
        const request = {
            messages: [{
                    role: "user",
                    content: `[SCHEDULED TASK: ${taskName}] ${taskPrompt}`
                }],
            session_id: GROK_SESSION_ID,
            message_type: 'task',
        };
        const response = await grokClient.chat(request);
        const taskResponse = response.message?.content || '';
        if (taskResponse && taskResponse.trim()) {
            // Log task response
            (0, conversationLogger_1.logTaskResponse)(taskResponse, taskName, undefined, undefined);
            console.log(`📅 Task completed: ${taskName}`);
            return taskResponse;
        }
        console.warn(`⚠️ Empty task response for: ${taskName}`);
        return "";
    }
    catch (error) {
        console.error(`❌ Error executing task "${taskName}":`, error);
        return "";
    }
}
