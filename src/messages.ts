import { Message, OmitPartialGroupDMChannel } from "discord.js";
import { GrokClient, GrokMessage, GrokChatRequest } from "./grokClient";
import { processFileAttachment } from "./fileChunking";
import {
  logUserMessage,
  logBotResponse,
  logHeartbeat,
  logTask,
  logTaskResponse,
  logLettaInput,
  logConversationTurn,
  getRecentConversationTurns
} from "./conversationLogger";

export enum MessageType {
  GENERIC = 0,
  MENTION = 1,
  REPLY = 2,
  DM = 3,
}

// Grok Client Configuration
const GROK_BASE_URL = process.env.GROK_BASE_URL || 'http://localhost:8091';
const GROK_SESSION_ID = process.env.GROK_SESSION_ID || 'discord-bot';
const GROK_MODEL = process.env.GROK_MODEL || 'mistralai/mistral-large-2512';
const GROK_MAX_TOKENS = parseInt(process.env.GROK_MAX_TOKENS || '8192', 10);  // Allow longer responses
const USE_SENDER_PREFIX = process.env.USE_SENDER_PREFIX === 'true';
const SURFACE_ERRORS = process.env.SURFACE_ERRORS === 'true';
const GROK_API_TIMEOUT_MS = parseInt(process.env.GROK_API_TIMEOUT_MS || '300000', 10);
const TIMEZONE = process.env.TIMEZONE || 'America/New_York';
const LOCALE = process.env.LOCALE || 'en-US';

// Initialize Grok Client
const grokClient = new GrokClient({
  baseUrl: GROK_BASE_URL,
  sessionId: GROK_SESSION_ID,
  model: GROK_MODEL,
  timeout: GROK_API_TIMEOUT_MS,
  maxTokens: GROK_MAX_TOKENS,  // Pass max tokens to client
});

/**
 * Send a user message to Grok and get a response
 */
async function sendMessage(
  discordMessageObject: OmitPartialGroupDMChannel<Message<boolean>>,
  messageType: MessageType,
  conversationContext: string | null = null,
  customContent: string | null = null
): Promise<string> {
  const { author: { username: senderName, id: senderId }, content: originalMessage } =
    discordMessageObject;

  // Use custom content if provided (e.g. for file chunks or transcripts), otherwise use original message
  const message = customContent || originalMessage;

  // Generate current timestamp (configured timezone) for this message
  // Timezone is already defined at the top of the file
  let timestampString = '';
  try {
    const now = new Date();
    if (isNaN(now.getTime())) {
      throw new Error('Invalid system time');
    }

    const dateFormatter = new Intl.DateTimeFormat(LOCALE, {
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
  } catch (err) {
    console.error('⚠️ Timestamp generation failed:', err instanceof Error ? err.message : err);
    timestampString = '';
  }

  // Sender receipt with Discord ID and timestamp
  const senderNameReceipt = `${senderName} (id=${senderId}${timestampString})`;

  // Extract channel context
  const channel = discordMessageObject.channel;
  const channelId = channel.id;
  const channelType = (channel as any).type;
  const isDM = channelType === 1;
  const channelName = isDM ? "DM" : ((channel as any).name || "unknown-channel");
  const channelContext = isDM
    ? `DM`
    : `#${channelName} (channel_id=${channelId})`;

  // Process file attachments
  let attachmentInfo = '';
  if (discordMessageObject.attachments && discordMessageObject.attachments.size > 0) {
    const nonImageAttachments = Array.from(discordMessageObject.attachments.values()).filter(att => {
      const ct = (att as any).contentType || '';
      return ct && !ct.startsWith('image/');
    });

    if (nonImageAttachments.length > 0) {
      console.log(`📎 Processing ${nonImageAttachments.length} non-image attachment(s)...`);

      const attachmentPromises = nonImageAttachments.map(async (att) => {
        const name = (att as any).name || 'unknown';
        const url = (att as any).url || '';
        const type = (att as any).contentType || 'unknown';
        const size = (att as any).size || 0;

        try {
          const processed = await processFileAttachment(name, url, type, size);
          return processed;
        } catch (err) {
          console.error(`⚠️ Failed to process attachment ${name}:`, err);
          const sizeStr = size > 1024*1024 ? `${(size/1024/1024).toFixed(1)}MB` : `${(size/1024).toFixed(0)}KB`;
          return `- \`${name}\` (${type}, ${sizeStr})\n  URL: ${url}\n  ⚠️ Auto-processing failed`;
        }
      });

      const processedAttachments = await Promise.all(attachmentPromises);
      attachmentInfo = '\n\n📎 **Attachments:**\n' + processedAttachments.join('\n');
      console.log(`✅ Processed ${processedAttachments.length} attachment(s)`);
    }
  }

  // Build message content with optional conversation context
  let messageContent: string;

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
  } else {
    messageContent = conversationContext
      ? `${conversationContext}\n\n${message}${attachmentInfo}`
      : message + attachmentInfo;
  }

  // Log user message
  const attachmentCount = discordMessageObject.attachments?.size || 0;
  logUserMessage(
    message,
    channelId,
    channelName,
    senderId,
    senderName,
    discordMessageObject.id,
    isDM,
    attachmentCount
  );

  // Create Grok message
  const grokMessage: GrokMessage = {
    role: "user",
    content: messageContent
  };

  // Log Letta input (kept for compatibility with logging system)
  logLettaInput(
    messageContent,
    conversationContext,
    [grokMessage],
    channelId,
    channelName,
    senderId,
    senderName,
    GROK_SESSION_ID
  );

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
    const request: GrokChatRequest = {
      messages: [grokMessage],
      session_id: GROK_SESSION_ID,
      message_type: messageType === MessageType.DM ? 'inbox' : 'inbox',
      max_tokens: GROK_MAX_TOKENS,  // Explicitly set max tokens
    };

    console.log(`📊 Request config: max_tokens=${request.max_tokens}, model=${GROK_MODEL}, session=${GROK_SESSION_ID}`);

    let agentMessageResponse = '';
    let thinkingContent = '';
    const toolCalls: Array<{ name: string; arguments: any }> = [];
    let tokens: { prompt: number; completion: number; total: number } | null = null;

    // Process streaming response
    for await (const chunk of grokClient.chatStream(request)) {
      console.log(`📦 [STREAM CHUNK] Event: ${chunk.event}`);

      if (chunk.event === 'thinking' && chunk.data) {
        const content = typeof chunk.data === 'string' ? chunk.data : (chunk.data.chunk || chunk.data.content || '');
        console.log(`💭 [THINKING] ${content.substring(0, 100)}...`);
        thinkingContent += content;
      } else if (chunk.event === 'content' && chunk.data) {
        const content = typeof chunk.data === 'string' ? chunk.data : (chunk.data.chunk || chunk.data.content || '');
        console.log(`💬 [CONTENT] ${content.substring(0, 100)}...`);
        agentMessageResponse += content;
      } else if (chunk.event === 'tool_call' && chunk.data) {
        const toolName = chunk.data.name || 'unknown';
        const toolArgs = chunk.data.arguments || {};
        console.log(`🔧 [TOOL CALL] ${toolName}(${JSON.stringify(toolArgs).substring(0, 100)}...)`);
        toolCalls.push({
          name: toolName,
          arguments: toolArgs
        });
      } else if (chunk.event === 'done') {
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
    logBotResponse(
      agentMessageResponse,
      channelId,
      channelName,
      senderId,
      senderName,
      undefined,
      isDM
    );

    // Log conversation turn
    logConversationTurn(
      messageContent,
      agentMessageResponse,
      conversationContext,
      [grokMessage],
      channelId,
      channelName,
      senderId,
      senderName,
      GROK_SESSION_ID
    );

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

  } catch (error) {
    clearInterval(typingInterval);

    console.error("❌ Error communicating with Grok API:", error);

    const errorMessage = error instanceof Error ? error.message : String(error);

    if (SURFACE_ERRORS) {
      if (errorMessage.includes('timeout')) {
        return "⚠️ **Timeout Error**\n> The AI service took too long to respond. Please try again!";
      } else if (errorMessage.includes('ECONNREFUSED') || errorMessage.includes('ENOTFOUND')) {
        return "⚠️ **Connection Error**\n> Cannot connect to the AI service. Please check if nate_api_substrate is running!";
      } else {
        return `⚠️ **Error**\n> ${errorMessage}\n\nPlease try again or contact support if the issue persists.`;
      }
    }

    return "";
  }
}

/**
 * Send a timer/heartbeat message to Grok
 * Returns the message to send to Discord, or empty string if no message should be sent
 */
async function sendTimerMessage(channel: any): Promise<string> {
  if (!channel) {
    console.log('⏰ No channel provided for timer message');
    return "";
  }

  console.log('🜂 Generating heartbeat...');

  try {
    // Get the last 3 conversation turns for context
    const recentTurns = await getRecentConversationTurns(3);

    // Build conversation context from recent turns
    let conversationContext = '';
    if (recentTurns.length > 0) {
      conversationContext = '\n\n## Recent Conversation Context (Last 3 Turns):\n\n';
      recentTurns.forEach((turn, index) => {
        const turnDate = new Date(turn.timestamp);
        const timeStr = turnDate.toLocaleString(LOCALE, {
          timeZone: TIMEZONE,
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false
        });
        conversationContext += `### Turn ${index + 1} (${timeStr}):\n`;
        conversationContext += `**User:** ${turn.user.substring(0, 500)}${turn.user.length > 500 ? '...' : ''}\n\n`;
        conversationContext += `**Assistant:** ${turn.assistant.substring(0, 500)}${turn.assistant.length > 500 ? '...' : ''}\n\n`;
      });
    } else {
      conversationContext = '\n\n## Recent Conversation Context:\nNo recent conversations found in today\'s logs.\n\n';
    }

    // Generate current timestamp
    const now = new Date();
    const currentTimeStr = now.toLocaleString(LOCALE, {
      timeZone: TIMEZONE,
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    });

    // Create heartbeat system message with context
    const systemMessage = `# Autonomous Heartbeat - Your Time to Act

**Current Date & Time:** ${currentTimeStr}

This is your scheduled heartbeat - a moment of autonomy where YOU decide what you want to do. You have full access to your tools and can take any action that interests you or seems valuable.

## Ideas:
- Research topics that interest you or relate to recent conversations
- Journal thoughts, insights, or reflections to archival memory
- Search past memories and conversations for connections
- Send a meaningful message to the user (only if you have real value to add)
- Or do nothing - if nothing feels valuable right now, that's fine

## Remember:
- Use your tools directly as described in your system instructions - do not narrate tool usage in your text
- Your text response is what gets displayed in Discord
- If you only want to perform background actions with nothing to say, keep your text empty
${conversationContext}`;

    // Create heartbeat request
    const request: GrokChatRequest = {
      messages: [{
        role: "system",
        content: systemMessage
      }],
      session_id: GROK_SESSION_ID,
      message_type: 'system',  // 'system' triggers autonomous mode in substrate
      max_tokens: GROK_MAX_TOKENS,  // Explicitly set max tokens
    };

    console.log(`🜂 Heartbeat config: max_tokens=${request.max_tokens}`);

    const response = await grokClient.chat(request);
    const sendMessage = response.send_message !== false; // Default true for backward compatibility
    let content = response.message?.content || '';
    const toolCalls = response.tool_calls || [];

    // TEMPORARY WORKAROUND: Strip <decision> block if substrate didn't remove it
    // This should be fixed in substrate, but adding safety check here
    const decisionBlockRegex = /<decision>[\s\S]*?<\/decision>/gi;
    if (decisionBlockRegex.test(content)) {
      console.warn('⚠️ Decision block found in message content - stripping it out (substrate should handle this)');
      content = content.replace(decisionBlockRegex, '').trim();
    }

    // Log tool usage for visibility
    if (toolCalls.length > 0) {
      console.log(`🔧 [HEARTBEAT] Used ${toolCalls.length} tool(s): ${toolCalls.map(t => t.name).join(', ')}`);
    }

    // Check if Nate wants to send a message to Discord
    if (sendMessage && content && content.trim()) {
      logHeartbeat(content, channel.id, channel.name || 'unknown');
      console.log(`💬 [HEARTBEAT → USER] ${content.substring(0, 100)}...`);
      return content;
    } else if (!sendMessage) {
      console.log(`🔕 [HEARTBEAT → BACKGROUND] Autonomous actions completed, no message to user`);
      // Log the background activity for debugging
      if (content && content.trim()) {
        logHeartbeat(`[BACKGROUND] ${content}`, channel.id, channel.name || 'unknown');
      }
      return "";
    } else {
      console.log(`💤 [HEARTBEAT → NONE] No action taken`);
      return "";
    }

  } catch (error) {
    console.error("❌ Error generating heartbeat:", error);
    return "";
  }
}

/**
 * Send a scheduled task message to Grok
 */
async function sendTaskMessage(taskName: string, taskPrompt: string): Promise<string> {
  console.log(`📅 Executing scheduled task: ${taskName}`);

  try {
    // Log task execution
    logTask(taskName, 'scheduled_task', taskPrompt, undefined, undefined);

    // Create task request
    const request: GrokChatRequest = {
      messages: [{
        role: "user",
        content: `[SCHEDULED TASK: ${taskName}] ${taskPrompt}`
      }],
      session_id: GROK_SESSION_ID,
      message_type: 'task',
      max_tokens: GROK_MAX_TOKENS,  // Explicitly set max tokens
    };

    const response = await grokClient.chat(request);
    const taskResponse = response.message?.content || '';

    if (taskResponse && taskResponse.trim()) {
      // Log task response
      logTaskResponse(taskResponse, taskName, undefined, undefined);

      console.log(`📅 Task completed: ${taskName}`);
      return taskResponse;
    }

    console.warn(`⚠️ Empty task response for: ${taskName}`);
    return "";

  } catch (error) {
    console.error(`❌ Error executing task "${taskName}":`, error);
    return "";
  }
}

export { sendMessage, sendTimerMessage, sendTaskMessage };
