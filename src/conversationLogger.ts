/**
 * CONVERSATION LOGGER - Persistent JSONL Logging for Training Data
 * ================================================================
 * 
 * Logs ALL conversations (User → Bot, Bot → User, Tasks, Heartbeats)
 * in JSONL format (one JSON object per line) for training purposes.
 * 
 * Features:
 * - JSONL format (perfect for training data)
 * - Automatic daily file rotation
 * - Batched writes (performance optimized)
 * - Structured data with metadata
 * - Thread-safe (async queue)
 * 
 * Format:
 * Each line is a JSON object:
 * {
 *   "timestamp": "2025-01-15T10:30:45.123Z",
 *   "type": "user_message" | "bot_response" | "heartbeat" | "task",
 *   "direction": "in" | "out",
 *   "channel_id": "123456789",
 *   "channel_name": "#general",
 *   "user_id": "987654321",
 *   "username": "user",
 *   "content": "Message content here...",
 *   "metadata": { ... }
 * }
 */

import * as fs from 'fs';
import * as path from 'path';

// ===== CONFIGURATION =====
const ENABLE_CONVERSATION_LOGGING = process.env.ENABLE_CONVERSATION_LOGGING === 'true';
const LOGS_DIR = process.env.CONVERSATION_LOGS_DIR 
  ? path.resolve(process.env.CONVERSATION_LOGS_DIR)
  : path.join(__dirname, '..', 'logs', 'conversations');
const FLUSH_INTERVAL_MS = 5000; // Flush every 5 seconds
const MAX_BUFFER_SIZE = 50; // Max entries before forced flush

// ===== TYPES =====
export type ConversationType = 
  | 'user_message' 
  | 'bot_response' 
  | 'heartbeat' 
  | 'task'
  | 'task_response'
  | 'reasoning'
  | 'tool_call'
  | 'tool_return'
  | 'letta_input'  // Full input sent to Letta API (for fine-tuning)
  | 'conversation_turn';  // Complete turn: input + output (for fine-tuning)

export type ConversationDirection = 'in' | 'out';

export interface ConversationLogEntry {
  timestamp: string;
  type: ConversationType;
  direction: ConversationDirection;
  channel_id?: string;
  channel_name?: string;
  user_id?: string;
  username?: string;
  content: string;
  message_id?: string;
  metadata?: {
    [key: string]: any;
    task_name?: string;
    task_action_type?: string;
    is_dm?: boolean;
    attachments?: number;
    reasoning?: string;
    tool_calls?: string[];
    // Fine-tuning specific
    conversation_context?: string;  // Full conversation context sent to Letta
    full_input?: string;  // Complete input sent to Letta API
    full_output?: string;  // Complete output from Letta
    message_history?: any[];  // Full message history (what Letta sees)
    agent_id?: string;  // Letta agent ID
    system_prompt?: string;  // System prompt/instructions (if available)
    tool_calls_data?: Array<{ tool_name: string; arguments: any }>;  // Tool calls in conversation turn
    tool_returns_data?: Array<{ tool_name: string; return_value: any }>;  // Tool returns in conversation turn
    reasoning_chain?: Array<{ step: number; reasoning: string; timestamp: string }>;  // Reasoning chain for reasoning models (Gemma 3, etc.)
    reasoning_before_tools?: string;  // Reasoning before any tool calls
    reasoning_after_tools?: string;  // Reasoning after tool returns
  };
}

// ===== STATE =====
let logBuffer: ConversationLogEntry[] = [];
let currentDate = getCurrentDate();
let writeQueue: Promise<void> = Promise.resolve();
let flushInterval: NodeJS.Timeout | null = null;

/**
 * Get current date in YYYY-MM-DD format (configured timezone)
 */
function getCurrentDate(): string {
  const TIMEZONE = process.env.TIMEZONE || 'Europe/Berlin';
  const now = new Date();
  // Get date in configured timezone
  const tzTime = new Date(now.toLocaleString('en-US', { timeZone: TIMEZONE }));
  return tzTime.toISOString().split('T')[0];
}

/**
 * Get current log file path
 */
function getLogFilePath(date?: string): string {
  const logDate = date || getCurrentDate();
  const filename = `conversations-${logDate}.jsonl`;
  return path.join(LOGS_DIR, filename);
}

/**
 * Ensure logs directory exists
 */
function ensureLogsDir(): void {
  if (!fs.existsSync(LOGS_DIR)) {
    fs.mkdirSync(LOGS_DIR, { recursive: true });
  }
}

/**
 * Write entry to file (async, queued)
 */
async function writeEntry(entry: ConversationLogEntry): Promise<void> {
  // Skip logging if disabled
  if (!ENABLE_CONVERSATION_LOGGING) {
    return;
  }
  // Check if date changed (new day)
  const today = getCurrentDate();
  if (today !== currentDate) {
    // Flush old buffer before switching dates
    await flushBuffer();
    currentDate = today;
  }

  // Add to buffer
  logBuffer.push(entry);

  // Force flush if buffer is full
  if (logBuffer.length >= MAX_BUFFER_SIZE) {
    await flushBuffer();
  }
}

/**
 * Flush buffer to disk
 */
async function flushBuffer(): Promise<void> {
  if (logBuffer.length === 0) {
    return;
  }

  // Wait for previous write to complete
  await writeQueue;

  // Create new write promise
  writeQueue = (async () => {
    try {
      ensureLogsDir();
      const filepath = getLogFilePath();
      
      // Append entries as JSONL (one JSON object per line)
      const lines = logBuffer.map(entry => JSON.stringify(entry)).join('\n') + '\n';
      
      await fs.promises.appendFile(filepath, lines, 'utf8');
      
      const count = logBuffer.length;
      logBuffer = []; // Clear buffer
      
      if (count > 0) {
        console.log(`📝 [Conversation Logger] Flushed ${count} entries to ${filepath}`);
      }
    } catch (error) {
      console.error('❌ [Conversation Logger] Failed to write entries:', error);
      // Don't clear buffer on error - will retry on next flush
    }
  })();

  await writeQueue;
}

/**
 * Start auto-flush interval
 */
function startAutoFlush(): void {
  if (flushInterval) {
    return; // Already started
  }

  flushInterval = setInterval(() => {
    flushBuffer().catch(err => {
      console.error('❌ [Conversation Logger] Auto-flush error:', err);
    });
  }, FLUSH_INTERVAL_MS);

  console.log(`📝 [Conversation Logger] Auto-flush enabled (every ${FLUSH_INTERVAL_MS / 1000}s)`);
}

/**
 * Stop auto-flush (for graceful shutdown)
 */
export async function stopAutoFlush(): Promise<void> {
  if (flushInterval) {
    clearInterval(flushInterval);
    flushInterval = null;
  }
  
  // Final flush
  await flushBuffer();
}

// ===== PUBLIC API =====

/**
 * Log a user message (incoming)
 */
export function logUserMessage(
  content: string,
  channelId: string,
  channelName: string | undefined,
  userId: string,
  username: string,
  messageId?: string,
  isDM: boolean = false,
  attachments?: number
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'user_message',
    direction: 'in',
    channel_id: channelId,
    channel_name: channelName,
    user_id: userId,
    username: username,
    content: content,
    message_id: messageId,
    metadata: {
      is_dm: isDM,
      attachments: attachments || 0
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log user message:', err);
  });
}

/**
 * Log a bot response (outgoing)
 */
export function logBotResponse(
  content: string,
  channelId: string | undefined,
  channelName: string | undefined,
  userId?: string,
  username?: string,
  messageId?: string,
  isDM: boolean = false
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'bot_response',
    direction: 'out',
    channel_id: channelId,
    channel_name: channelName,
    user_id: userId,
    username: username,
    content: content,
    message_id: messageId,
    metadata: {
      is_dm: isDM
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log bot response:', err);
  });
}

/**
 * Log a heartbeat (system-initiated)
 */
export function logHeartbeat(
  content: string,
  channelId: string | undefined,
  channelName: string | undefined
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'heartbeat',
    direction: 'out',
    channel_id: channelId,
    channel_name: channelName,
    content: content,
    metadata: {}
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log heartbeat:', err);
  });
}

/**
 * Log a task execution
 */
export function logTask(
  taskName: string,
  actionType: string,
  content: string,
  channelId: string | undefined,
  channelName: string | undefined
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'task',
    direction: 'in',
    channel_id: channelId,
    channel_name: channelName,
    content: content,
    metadata: {
      task_name: taskName,
      task_action_type: actionType
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log task:', err);
  });
}

/**
 * Log a task response
 */
export function logTaskResponse(
  content: string,
  taskName: string,
  channelId: string | undefined,
  channelName: string | undefined
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'task_response',
    direction: 'out',
    channel_id: channelId,
    channel_name: channelName,
    content: content,
    metadata: {
      task_name: taskName
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log task response:', err);
  });
}

/**
 * Log reasoning (if enabled)
 */
export function logReasoning(
  reasoning: string,
  channelId: string | undefined
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'reasoning',
    direction: 'out',
    channel_id: channelId,
    content: reasoning,
    metadata: {}
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log reasoning:', err);
  });
}

/**
 * Log a tool call (when bot uses a tool)
 */
export function logToolCall(
  toolName: string,
  toolArguments: any,
  channelId: string | undefined,
  channelName: string | undefined
): void {
  // Serialize arguments for logging
  const argsString = typeof toolArguments === 'string' 
    ? toolArguments 
    : JSON.stringify(toolArguments);
  
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'tool_call',
    direction: 'out',
    channel_id: channelId,
    channel_name: channelName,
    content: `Tool: ${toolName}`,
    metadata: {
      tool_name: toolName,
      tool_arguments: argsString,
      tool_arguments_parsed: typeof toolArguments === 'object' ? toolArguments : undefined
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log tool call:', err);
  });
}

/**
 * Log a tool return (when tool execution completes)
 */
export function logToolReturn(
  toolName: string,
  returnValue: any,
  channelId: string | undefined,
  channelName: string | undefined
): void {
  // Serialize return value for logging
  let returnValueString: string;
  let returnValueParsed: any = undefined;
  
  // 🔧 FIX: Handle undefined/null returnValue
  if (returnValue === undefined || returnValue === null) {
    returnValueString = 'null';
    returnValueParsed = null;
  } else if (typeof returnValue === 'string') {
    returnValueString = returnValue;
    returnValueParsed = returnValue;
  } else {
    try {
      returnValueString = JSON.stringify(returnValue);
      returnValueParsed = returnValue;
    } catch (err) {
      // Fallback if JSON.stringify fails
      returnValueString = String(returnValue);
      returnValueParsed = returnValue;
    }
  }
  
  // 🔧 FIX: Ensure returnValueString is always a string (never undefined)
  if (!returnValueString || typeof returnValueString !== 'string') {
    returnValueString = 'null';
  }
  
  // Truncate very long return values (keep first 2000 chars for content field)
  const contentPreview = returnValueString.length > 2000 
    ? returnValueString.substring(0, 2000) + '... (truncated)'
    : returnValueString;
  
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'tool_return',
    direction: 'in',
    channel_id: channelId,
    channel_name: channelName,
    content: `Tool Return: ${toolName}`,
    metadata: {
      tool_name: toolName,
      return_value: returnValueString,
      return_value_parsed: returnValueParsed,
      return_value_length: returnValueString.length
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log tool return:', err);
  });
}

/**
 * Log full input sent to Letta API (for fine-tuning)
 * This logs what Letta actually sees, including conversation context
 */
export function logLettaInput(
  fullInput: string,
  conversationContext: string | null,
  messageHistory: any[],
  channelId: string | undefined,
  channelName: string | undefined,
  userId: string | undefined,
  username: string | undefined,
  agentId: string | undefined
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'letta_input',
    direction: 'in',
    channel_id: channelId,
    channel_name: channelName,
    user_id: userId,
    username: username,
    content: fullInput,
    metadata: {
      conversation_context: conversationContext || undefined,
      full_input: fullInput,
      message_history: messageHistory,
      agent_id: agentId
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log Letta input:', err);
  });
}

/**
 * Log complete conversation turn (input + output) for fine-tuning
 * This is the most useful format for training: one turn = one training example
 */
export function logConversationTurn(
  input: string,
  output: string,
  conversationContext: string | null,
  messageHistory: any[],
  channelId: string | undefined,
  channelName: string | undefined,
  userId: string | undefined,
  username: string | undefined,
  agentId: string | undefined,
  toolCalls?: Array<{ tool_name: string; arguments: any }>,
  toolReturns?: Array<{ tool_name: string; return_value: any }>,
  reasoningChain?: Array<{ step: number; reasoning: string; timestamp: string }>
): void {
  const entry: ConversationLogEntry = {
    timestamp: new Date().toISOString(),
    type: 'conversation_turn',
    direction: 'in',
    channel_id: channelId,
    channel_name: channelName,
    user_id: userId,
    username: username,
    content: `Input: ${input.substring(0, 200)}... | Output: ${output.substring(0, 200)}...`,
    metadata: {
      conversation_context: conversationContext || undefined,
      full_input: input,
      full_output: output,
      message_history: messageHistory,
      agent_id: agentId,
      tool_calls_data: toolCalls,  // Renamed to avoid conflict with existing tool_calls type
      tool_returns_data: toolReturns,  // Renamed for consistency
      reasoning_chain: reasoningChain,  // Full reasoning chain for reasoning models (Gemma 3, etc.)
      // Extract first reasoning (before tools) and last reasoning (after tools) for convenience
      reasoning_before_tools: reasoningChain && reasoningChain.length > 0 && toolCalls && toolCalls.length > 0
        ? reasoningChain[0]?.reasoning  // First reasoning step (before any tool calls)
        : undefined,
      reasoning_after_tools: reasoningChain && reasoningChain.length > 1 && toolReturns && toolReturns.length > 0
        ? reasoningChain[reasoningChain.length - 1]?.reasoning  // Last reasoning step (after tool returns)
        : reasoningChain && reasoningChain.length === 1 && toolCalls && toolCalls.length === 0
        ? reasoningChain[0]?.reasoning  // Single reasoning if no tools
        : undefined
    }
  };

  writeEntry(entry).catch(err => {
    console.error('❌ [Conversation Logger] Failed to log conversation turn:', err);
  });
}

/**
 * Initialize logger (call on bot startup)
 */
export function initializeLogger(): void {
  if (!ENABLE_CONVERSATION_LOGGING) {
    console.log('📝 [Conversation Logger] Disabled (set ENABLE_CONVERSATION_LOGGING=true to enable)');
    return;
  }
  
  ensureLogsDir();
  startAutoFlush();
  console.log(`📝 [Conversation Logger] Initialized - logs will be saved to: ${LOGS_DIR}`);
}

/**
 * Force flush (for graceful shutdown)
 */
export async function forceFlush(): Promise<void> {
  await flushBuffer();
  console.log('📝 [Conversation Logger] Force flushed');
}

