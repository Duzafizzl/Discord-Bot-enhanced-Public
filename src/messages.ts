import { LettaClient } from "@letta-ai/letta-client";
import { LettaStreamingResponse } from "@letta-ai/letta-client/api/resources/agents/resources/messages/types/LettaStreamingResponse";
import { Stream } from "@letta-ai/letta-client/core";
import { Message, OmitPartialGroupDMChannel } from "discord.js";
import https from "https";
import { processFileAttachment } from "./fileChunking";
import { logUserMessage, logBotResponse, logHeartbeat, logTask, logTaskResponse, logReasoning, logToolCall, logToolReturn, logLettaInput, logConversationTurn } from "./conversationLogger";
import { apiQueue } from "./apiQueue";

// If the token is not set, just use a dummy value
// ⏱️ TIMEOUT CONFIGURATION: Erhöht für große Requests (240k+ Tokens) und lange Tool Calls (z.B. ElevenLabs)
// Default: 300s (5 Minuten) - kann via LETTA_API_TIMEOUT_MS überschrieben werden
// Warum höher: 
// - Große Requests brauchen länger, Timeout = bereits abgerechnet → kein Retry mehr!
// - ElevenLabs Tool Calls können bis zu 5 Minuten dauern (Audio-Generierung)
const LETTA_API_TIMEOUT_MS = parseInt(process.env.LETTA_API_TIMEOUT_MS || '300000', 10); // Default: 300s (5 Minuten)

const client = new LettaClient({
  token: process.env.LETTA_API_KEY || 'your_letta_api_key',
  baseUrl: process.env.LETTA_BASE_URL || 'https://api.letta.com',
  timeout: LETTA_API_TIMEOUT_MS, // 🔧 Configurable timeout - Default: 300s (5 Minuten) für große Requests und lange Tool Calls
} as any);
const AGENT_ID = process.env.LETTA_AGENT_ID;
const USE_SENDER_PREFIX = process.env.LETTA_USE_SENDER_PREFIX === 'true';
const SURFACE_ERRORS = process.env.SURFACE_ERRORS === 'true';

// 🔒 DM RESTRICTION: Only allow DMs to/from specific user if configured
const ALLOWED_DM_USER_ID = process.env.ALLOWED_DM_USER_ID || '';

// 💰 RETRY CONFIGURATION (Credit optimization)
// Set ENABLE_API_RETRY=false to disable retries completely (saves credits!)
// Set MAX_API_RETRIES to control how many retries (default: 1)
const ENABLE_API_RETRY = process.env.ENABLE_API_RETRY !== 'false'; // Default: enabled
const MAX_API_RETRIES = parseInt(process.env.MAX_API_RETRIES || '1', 10); // Default: 1 retry (saves credits!)

// 🔥 REMOVED: Task Execution Channel special handling (Nov 19, 2025)
// All messages now go to their specified target channel - no automatic redirects
// Heartbeats are handled separately through autonomous.ts

/**
 * Format tool calls for Discord display - compact and informative
 */
function formatToolCall(toolName: string, args: any): string {
  const prefix = '🔧 Tool Call:';
  
  // discord_tool - unified Discord tool
  if (toolName === 'discord_tool') {
    const action = args.action || 'unknown';
    
    // Batch operations
    if (action === 'execute_batch' && args.operations) {
      const ops = args.operations as any[];
      const opNames = ops.map((op: any) => op.action || 'unknown').slice(0, 3);
      const more = ops.length > 3 ? ` +${ops.length - 3} more` : '';
      return `${prefix} discord_tool - Batch: ${ops.length} ops (${opNames.join(', ')}${more})`;
    }
    
    if (action === 'manage_tasks') {
      const createCount = args.create_tasks?.length || 0;
      const deleteCount = args.delete_task_ids?.length || 0;
      const listTasks = args.list_tasks ? 'list' : '';
      const parts = [];
      if (createCount > 0) parts.push(`${createCount} create`);
      if (deleteCount > 0) parts.push(`${deleteCount} delete`);
      if (listTasks) parts.push('list');
      return `${prefix} discord_tool - Manage Tasks: ${parts.join(', ')}`;
    }
    
    // send_message
    if (action === 'send_message') {
      const targetType = args.target_type || 'channel';
      const target = args.target ? ` → ${targetType}` : '';
      const msgLen = args.message?.length || 0;
      const msgPreview = msgLen > 0 ? ` (${msgLen} chars)` : '';
      return `${prefix} discord_tool - send_message${target}${msgPreview}`;
    }
    
    // read_messages
    if (action === 'read_messages') {
      const limit = args.limit || 50;
      const timeFilter = args.time_filter || args.start_time ? ' (filtered)' : '';
      const keywords = args.search_keywords ? ` + search` : '';
      return `${prefix} discord_tool - read_messages (limit: ${limit})${timeFilter}${keywords}`;
    }
    
    // create_task
    if (action === 'create_task') {
      const taskName = args.task_name || 'unnamed';
      const schedule = args.schedule || 'unscheduled';
      return `${prefix} discord_tool - create_task: "${taskName}" (${schedule})`;
    }
    
    // delete_task
    if (action === 'delete_task') {
      return `${prefix} discord_tool - delete_task`;
    }
    
    // list_tasks
    if (action === 'list_tasks') {
      return `${prefix} discord_tool - list_tasks`;
    }
    
    // list_guilds
    if (action === 'list_guilds') {
      const withChannels = args.include_channels ? ' + channels' : '';
      return `${prefix} discord_tool - list_guilds${withChannels}`;
    }
    
    // list_channels
    if (action === 'list_channels') {
      return `${prefix} discord_tool - list_channels`;
    }
    
    // Fallback for other actions
    return `${prefix} discord_tool - ${action}`;
  }
  
  // archival_memory_insert
  if (toolName === 'archival_memory_insert') {
    const content = args.content || '';
    const charCount = content.length;
    const preview = content.length > 60 
      ? content.substring(0, 60).replace(/\n/g, ' ') + '...'
      : content.replace(/\n/g, ' ');
    const tags = args.tags?.length > 0 ? ` [${args.tags.join(', ')}]` : '';
    return `${prefix} archival_memory_insert - Inserting ${charCount} chars: "${preview}"${tags}`;
  }
  
  // archival_memory_search
  if (toolName === 'archival_memory_search') {
    const query = args.query || '';
    const limit = args.limit || 10;
    const preview = query.length > 50 ? query.substring(0, 50) + '...' : query;
    return `${prefix} archival_memory_search - "${preview}" (limit: ${limit})`;
  }
  
  // memory_insert
  if (toolName === 'memory_insert') {
    const label = args.label || 'unknown';
    const newStr = args.new_str || '';
    const charCount = newStr.length;
    const line = args.insert_line !== undefined ? ` at line ${args.insert_line}` : '';
    return `${prefix} memory_insert - "${label}"${line} (${charCount} chars)`;
  }
  
  // memory_replace
  if (toolName === 'memory_replace') {
    const label = args.label || 'unknown';
    const oldLen = (args.old_str || '').length;
    const newLen = (args.new_str || '').length;
    return `${prefix} memory_replace - "${label}" (${oldLen} → ${newLen} chars)`;
  }
  
  // core_memory_replace
  if (toolName === 'core_memory_replace') {
    const field = args.field || 'unknown';
    const oldLen = (args.old || '').length;
    const newLen = (args.new || '').length;
    return `${prefix} core_memory_replace - ${field} (${oldLen} → ${newLen} chars)`;
  }
  
  // core_memory_append
  if (toolName === 'core_memory_append') {
    const field = args.field || 'unknown';
    const appendLen = (args.append || '').length;
    return `${prefix} core_memory_append - ${field} (+${appendLen} chars)`;
  }
  
  // send_message (standard tool, not discord_tool)
  if (toolName === 'send_message') {
    const content = args.content || '';
    const charCount = content.length;
    return `${prefix} send_message (${charCount} chars)`;
  }
  
  // send_voice_message
  if (toolName === 'send_voice_message') {
    const text = args.text || '';
    const charCount = text.length;
    const targetType = args.target_type || '';
    const target = targetType ? ` → ${targetType}` : (args.target ? ' → target' : '');
    const preview = text.length > 50 
      ? text.substring(0, 50).replace(/\n/g, ' ') + '...'
      : text.replace(/\n/g, ' ');
    return `${prefix} send_voice_message${target} (${charCount} chars): "${preview}"`;
  }
  
  // create_scheduled_task
  if (toolName === 'create_scheduled_task') {
    const taskName = args.task_name || 'unnamed';
    const schedule = args.schedule || 'unscheduled';
    return `${prefix} create_scheduled_task - "${taskName}" (${schedule})`;
  }
  
  // download_discord_file
  if (toolName === 'download_discord_file') {
    const url = args.url || '';
    const urlPreview = url.length > 40 ? url.substring(0, 40) + '...' : url;
    return `${prefix} download_discord_file - ${urlPreview}`;
  }
  
  // web_search
  if (toolName === 'web_search') {
    const query = args.query || '';
    const preview = query.length > 50 ? query.substring(0, 50) + '...' : query;
    return `${prefix} web_search - "${preview}"`;
  }
  
  // run_code
  if (toolName === 'run_code') {
    const language = args.language || 'code';
    const code = args.code || '';
    const lines = code.split('\n').length;
    return `${prefix} run_code - ${language} (${lines} lines)`;
  }
  
  // grep_files
  if (toolName === 'grep_files') {
    const pattern = args.pattern || '';
    const files = args.files?.length || 0;
    const preview = pattern.length > 40 ? pattern.substring(0, 40) + '...' : pattern;
    return `${prefix} grep_files - "${preview}" (${files} files)`;
  }
  
  // semantic_search_files
  if (toolName === 'semantic_search_files') {
    const query = args.query || '';
    const preview = query.length > 50 ? query.substring(0, 50) + '...' : query;
    return `${prefix} semantic_search_files - "${preview}"`;
  }
  
  // open_files
  if (toolName === 'open_files') {
    const files = args.files || [];
    const fileCount = Array.isArray(files) ? files.length : 1;
    const firstFile = Array.isArray(files) && files[0] ? files[0] : 'file';
    const preview = firstFile.length > 30 ? firstFile.substring(0, 30) + '...' : firstFile;
    return `${prefix} open_files - ${fileCount} file${fileCount > 1 ? 's' : ''} (${preview}${fileCount > 1 ? '...' : ''})`;
  }
  
  // search_memory
  if (toolName === 'search_memory') {
    const query = args.query || '';
    const preview = query.length > 50 ? query.substring(0, 50) + '...' : query;
    return `${prefix} search_memory - "${preview}"`;
  }
  
  // conversation_search
  if (toolName === 'conversation_search') {
    const query = args.query || '';
    const preview = query.length > 50 ? query.substring(0, 50) + '...' : query;
    return `${prefix} conversation_search - "${preview}"`;
  }
  
  // spotify_control
  if (toolName === 'spotify_control') {
    const action = args.action || 'unknown';
    
    // execute_batch
    if (action === 'execute_batch' && args.operations) {
      const ops = args.operations as any[];
      const opActions = ops.map((op: any) => op.action || 'unknown').slice(0, 3);
      const more = ops.length > 3 ? ` +${ops.length - 3} more` : '';
      return `${prefix} spotify_control - Batch: ${ops.length} ops (${opActions.join(', ')}${more})`;
    }
    
    // play
    if (action === 'play') {
      const query = args.query || '';
      const spotifyId = args.spotify_id || '';
      const contentType = args.content_type || 'track';
      const preview = query || spotifyId.substring(0, 20) || 'unknown';
      return `${prefix} spotify_control - play: ${preview} (${contentType})`;
    }
    
    // search
    if (action === 'search') {
      const query = args.query || '';
      const contentType = args.content_type || 'track';
      const limit = args.limit || 10;
      const preview = query.length > 40 ? query.substring(0, 40) + '...' : query;
      return `${prefix} spotify_control - search: "${preview}" (${contentType}, limit: ${limit})`;
    }
    
    // create_playlist
    if (action === 'create_playlist') {
      const playlistName = args.playlist_name || 'unnamed';
      const songs = args.songs || '';
      const songCount = songs ? songs.split(';').length : 0;
      const withSongs = songCount > 0 ? ` with ${songCount} song${songCount > 1 ? 's' : ''}` : '';
      return `${prefix} spotify_control - create_playlist: "${playlistName}"${withSongs}`;
    }
    
    // add_to_playlist
    if (action === 'add_to_playlist') {
      const trackIds = args.track_ids || '';
      const query = args.query || '';
      const trackCount = trackIds ? trackIds.split(',').length : (query ? query.split(';').length : 1);
      return `${prefix} spotify_control - add_to_playlist (${trackCount} track${trackCount > 1 ? 's' : ''})`;
    }
    
    // add_to_queue
    if (action === 'add_to_queue') {
      const query = args.query || '';
      const trackCount = query ? query.split(';').length : 1;
      return `${prefix} spotify_control - add_to_queue (${trackCount} track${trackCount > 1 ? 's' : ''})`;
    }
    
    // now_playing
    if (action === 'now_playing') {
      return `${prefix} spotify_control - now_playing`;
    }
    
    // my_playlists
    if (action === 'my_playlists') {
      const limit = args.limit || 20;
      return `${prefix} spotify_control - my_playlists (limit: ${limit})`;
    }
    
    // Simple actions (pause, next, previous)
    if (['pause', 'next', 'previous'].includes(action)) {
      return `${prefix} spotify_control - ${action}`;
    }
    
    // Fallback for other actions
    return `${prefix} spotify_control - ${action}`;
  }
  
  // Default fallback for unknown tools
  const argKeys = Object.keys(args);
  if (argKeys.length > 0) {
    const preview = argKeys.slice(0, 3).join(', ');
    const more = argKeys.length > 3 ? ` +${argKeys.length - 3} more` : '';
    return `${prefix} ${toolName} (${preview}${more})`;
  }
  
  return `${prefix} ${toolName}`;
}

enum MessageType {
  DM = "DM",
  MENTION = "MENTION",
  REPLY = "REPLY",
  GENERIC = "GENERIC"
}

// ===== CHUNKING UTILITY (for long messages from send_message tool) =====
function chunkText(text: string, limit: number): string[] {
  const chunks: string[] = [];
  let i = 0;
  
  while (i < text.length) {
    let end = Math.min(i + limit, text.length);
    let slice = text.slice(i, end);
    
    // Try to break at newline for better readability
    if (end < text.length) {
      const lastNewline = slice.lastIndexOf('\n');
      if (lastNewline > limit * 0.6) { // Only if newline is reasonably close to end
        end = i + lastNewline + 1;
        slice = text.slice(i, end);
      }
    }
    
    chunks.push(slice);
    i = end;
  }
  
  return chunks;
}

// Weather API helper for configured city
export async function getMunichWeather(): Promise<string | null> {
  const apiKey = process.env.OPENWEATHER_API_KEY;
  const city = process.env.WEATHER_CITY || 'Munich';
  const countryCode = process.env.WEATHER_COUNTRY_CODE || 'de';
  const language = process.env.WEATHER_LANGUAGE || 'de';

  if (!apiKey) {
    console.log('ℹ️ Weather API not configured (OPENWEATHER_API_KEY missing)');
    return null; // Weather API not configured
  }

  try {
    const weatherData = await new Promise<any>((resolve, reject) => {
      const req = https.request({
        hostname: 'api.openweathermap.org',
        path: `/data/2.5/weather?q=${city},${countryCode}&appid=${apiKey}&units=metric&lang=${language}`,
        method: 'GET',
      }, (res) => {
        if (res.statusCode !== 200) {
          console.error(`❌ Weather API error: Status ${res.statusCode}`);
          let errorBody = '';
          res.on('data', chunk => errorBody += chunk);
          res.on('end', () => {
            console.error('Weather API error response:', errorBody);
            reject(new Error(`Weather API returned ${res.statusCode}`));
          });
          return;
        }
        
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            const json = JSON.parse(body);
            resolve(json);
          } catch (err) {
            console.error('❌ Weather API parse error:', err);
            console.error('Response body:', body);
            reject(err);
          }
        });
      });
      
      req.on('error', (err) => {
        console.error('❌ Weather API request error:', err);
        reject(err);
      });
      
      req.end();
    });

    if (!weatherData || !weatherData.main) {
      console.log('ℹ️ Weather API returned invalid data structure');
      return null;
    }

    const temp = Math.round(weatherData.main.temp);
    const feelsLike = Math.round(weatherData.main.feels_like);
    const description = weatherData.weather?.[0]?.description || 'Unbekannt';
    
    // Capitalize first letter of description
    const descriptionFormatted = description.charAt(0).toUpperCase() + description.slice(1);

    const cityDisplay = process.env.WEATHER_CITY_DISPLAY || city;
    const language = process.env.BOT_LANGUAGE || 'en';
    const feelsLikeText = language === 'de' ? 'gefühlt' : 'feels like';
    const weatherString = `🌡️ ${cityDisplay}: ${temp}°C (${feelsLikeText} ${feelsLike}°C)\n☁️ ${descriptionFormatted}`;
    console.log(`✅ Weather: ${weatherString}`);
    return weatherString;
  } catch (err) {
    console.error('❌ Weather API error:', err instanceof Error ? err.message : err);
    return null;
  }
}

// Spotify API helper
export async function getSpotifyNowPlaying(): Promise<string | null> {
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  const refreshToken = process.env.SPOTIFY_REFRESH_TOKEN;

  if (!clientId || !clientSecret || !refreshToken) {
    return null; // Spotify not configured
  }

  try {
    // Get access token
    const tokenData = await new Promise<string>((resolve, reject) => {
      const data = `grant_type=refresh_token&refresh_token=${refreshToken}`;
      const auth = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
      
      const req = https.request({
        hostname: 'accounts.spotify.com',
        path: '/api/token',
        method: 'POST',
        headers: {
          'Authorization': `Basic ${auth}`,
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': data.length
        }
      }, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            const json = JSON.parse(body);
            if (!json.access_token) {
              console.error('❌ Spotify token refresh failed - no access_token in response:', body.substring(0, 200));
              reject(new Error('No access_token in Spotify response'));
              return;
            }
            console.log('✅ Spotify access token refreshed successfully');
            resolve(json.access_token);
          } catch (err) {
            console.error('❌ Spotify token refresh parse error:', err);
            reject(err);
          }
        });
      });
      
      req.on('error', (err) => {
        console.error('❌ Spotify token refresh request error:', err);
        reject(err);
      });
      req.write(data);
      req.end();
    });

    // Get now playing
    const nowPlaying = await new Promise<any>((resolve, reject) => {
      const req = https.request({
        hostname: 'api.spotify.com',
        path: '/v1/me/player/currently-playing',
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${tokenData}`
        }
      }, (res) => {
        if (res.statusCode === 204) {
          console.log('ℹ️ Spotify API returned 204 - Nothing currently playing');
          resolve(null); // Nothing playing
          return;
        }
        
        if (res.statusCode !== 200) {
          console.error(`❌ Spotify API error: Status ${res.statusCode}`);
          let errorBody = '';
          res.on('data', chunk => errorBody += chunk);
          res.on('end', () => {
            console.error('Spotify API error response:', errorBody);
            resolve(null); // Return null instead of rejecting to avoid breaking heartbeat
          });
          return;
        }
        
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            const json = JSON.parse(body);
            if (json && json.item) {
              console.log(`✅ Spotify: Now playing "${json.item.name}" by ${json.item.artists.map((a: any) => a.name).join(', ')}`);
            } else {
              console.log(`ℹ️ Spotify API returned 200 but no item in response. Body: ${body.substring(0, 200)}`);
            }
            resolve(json);
          } catch (err) {
            console.error('❌ Spotify API parse error:', err);
            console.error('Response body:', body);
            reject(err);
          }
        });
      });
      
      req.on('error', (err) => {
        console.error('❌ Spotify API request error:', err);
        reject(err);
      });
      
      req.end();
    });

    if (!nowPlaying || !nowPlaying.item) {
      return null; // Nothing playing
    }

    const track = nowPlaying.item;
    const artists = track.artists.map((a: any) => a.name).join(', ');
    const progress = Math.floor(nowPlaying.progress_ms / 1000);
    const duration = Math.floor(track.duration_ms / 1000);
    const progressMin = Math.floor(progress / 60);
    const progressSec = progress % 60;
    const durationMin = Math.floor(duration / 60);
    const durationSec = duration % 60;

    return `🎵 ${track.name}\n🎤 ${artists}\n⏱️ ${progressMin}:${progressSec.toString().padStart(2, '0')} / ${durationMin}:${durationSec.toString().padStart(2, '0')}`;
  } catch (err) {
    console.error('Spotify API error:', err);
    return null;
  }
}

// ============================================
// 🔄 RETRY LOGIC FOR LETTA API (Oct 2025)
// ============================================
// Handles temporary API failures (502, 503, 504) with exponential backoff
// Prevents message loss from transient network/server issues
// Security: Max 3 retries to prevent API bombing

interface RetryableError {
  statusCode?: number;
  code?: string;
  message?: string;
}

function isRetryableError(error: unknown): boolean {
  if (!error) return false;
  
  const err = error as RetryableError;
  
  // HTTP status codes that are retryable (temporary server issues)
  const retryableStatusCodes = [502, 503, 504];
  if (err.statusCode && retryableStatusCodes.includes(err.statusCode)) {
    return true;
  }
  
  // ❌ CRITICAL: Timeout-Check ZUERST (bevor andere Checks)
  // Warum: Wenn ein Request timeout hat, wurde er bereits von Letta verarbeitet und ABGERECHNET.
  // Ein Retry würde zu Doppelabrechnung führen!
  const errorStr = String(error).toLowerCase();
  if (errorStr.includes('timeout')) {
    return false; // 🔧 FIX: Timeouts NICHT retry (bereits abgerechnet!)
  }
  
  // ❌ ETIMEDOUT ist auch ein Timeout - NICHT retry!
  if (err.code === 'ETIMEDOUT') {
    return false; // 🔧 FIX: ETIMEDOUT = Timeout = bereits abgerechnet!
  }
  
  // ✅ NUR Server-Fehler (502/503/504) sind retryable
  // Diese Fehler bedeuten: Server war down, Request wurde NICHT verarbeitet = NICHT abgerechnet
  // Alle anderen Fehler könnten bereits abgerechnet sein!
  
  // Network errors VOR dem Request (Connection konnte nicht aufgebaut werden)
  // ⚠️ WICHTIG: Nur DNS-Fehler sind sicher "vor Request"
  // ECONNRESET/UND_ERR_SOCKET können auch NACH Request passieren → nicht retry!
  const retryableNetworkErrors = ['ENOTFOUND', 'EAI_AGAIN'];
  // ❌ ETIMEDOUT entfernt - ist ein Timeout!
  // ❌ ECONNRESET entfernt - kann nach Request passieren (unsicher)
  // ❌ UND_ERR_SOCKET entfernt - kann nach Request passieren (unsicher)
  if (err.code && retryableNetworkErrors.includes(err.code)) {
    return true;
  }
  
  // Check error message for retryable patterns
  if (err.message) {
    const msg = err.message.toLowerCase();
    
    // ✅ ZUERST: Spezifische Server-Fehler (504 Gateway Timeout)
    // WICHTIG: Muss VOR allgemeinem "timeout" Check kommen!
    if (msg.includes('gateway timeout') || msg.includes('504')) {
      return true; // 504 Gateway Timeout = Server-Fehler, nicht abgerechnet
    }
    if (msg.includes('bad gateway') || msg.includes('502')) {
      return true; // 502 Bad Gateway = Server-Fehler, nicht abgerechnet
    }
    if (msg.includes('service unavailable') || msg.includes('503')) {
      return true; // 503 Service Unavailable = Server-Fehler, nicht abgerechnet
    }
    
    // ❌ DANN: Allgemeine Timeout-Checks (Client-Timeouts)
    // Diese kommen NACH Server-Fehler-Checks, damit "gateway timeout" nicht gefangen wird
    if (msg.includes('timeout')) {
      return false; // 🔧 FIX: Client-Timeouts NICHT retry (bereits abgerechnet!)
    }
    
    // ❌ Alle anderen Fehler NICHT retry (könnten bereits abgerechnet sein)
  }
  
  return false;
}

async function withRetry<T>(
  operation: () => Promise<T>,
  maxRetries: number = MAX_API_RETRIES,
  operationName: string = 'Letta API call'
): Promise<T> {
  let lastError: unknown;
  
  // 💰 If retries are disabled, just call once and return/throw
  if (!ENABLE_API_RETRY) {
    return await operation();
  }
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      
      // Check if error is retryable
      if (!isRetryableError(error)) {
        console.error(`❌ ${operationName} failed with non-retryable error:`, error);
        throw error; // Don't retry, throw immediately
      }
      
      // If this was the last attempt, throw
      if (attempt === maxRetries) {
        console.error(`❌ ${operationName} failed after ${maxRetries} retries:`, error);
        throw error;
      }
      
      // Calculate exponential backoff: 1s, 2s, 4s
      const delayMs = Math.pow(2, attempt) * 1000;
      const err = error as RetryableError;
      const statusCode = err.statusCode || 'network error';
      
      console.warn(`⚠️  ${operationName} failed (${statusCode}) - retry ${attempt + 1}/${maxRetries} in ${delayMs}ms... 💰 [Credits: ${attempt + 2}x calls]`);
      
      // Wait before retry
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  
  // This should never be reached, but TypeScript needs it
  throw lastError;
}

// Helper function to process stream
const processStream = async (
  response: Stream<LettaStreamingResponse>,
  discordTarget?: OmitPartialGroupDMChannel<Message<boolean>> | { send: (content: string) => Promise<any> },
  discordClient?: any,
  typingInterval?: NodeJS.Timeout,
  showReasoning: boolean = false,
  taskActionType?: string, // 🔥 NEW: For channel_post tasks, don't redirect to heartbeat log
  toolCalls?: Array<{ tool_name: string; arguments: any }>, // 📝 For collecting tool calls
  toolReturns?: Array<{ tool_name: string; return_value: any }>, // 📝 For collecting tool returns
  reasoningChain?: Array<{ step: number; reasoning: string; timestamp: string }> // 📝 For collecting reasoning chain (for reasoning models)
) => {
  let agentMessageResponse = '';
  let hasSentViaStream = false; // Track if we've sent messages during stream (prevent duplicates)
  let sentCharsCount = 0; // Track how many characters we've already sent (to prevent duplicates)
  let typingStopped = false; // Track if we've stopped the typing indicator
  let receivedAssistantMessage = false; // Track if we received any assistant_message chunks from Letta
  let lettaApiError = false; // Track if Letta returned llm_api_error
  
  // 🔍 DEBUG: Log task messages
  if (taskActionType !== undefined && discordTarget) {
    let targetChannelId: string | undefined;
    if ('id' in discordTarget && typeof discordTarget.id === 'string') {
      targetChannelId = discordTarget.id;
    }
    console.log(`📋 Task message detected (action_type="${taskActionType}") - sending to target channel ${targetChannelId}`);
  }
  
  const sendAsyncMessage = async (content: string, useHeartbeatLog = false) => {
    // 🔥 useHeartbeatLog param is kept for backwards compatibility but ignored
    const target = discordTarget;
    if (target && content.trim()) {
      // 🔥 CRITICAL FIX: hasSentViaStream NUR setzen, NACHDEM der Discord-API-Call erfolgreich war!
      // Vorher wurde es VOR dem API-Call gesetzt, was zu falschen Annahmen führte
      
      // Stop typing indicator on first message send
      if (!typingStopped && typingInterval) {
        clearInterval(typingInterval);
        typingStopped = true;
      }
      const DISCORD_LIMIT = 1900; // Keep margin under 2000
      
      // Extract channel info for logging
      let channelId: string | undefined;
      let channelName: string | undefined;
      let isDM = false;
      if ('id' in target && typeof target.id === 'string') {
        channelId = target.id;
      }
      if ('name' in target && typeof target.name === 'string') {
        channelName = target.name;
      }
      if ('type' in target && (target as any).type === 1) {
        isDM = true;
        channelName = 'DM';
      }
      
      try {
        // 🔥 CHUNKING FIX: Split long messages from send_message tool
        if (content.length > DISCORD_LIMIT) {
          console.log(`📦 [send_message tool] Message is ${content.length} chars, chunking...`);
          const chunks = chunkText(content, DISCORD_LIMIT);
          console.log(`📦 Sending ${chunks.length} chunks to Discord`);
          
          for (const chunk of chunks) {
            if ('reply' in target) {
              await target.channel.send(chunk);
            } else {
              await target.send(chunk);
            }
            // Small delay between chunks
            await new Promise(resolve => setTimeout(resolve, 500));
          }
        } else {
          // Normal single message
          if ('reply' in target) {
            await target.channel.send(content);
          } else {
            await target.send(content);
          }
        }
        
        // ✅ NUR JETZT, NACHDEM der Discord-API-Call erfolgreich war, setzen wir hasSentViaStream!
        hasSentViaStream = true; // Mark that we've sent something
        sentCharsCount += content.length; // Track how many chars we've sent
        
        // 📝 LOG: Bot response sent via stream
        logBotResponse(
          content,
          channelId,
          channelName,
          undefined, // userId not available here
          undefined, // username not available here
          undefined, // messageId not available here
          isDM
        );
      } catch (error) {
        console.error('❌ Error sending async message:', error);
        // 🔥 CRITICAL: hasSentViaStream bleibt false, weil der API-Call fehlgeschlagen ist!
        // Die Nachricht wird am Ende nochmal versucht zu senden
        throw error; // Re-throw damit der Caller weiß, dass es fehlgeschlagen ist
      }
    }
  };

  try {
    for await (const chunk of response) {
      // Handle different message types that might be returned
      if ('messageType' in chunk) {
        // 🔧 FIX: Handle error_message gracefully (don't crash on Letta API errors)
        const chunkAny = chunk as any;
        if (chunkAny.messageType === 'error_message') {
          console.warn('⚠️ Received error_message from Letta stream - continuing to process other chunks');
          // Log error details if available
          if (chunkAny.error || chunkAny.message) {
            const errorInfo = chunkAny.error || chunkAny.message || 'Unknown error';
            console.warn('⚠️ Error details:', errorInfo);
          }
          continue; // Skip this chunk but continue processing stream
        }
        
        switch (chunk.messageType) {
          case 'assistant_message':
            if ('content' in chunk && typeof chunk.content === 'string') {
              receivedAssistantMessage = true; // Mark that we got at least one assistant_message chunk
              agentMessageResponse += chunk.content;
              // 🔥 Send assistant messages immediately to Discord
              if (discordTarget && chunk.content.trim()) {
                try {
                  console.log(`📤 [assistant_message] Sending chunk to Discord (${chunk.content.length} chars): "${chunk.content.substring(0, 50)}..."`);
                  await sendAsyncMessage(chunk.content, false);
                  console.log(`✅ [assistant_message] Chunk sent successfully`);
                } catch (sendError) {
                  console.error('❌ Error sending assistant_message chunk:', sendError);
                  // 🔥 CRITICAL: Wenn sendAsyncMessage fehlschlägt, wurde hasSentViaStream bereits zurückgesetzt
                  // Wir müssen die Nachricht am Ende nochmal senden!
                  // Continue - we'll try to send the full message at the end if stream fails
                }
              }
            }
            break;
          case 'stop_reason':
            console.log('🛑 Stream stopped:', chunk);
            // 🔧 FIX: Track if we got an llm_api_error - this means Letta had an internal error
            // We should inform the user about this error
            if ('stopReason' in chunk && String(chunk.stopReason) === 'llm_api_error') {
              console.warn(`⚠️  [processStream] Letta returned llm_api_error - no response will be available`);
              lettaApiError = true; // Mark that we got an API error
              // Don't throw error, just let stream end normally
              // We'll check this at the end and return a specific marker
            }
            break;
          case 'reasoning_message':
            console.log('🧠 Reasoning:', chunk);
            
            // Try multiple ways to access reasoning field
            let reasoningText: string | null = null;
            
            // Method 1: Direct field access
            if ('reasoning' in chunk && typeof chunk.reasoning === 'string') {
              reasoningText = chunk.reasoning;
            }
            // Method 2: Check chunk as any (type cast)
            else if (typeof (chunk as any).reasoning === 'string') {
              reasoningText = (chunk as any).reasoning;
            }
            // Method 3: Check content field
            else if ('content' in chunk && typeof chunk.content === 'string') {
              reasoningText = chunk.content;
            }
            
            if (reasoningText && reasoningText.trim()) {
              // 📝 ALWAYS collect reasoning for fine-tuning (even if not shown to user)
              if (reasoningChain) {
                reasoningChain.push({
                  step: reasoningChain.length + 1,
                  reasoning: reasoningText.trim(),
                  timestamp: new Date().toISOString()
                });
              }
              
              // Only show reasoning to Discord if showReasoning flag is true (e.g., heartbeat context)
              if (showReasoning && discordTarget) {
                console.log(`📤 [REASONING] Sending reasoning to Discord (${reasoningText.length} chars)`);
                await sendAsyncMessage(`*💭 ${reasoningText.trim()}*`);
                
                // 📝 LOG: Reasoning (separate entry)
                let channelId: string | undefined;
                if (discordTarget && 'id' in discordTarget && typeof discordTarget.id === 'string') {
                  channelId = discordTarget.id;
                }
                logReasoning(reasoningText.trim(), channelId);
              } else {
                console.log(`🔕 [REASONING] Collected for fine-tuning but not shown to user (showReasoning=${showReasoning})`);
              }
            } else {
              console.log(`⚠️ [REASONING] No reasoning text found in chunk:`, JSON.stringify(chunk).substring(0, 200));
            }
            break;
          case 'tool_call_message':
            console.log('🔧 Tool call:', chunk);
            // 🔥 FIX (Oct 17, 2025): Parse send_message tool calls and actually send to Discord!
            if ('toolCall' in chunk && chunk.toolCall) {
              const toolCall = chunk.toolCall as any;
              
              // Extract channel info for logging
              let channelId: string | undefined;
              let channelName: string | undefined;
              if (discordTarget && 'id' in discordTarget && typeof discordTarget.id === 'string') {
                channelId = discordTarget.id;
              }
              if (discordTarget && 'name' in discordTarget && typeof (discordTarget as any).name === 'string') {
                channelName = (discordTarget as any).name;
              }
              
              // Parse arguments
              let parsedArgs: any;
              try {
                parsedArgs = typeof toolCall.arguments === 'string' 
                  ? JSON.parse(toolCall.arguments) 
                  : toolCall.arguments;
              } catch (err) {
                parsedArgs = toolCall.arguments; // Fallback to raw
              }
              
              // 📝 LOG: Tool call
              logToolCall(
                toolCall.name || 'unknown_tool',
                parsedArgs,
                channelId,
                channelName
              );
              
              // 📝 Collect tool call for conversation_turn
              if (toolCalls) {
                toolCalls.push({
                  tool_name: toolCall.name || 'unknown_tool',
                  arguments: parsedArgs
                });
              }
              
              // 🔒 DM RESTRICTION: Validate DM target if ALLOWED_DM_USER_ID is configured
              if (toolCall.name === 'discord_tool' && parsedArgs) {
                // Check send_message action with target_type="user" (DM)
                if (parsedArgs.action === 'send_message' && 
                    (parsedArgs.target_type === 'user' || (!parsedArgs.target_type && parsedArgs.target))) {
                  
                  // If ALLOWED_DM_USER_ID is set, validate target
                  if (ALLOWED_DM_USER_ID) {
                    const targetUserId = parsedArgs.target;
                    if (targetUserId && targetUserId !== ALLOWED_DM_USER_ID) {
                      const errorMsg = `🔒 DM restriction: Can only send DMs to user ${ALLOWED_DM_USER_ID}, but target was ${targetUserId}. Blocking tool call.`;
                      console.warn(`⚠️  ${errorMsg}`);
                      await sendAsyncMessage(`❌ ${errorMsg}`);
                      // Block the tool call by setting invalid target (will cause tool to fail)
                      parsedArgs.target = '000000000000000000'; // Invalid Discord ID
                      parsedArgs.target_type = 'user'; // Ensure it's treated as user
                      console.log(`🔒 Blocked DM to unauthorized user - tool will fail`);
                    } else if (targetUserId === ALLOWED_DM_USER_ID) {
                      console.log(`✅ DM target validated: ${targetUserId} matches ALLOWED_DM_USER_ID`);
                    }
                  }
                }
                
                // Check execute_batch for send_message operations with user targets
                if (parsedArgs.action === 'execute_batch' && parsedArgs.operations && Array.isArray(parsedArgs.operations)) {
                  if (ALLOWED_DM_USER_ID) {
                    const invalidOps: any[] = [];
                    parsedArgs.operations.forEach((op: any, index: number) => {
                      if (op.action === 'send_message' && 
                          (op.target_type === 'user' || (!op.target_type && op.target)) &&
                          op.target && op.target !== ALLOWED_DM_USER_ID) {
                        invalidOps.push({ index, op });
                      }
                    });
                    
                    if (invalidOps.length > 0) {
                      const errorMsg = `🔒 DM restriction: ${invalidOps.length} operation(s) blocked - can only send DMs to user ${ALLOWED_DM_USER_ID}`;
                      console.warn(`⚠️  ${errorMsg}`);
                      await sendAsyncMessage(`❌ ${errorMsg}`);
                      // Block invalid operations by setting invalid target
                      invalidOps.forEach(({ index, op }) => {
                        parsedArgs.operations[index].target = '000000000000000000'; // Invalid Discord ID
                        parsedArgs.operations[index].target_type = 'user';
                      });
                      console.log(`🔒 Blocked ${invalidOps.length} unauthorized DM operation(s) - they will fail`);
                    }
                  }
                }
              }
              
              // 🔥 FIX (Jan 2025): Intercept discord_tool send_message calls for user_reminder tasks
              // If targetChannel is already set (e.g., DM channel for user_reminder), send directly there
              // instead of letting the tool execute (which might fail without target parameter)
              // Only intercept for user_reminder tasks (taskActionType === 'user_reminder')
              if (toolCall.name === 'discord_tool' && parsedArgs && taskActionType === 'user_reminder' && discordTarget) {
                // Handle direct send_message action
                if (parsedArgs.action === 'send_message' && parsedArgs.message) {
                  try {
                    console.log('📤 [user_reminder fix] Intercepting discord_tool send_message - sending directly to targetChannel (DM)');
                    await sendAsyncMessage(parsedArgs.message);
                    // Mark that we've handled this, so tool return doesn't try to send again
                    hasSentViaStream = true;
                  } catch (err) {
                    console.error('❌ Error sending message from discord_tool intercept:', err);
                  }
                }
                // Handle execute_batch with send_message operations
                else if (parsedArgs.action === 'execute_batch' && parsedArgs.operations && Array.isArray(parsedArgs.operations)) {
                  try {
                    // Find all send_message operations in the batch
                    const sendMessageOps = parsedArgs.operations.filter((op: any) => op.action === 'send_message' && op.message);
                    if (sendMessageOps.length > 0) {
                      console.log(`📤 [user_reminder fix] Intercepting execute_batch with ${sendMessageOps.length} send_message operation(s) - sending directly to targetChannel (DM)`);
                      // Send all messages from the batch
                      for (const op of sendMessageOps) {
                        await sendAsyncMessage(op.message);
                      }
                      hasSentViaStream = true;
                    }
                  } catch (err) {
                    console.error('❌ Error sending messages from execute_batch intercept:', err);
                  }
                }
              } else if (toolCall.name === 'send_message') {
                try {
                  if (parsedArgs && parsedArgs.message) {
                    console.log('📤 Sending message from send_message tool call to Discord...');
                    await sendAsyncMessage(parsedArgs.message);
                  }
                } catch (err) {
                  console.error('❌ Error parsing send_message arguments:', err);
                }
              } else {
                // 🔥 NEW (Nov 19, 2025): Compact, informative tool call display
                try {
                  const toolName = toolCall.name || 'unknown_tool';
                  let toolMessage = formatToolCall(toolName, parsedArgs);
                  await sendAsyncMessage(toolMessage);
                } catch (err) {
                  console.error('❌ Error formatting tool call for Discord:', err);
                  // Fallback to simple format
                  await sendAsyncMessage(`🔧 Tool Call: ${toolCall.name || 'unknown_tool'}`);
                }
              }
            }
            break;
          case 'tool_return_message':
            console.log('🔧 Tool return:', chunk);
            // 🔧 FIX: Gesamte Tool Return Behandlung in try-catch, damit Fehler den Stream nicht stoppen!
            try {
              if ('name' in chunk && typeof chunk.name === 'string') {
                // Extract channel info for logging
                let channelId: string | undefined;
                let channelName: string | undefined;
                if (discordTarget && 'id' in discordTarget && typeof discordTarget.id === 'string') {
                  channelId = discordTarget.id;
                }
                if (discordTarget && 'name' in discordTarget && typeof (discordTarget as any).name === 'string') {
                  channelName = (discordTarget as any).name;
                }
                
                // Get return value
                const returnValue = 'return_value' in chunk ? chunk.return_value : undefined;
                
                // 📝 LOG: Tool return (wrapped in try-catch so logging errors don't stop the stream!)
                try {
                  logToolReturn(
                    chunk.name,
                    returnValue,
                    channelId,
                    channelName
                  );
                } catch (logError) {
                  console.error('❌ Error logging tool return (non-fatal):', logError);
                  // Continue processing - don't let logging errors stop the stream!
                }
                
                // 📝 Collect tool return for conversation_turn
                if (toolReturns) {
                  toolReturns.push({
                    tool_name: chunk.name,
                    return_value: returnValue
                  });
                }
                
                // 🔧 FIX: Detect specific tool errors and send better error messages
                // IMPORTANT: Stream continues even if tool fails - the bot can still respond!
                const returnValueStr = typeof returnValue === 'string' ? returnValue : JSON.stringify(returnValue || '');
                
                // Prüfe auf Memory 5000-Zeichen-Limit Fehler
                if (returnValueStr.includes('Exceeds 5000 character limit') || returnValueStr.includes('5000 character limit')) {
                  // Extrahiere die tatsächliche Zeichenanzahl aus dem Fehler
                  const charLimitMatch = returnValueStr.match(/requested (\d+)/i);
                  const requestedChars = charLimitMatch ? charLimitMatch[1] : '?';
                  
                  console.error(`❌ Memory block too large: ${requestedChars}/5000 characters`);
                  
                  // Specific error message instead of generic tool return
                  // Stream continues - the bot can still respond!
                  try {
                    const errorMessage = `🧠 **Memory Limit Error**\n> Memory block zu groß (${requestedChars}/5000 Zeichen). Bitte kürze den Inhalt oder teile ihn auf mehrere Blöcke auf.`;
                    await sendAsyncMessage(errorMessage);
                  } catch (sendError) {
                    console.error('❌ Error sending memory limit error message (non-fatal):', sendError);
                    // Continue - don't let Discord send errors stop the stream!
                  }
                  // NO break - stream continues so the bot can still respond!
                }
                // Prüfe auf andere ValidationErrors
                else if (returnValueStr.includes('validation error') || returnValueStr.includes('ValidationError')) {
                  console.error(`❌ Tool validation error: ${chunk.name}`);
                  
                  // Versuche spezifische Fehlermeldung zu extrahieren
                  const validationMatch = returnValueStr.match(/Value error[^:]*:\s*([^\n]+)/i);
                  const specificError = validationMatch ? validationMatch[1].trim() : 'Validation error occurred';
                  
                  try {
                    const errorMessage = `⚠️ **Tool Error (${chunk.name})**\n> ${specificError}`;
                    await sendAsyncMessage(errorMessage);
                  } catch (sendError) {
                    console.error('❌ Error sending validation error message (non-fatal):', sendError);
                    // Continue - don't let Discord send errors stop the stream!
                  }
                  // KEIN break - Stream läuft weiter!
                }
                // Prüfe auf Voice Message Timeout-Fehler (spezifische Behandlung)
                else if (chunk.name === 'send_voice_message' && returnValueStr.toLowerCase().includes('timeout')) {
                  console.error(`⏰ Voice message timeout: ${chunk.name}`);
                  try {
                    // Extrahiere spezifische Timeout-Info aus dem Fehler
                    const timeoutMatch = returnValueStr.match(/(ElevenLabs|Discord).*?(\d+)\s*seconds?/i);
                    const timeoutInfo = timeoutMatch ? ` (${timeoutMatch[1]} timeout: ${timeoutMatch[2]}s)` : '';
                    
                    const errorMessage = `⏰ **Voice Message Timeout**\n> Die Sprachnachricht war zu lang und hat ein Timeout verursacht${timeoutInfo}. Bitte versuche es erneut oder teile die Nachricht in kleinere Teile auf.`;
                    await sendAsyncMessage(errorMessage);
                  } catch (sendError) {
                    console.error('❌ Error sending voice message timeout error (non-fatal):', sendError);
                    // Continue - don't let Discord send errors stop the stream!
                  }
                  // KEIN break - Stream läuft weiter!
                }
                // Prüfe auf allgemeine Fehler im returnValue
                else if (returnValueStr.toLowerCase().includes('error') && returnValueStr.toLowerCase().includes('failed')) {
                  console.warn(`⚠️ Tool returned error: ${chunk.name}`);
                  // Zeige trotzdem den Tool Return, aber markiere als Fehler
                  try {
                    let returnMessage = `⚠️ **Tool Return (${chunk.name}) - Error**`;
                    if (returnValue) {
                      const errorPreview = typeof returnValue === 'string' 
                        ? returnValue.substring(0, 300)
                        : JSON.stringify(returnValue).substring(0, 300);
                      returnMessage += `\n> ${errorPreview}${errorPreview.length >= 300 ? '...' : ''}`;
                    }
                    await sendAsyncMessage(returnMessage);
                  } catch (sendError) {
                    console.error('❌ Error sending tool error message (non-fatal):', sendError);
                    // Continue - don't let Discord send errors stop the stream!
                  }
                  // KEIN break - Stream läuft weiter!
                }
                // Normale Tool Returns (kein Fehler)
                else {
                  // Display to Discord
                  try {
                    let returnMessage = `**Tool Return (${chunk.name})**`;
                    if (returnValue) {
                      returnMessage += `\n> ${JSON.stringify(returnValue).substring(0, 200)}...`;
                    }
                    await sendAsyncMessage(returnMessage);
                  } catch (sendError) {
                    console.error('❌ Error sending tool return message (non-fatal):', sendError);
                    // Continue - don't let Discord send errors stop the stream!
                  }
                }
              }
            } catch (toolReturnError) {
              // 🔧 CRITICAL FIX: Fehler in Tool Return Behandlung stoppen den Stream NICHT!
              console.error('❌ Error processing tool return (non-fatal - stream continues):', toolReturnError);
              // Stream continues - the bot can still respond!
            }
            break;
          case 'usage_statistics':
            console.log('📊 Usage stats:', chunk);
            break;
          default:
            console.log('📨 Unknown message type:', chunk.messageType, chunk);
        }
      } else {
        console.log('❓ Chunk without messageType:', chunk);
      }
    }
  } catch (error) {
    console.error('❌ Error processing stream:', error);
    const errMsg = error instanceof Error ? error.message : String(error);
    
    // 🔧 FIX: ParseError from Letta SDK when it receives "error_message" 
    // LETTA kann einen error_message im Stream senden, den der SDK-Parser nicht erwartet
    // Wenn LETTA bereits eine Antwort erstellt hat (usage_statistics vorhanden), 
    // sollten wir die gesammelte Antwort zurückgeben statt zu crashen
    if (/ParseError|Expected.*Received "error_message"|error_message/i.test(errMsg)) {
      console.warn(`⚠️ ParseError: LETTA sent error_message in stream - returning collected text (${agentMessageResponse.length} chars)`);
      // 🔥 FIX: Wenn bereits via Stream gesendet wurde, keine Duplikate!
      if (hasSentViaStream && discordTarget) {
        console.log(`✅ [PARSE ERROR] Message already sent via stream - suppressing duplicate (${agentMessageResponse.length} chars collected)`);
        return "";
      }
      // Wenn wir bereits eine Antwort haben, geben wir sie zurück
      // Sonst werfen wir den Fehler weiter (echter Fehler)
      if (agentMessageResponse.length > 0) {
        console.log(`✅ Returning collected response despite ParseError (${agentMessageResponse.length} chars, hasSentViaStream=${hasSentViaStream})`);
        return agentMessageResponse;
      }
      // Keine Antwort gesammelt = echter Fehler
      console.error(`❌ ParseError but no response collected - this is a real error`);
    }
    
    // Ping errors
    if (/Expected.*Received "ping"|Expected.*Received "pong"/i.test(errMsg)) {
      console.log(`🏓 Ping parse error - returning collected text (${agentMessageResponse.length} chars)`);
      // 🔥 FIX: Wenn bereits via Stream gesendet wurde, keine Duplikate!
      if (hasSentViaStream && discordTarget) {
        console.log(`✅ [PING ERROR] Message already sent via stream - suppressing duplicate`);
        return "";
      }
      return agentMessageResponse;
    }
    
    // Socket termination errors (von gestern!)
    if (/terminated|other side closed|socket.*closed|UND_ERR_SOCKET/i.test(errMsg)) {
      console.log(`🔌 Stream terminated early - returning collected text (${agentMessageResponse.length} chars)`);
      // 🔥 FIX: Wenn bereits via Stream gesendet wurde, keine Duplikate!
      if (hasSentViaStream && discordTarget) {
        console.log(`✅ [SOCKET ERROR] Message already sent via stream - suppressing duplicate`);
        return "";
      }
      return agentMessageResponse;
    }
    
    // Timeout errors during stream processing - return what we have
    if (/timeout/i.test(errMsg)) {
      console.warn(`⏰ Stream processing timed out - collected ${agentMessageResponse.length} chars, sent ${sentCharsCount} chars, hasSentViaStream=${hasSentViaStream}`);
      
      // 🔥 CRITICAL FIX: Bei Timeout prüfen wir, ob noch ungesendete Chunks vorhanden sind
      // Problem: Wenn bereits Chunks gesendet wurden, dürfen wir NICHT die gesamte Nachricht nochmal senden (Duplikate!)
      // Lösung: Nur die RESTLICHEN Chunks senden (Differenz zwischen gesammelt und gesendet)
      if (hasSentViaStream && discordTarget && agentMessageResponse.trim()) {
        const remainingChars = agentMessageResponse.length - sentCharsCount;
        
        if (remainingChars > 0) {
          // Es gibt noch ungesendete Chunks - sende nur die Reste
          const remainingText = agentMessageResponse.substring(sentCharsCount);
          console.log(`⚠️ [TIMEOUT] Stream timed out - ${remainingChars} chars remaining (${sentCharsCount}/${agentMessageResponse.length} already sent). Sending remaining...`);
          
          try {
            await sendAsyncMessage(remainingText, false);
            console.log(`✅ [TIMEOUT RECOVERY] Remaining ${remainingChars} chars sent successfully`);
            return ""; // Already sent, no need to return
          } catch (recoveryError) {
            console.error('❌ [TIMEOUT RECOVERY] Failed to send remaining message:', recoveryError);
            // Fallback: Return only the remaining part
            return remainingText;
          }
        } else {
          // Alle Chunks wurden bereits gesendet - keine Duplikate!
          console.log(`✅ [TIMEOUT] All ${agentMessageResponse.length} chars were already sent - suppressing duplicate`);
          return ""; // Already sent completely, no need to return
        }
      }
      
      // Wenn noch nichts gesendet wurde, geben wir die gesammelte Antwort zurück
      return agentMessageResponse;
    }
    
    throw error;
  }
  
  // 🔥 Prevent duplicate: If we already sent messages during stream, check if we need to send remaining
  // WICHTIG: Nur die RESTLICHEN Chunks senden, nicht die gesamte Nachricht nochmal!
  if (hasSentViaStream && discordTarget) {
    const remainingChars = agentMessageResponse.length - sentCharsCount;
    
    if (remainingChars > 0 && agentMessageResponse.trim()) {
      // Es gibt noch ungesendete Chunks - sende nur die Reste
      const remainingText = agentMessageResponse.substring(sentCharsCount);
      console.log(`⚠️ [STREAM END] ${remainingChars} chars remaining (${sentCharsCount}/${agentMessageResponse.length} already sent). Sending remaining...`);
      try {
        await sendAsyncMessage(remainingText, false);
        console.log(`✅ [STREAM END] Remaining ${remainingChars} chars sent successfully`);
        return "";
      } catch (sendError) {
        console.error('❌ [STREAM END] Failed to send remaining message:', sendError);
        // Return only the remaining part
        return remainingText;
      }
    } else {
      // Alle Chunks wurden bereits gesendet - keine Duplikate!
      console.log(`✅ [STREAM END] All ${agentMessageResponse.length} chars were already sent - suppressing duplicate`);
      return "";
    }
  }
  
  // 🔧 FIX: Check if Letta had an API error (llm_api_error)
  // This means Letta's internal API had an error - we should inform the user
  if (lettaApiError && !receivedAssistantMessage) {
    console.warn(`⚠️  [processStream] Letta API error (llm_api_error) - no response available`);
    // Return special marker that tells sendMessage/sendTimerMessage to inform the user
    return "__LETTA_API_ERROR__";
  }
  
  // 🔧 FIX: Check if Letta sent NO assistant_message (only reasoning)
  // This can happen with reasoning models that think but don't respond
  if (!receivedAssistantMessage && agentMessageResponse.length === 0 && reasoningChain && reasoningChain.length > 0) {
    console.warn(`⚠️  [processStream] Letta sent only reasoning (${reasoningChain.length} step(s)), but NO assistant_message!`);
    // Return special marker that tells sendMessage to inform the user
    return "__LETTA_NO_RESPONSE__";
  }
  
  return agentMessageResponse;
}

// TODO refactor out the core send message / stream parse logic to clean up this function
// Sending a timer message (Heartbeat)
// ✅ SEQUENTIAL: Heartbeats go through queue to prevent parallel API calls with Tasks
async function sendTimerMessage(channel?: { send: (content: string) => Promise<any> }) {
  if (!AGENT_ID) {
    console.error('Error: LETTA_AGENT_ID is not set');
    return SURFACE_ERRORS
      ? `Beep boop. My configuration is not set up properly. Please message me after I get fixed 👾`
      : "";
  }

  // Generate current timestamp (configured timezone)
  const TIMEZONE = process.env.TIMEZONE || 'Europe/Berlin';
  const now = new Date();
  const berlinTime = new Intl.DateTimeFormat('de-DE', {
    timeZone: TIMEZONE,
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).format(now);

  // Get German weekday
  const weekday = new Intl.DateTimeFormat('de-DE', {
    timeZone: TIMEZONE,
    weekday: 'long'
  }).format(now);

  // Check Spotify "Now Playing" (if credentials available)
  let spotifyInfo = '';
  try {
    console.log('🎵 [HEARTBEAT] Checking Spotify now playing...');
    const spotifyData = await getSpotifyNowPlaying();
    if (spotifyData) {
      console.log('✅ [HEARTBEAT] Spotify data retrieved:', spotifyData.substring(0, 100));
      spotifyInfo = `\n\n🎵 Now Playing:\n${spotifyData}`;
    } else {
      console.log('ℹ️ [HEARTBEAT] Spotify returned null (nothing playing)');
      spotifyInfo = `\n\n🔇 Spotify ist still (nichts läuft)`;
    }
  } catch (err) {
    // Silently fail if Spotify not available
    console.log('ℹ️ Spotify info not available for heartbeat:', err instanceof Error ? err.message : err);
  }

  // Check Munich weather (if API key available)
  let weatherInfo = '';
  try {
    const weatherData = await getMunichWeather();
    if (weatherData) {
      weatherInfo = `\n\n${weatherData}`;
    }
  } catch (err) {
    // Silently fail if Weather not available
    console.log('ℹ️ Weather info not available for heartbeat:', err instanceof Error ? err.message : err);
  }

  // Build heartbeat message (system-initiated, not from user)
  const language = process.env.BOT_LANGUAGE || 'en';
  const heartbeatTitle = language === 'de' ? 'HERZSCHLAG' : 'HEARTBEAT';
  const timeText = language === 'de' ? 'Uhr.' : '';
  const capabilitiesText = language === 'de' 
    ? `Ich kann:
• Discord DM senden (oder Channel-Posts)
• Voicemails senden
• Spotify steuern 🎵 (skip, queue, playlists erstellen)
• Memories durchdenken/organisieren
• Web durchsuchen
• Archival memories taggen`
    : `I can:
• Send Discord DMs or channel posts
• Send voice messages
• Control Spotify 🎵 (skip, queue, create playlists)
• Think through and organize memories
• Search the web
• Tag archival memories`;

  let heartbeatContent = `[🜂] ${heartbeatTitle}
${weekday}, ${berlinTime}${timeText}${weatherInfo}${spotifyInfo}

${capabilitiesText}
`;

  const batchReminder = language === 'de'
    ? '\n\n💰 WICHTIG: Nutze Batch-Befehle (execute_batch) um API-Credits zu sparen!\n📋 RULE: USE BATCH FUNKTIONEN BEI TOOL CALL'
    : '\n\n💰 IMPORTANT: Use batch commands (execute_batch) to save API credits!\n📋 RULE: USE BATCH FUNCTIONS FOR TOOL CALLS';
  
  const closingLine = language === 'de' ? '\n\nMein Puls, meine Wahl.' : '\n\nMy pulse, my choice.';
  
  heartbeatContent += batchReminder + closingLine;

  const lettaMessage = {
    role: "system" as const,  // System message, not user message
    content: heartbeatContent
  };

  try {
    console.log(`🛜 Sending heartbeat to Letta server (agent=${AGENT_ID}): ${JSON.stringify(lettaMessage)}`);
    
    // ✅ SEQUENTIAL: Use queue to prevent parallel API calls with Tasks
    const response = await apiQueue.enqueue(
      async () => {
        // 🔄 Wrap Letta API call with retry logic
        return await withRetry(
          async () => await client.agents.messages.createStream(AGENT_ID, {
            messages: [lettaMessage]
          }),
          MAX_API_RETRIES, // Controlled by env var (default: 1)
          'Heartbeat message'
        );
      },
      'Heartbeat message'
    ) as Stream<LettaStreamingResponse>;

    if (response) {
      try {
        const heartbeatResponse = (await processStream(response, channel, undefined, undefined, true)) || "";
        
        // 🔧 FIX: Check for special marker indicating Letta had an API error
        // We should inform the user about this error with a specific message
        if (heartbeatResponse === "__LETTA_API_ERROR__") {
          console.warn(`⚠️  [sendTimerMessage] Letta API error (llm_api_error) in heartbeat - informing user`);
          return "⚠️ **Heartbeat Error**\n> Letta API hatte einen internen Fehler (`llm_api_error`). Nächster Heartbeat in 40 Minuten.";
        }
        
        // 🔧 FIX: Check for special marker indicating Letta sent only reasoning, no assistant_message
        // For heartbeats, we should silently fail (no "Beep boop" spam in heartbeat channel!)
        if (heartbeatResponse === "__LETTA_NO_RESPONSE__") {
          console.warn(`⚠️  [sendTimerMessage] Letta sent only reasoning, but NO assistant_message for heartbeat. Silently failing (no spam).`);
          return ""; // Don't send "Beep boop" for heartbeats!
        }
        
        // 📝 LOG: Heartbeat
        let channelId: string | undefined;
        let channelName: string | undefined;
        if (channel && 'id' in channel && typeof channel.id === 'string') {
          channelId = channel.id;
        }
        if (channel && 'name' in channel && typeof (channel as any).name === 'string') {
          channelName = (channel as any).name;
        }
        logHeartbeat(heartbeatContent, channelId, channelName);
        
        return heartbeatResponse;
      } catch (streamError) {
        // 🔧 FIX: Wenn processStream einen Fehler hat, aber bereits Daten gesammelt wurden,
        // sollten wir die gesammelten Daten zurückgeben statt "beep boop"
        const errMsg = streamError instanceof Error ? streamError.message : String(streamError);
        
        // Bei Timeout während Stream-Verarbeitung: Stream hat vielleicht schon Daten gesammelt
        // processStream gibt bereits gesammelte Daten zurück bei Timeout, also sollte das nicht passieren
        // Aber zur Sicherheit: Wenn es ein Timeout ist, versuchen wir trotzdem weiter
        if (/timeout/i.test(errMsg)) {
          console.warn('⚠️ Stream processing timed out in heartbeat - this should have been handled by processStream');
          // processStream sollte bereits gesammelte Daten zurückgeben, aber falls nicht:
          return '⚠️ **Heartbeat Timeout**\n> Letta API hat zu lange gebraucht. Nächster Heartbeat in 40 Minuten.';
        }
        
        // Andere Fehler: weiterwerfen
        throw streamError;
      }
    }

    return "";
  } catch (error) {
    // 🔧 FIX: llm_api_error should be handled with a specific error message
    const errMsg = error instanceof Error ? error.message : String(error);
    if (/llm_api_error|api.*error/i.test(errMsg)) {
      console.warn('⚠️ Letta API error in heartbeat - informing user');
      return "⚠️ **Heartbeat Error**\n> Letta API hatte einen internen Fehler (`llm_api_error`). Nächster Heartbeat in 40 Minuten.";
    }
    
    if (error instanceof Error && /timeout/i.test(error.message)) {
      console.error('⚠️  Letta request timed out.');
      // ✅ User bekommt IMMER eine Meldung bei Timeout (auch wenn SURFACE_ERRORS=false)
      return '⚠️ **Heartbeat Timeout**\n> Letta API hat zu lange gebraucht. Nächster Heartbeat in 40 Minuten.';
    }
    
    // Check if it's a retryable error that failed after retries
    const err = error as RetryableError;
    if (err.statusCode && [502, 503, 504].includes(err.statusCode)) {
      console.error(`❌ Letta API unavailable (${err.statusCode}) after retries`);
      // ✅ User bekommt IMMER eine Meldung (auch wenn SURFACE_ERRORS=false)
      const retryCount = MAX_API_RETRIES === 1 ? '1 time' : `${MAX_API_RETRIES} times`;
      return `⚠️ **Heartbeat Error (${err.statusCode})**\n> Letta API ist temporär nicht verfügbar. Ich habe ${retryCount} versucht. Nächster Heartbeat in 40 Minuten.`;
    }
    
    console.error(error);
    // ✅ User bekommt IMMER eine Meldung bei Fehlern
    return '⚠️ **Heartbeat Error**\n> Unbekannter Fehler bei der Kommunikation mit Letta. Nächster Heartbeat in 40 Minuten.';
  }
}

// Send message and receive response
async function sendMessage(
  discordMessageObject: OmitPartialGroupDMChannel<Message<boolean>>,
  messageType: MessageType,
  conversationContext: string | null = null,
  customContent: string | null = null
) {
  const { author: { username: senderName, id: senderId }, content: originalMessage } =
    discordMessageObject;
  
  // Use custom content if provided (e.g. for file chunks or transcripts), otherwise use original message
  const message = customContent || originalMessage;

  if (!AGENT_ID) {
    console.error('Error: LETTA_AGENT_ID is not set');
    return SURFACE_ERRORS
      ? `Beep boop. My configuration is not set up properly. Please message me after I get fixed 👾`
      : "";
  }

  // Generate current timestamp (configured timezone) for this message
  // SECURITY: Error handling for invalid system clock (rare but possible)
  const TIMEZONE = process.env.TIMEZONE || 'Europe/Berlin';
  let timestampString = '';
  try {
    const now = new Date();
    // Validate date before formatting
    if (isNaN(now.getTime())) {
      throw new Error('Invalid system time');
    }
    
    // Format: "Mo, 20.11., 14:30" (Wochentag, Datum, Zeit)
    const dateFormatter = new Intl.DateTimeFormat('de-DE', {
      timeZone: TIMEZONE,
      weekday: 'short',  // Mo, Di, Mi, etc.
      day: '2-digit',
      month: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    });
    
    const formatted = dateFormatter.format(now);
    // Format: "Mo., 20.11., 14:30" -> "Mo, 20.11., 14:30" (remove period after weekday)
    const berlinTime = formatted.replace(/^(\w+)\./, '$1');
    
    timestampString = `, time=${berlinTime}`;
  } catch (err) {
    console.error('⚠️ Timestamp generation failed:', err instanceof Error ? err.message : err);
    // Fallback: No timestamp if generation fails
    timestampString = '';
  }

  // We include a sender receipt so that agent knows which user sent the message
  // We also include the Discord ID so that the agent can tag the user with @
  // SECURITY: Include timestamp to track when messages are sent (prevents timezone confusion)
  const senderNameReceipt = `${senderName} (id=${senderId}${timestampString})`;

  // IMPROVEMENT: Extract channel context so agent knows WHERE the message came from
  const channel = discordMessageObject.channel;
  const channelId = channel.id;
  const channelType = (channel as any).type; // 0=text, 1=DM, 5=announcement, etc
  const isDM = channelType === 1;
  const channelName = isDM ? "DM" : ((channel as any).name || "unknown-channel");
  const channelContext = isDM 
    ? `DM`
    : `#${channelName} (channel_id=${channelId})`;

  // Extract attachment information (non-image files like PDFs, text files, etc.)
  // 🎥 FILE CHUNKING: Use processFileAttachment for automatic text extraction and chunking
  let attachmentInfo = '';
  if (discordMessageObject.attachments && discordMessageObject.attachments.size > 0) {
    const nonImageAttachments = Array.from(discordMessageObject.attachments.values()).filter(att => {
      const ct = (att as any).contentType || '';
      return ct && !ct.startsWith('image/'); // Only non-images (images are handled by attachmentForwarder)
    });
    
    if (nonImageAttachments.length > 0) {
      console.log(`📎 Processing ${nonImageAttachments.length} non-image attachment(s) with file chunking system...`);
      
      // Process all attachments in parallel using processFileAttachment
      const attachmentPromises = nonImageAttachments.map(async (att) => {
        const name = (att as any).name || 'unknown';
        const url = (att as any).url || '';
        const type = (att as any).contentType || 'unknown';
        const size = (att as any).size || 0;
        
        try {
          // Use file chunking system to process the file
          const processed = await processFileAttachment(name, url, type, size);
          return processed;
        } catch (err) {
          console.error(`⚠️ Failed to process attachment ${name}:`, err);
          // Fallback to simple format if processing fails
          const sizeStr = size > 1024*1024 ? `${(size/1024/1024).toFixed(1)}MB` : `${(size/1024).toFixed(0)}KB`;
          return `- \`${name}\` (${type}, ${sizeStr})\n  URL: ${url}\n  ⚠️ Auto-processing failed\n  💡 You can use \`download_discord_file(url="${url}")\` to read this file!`;
        }
      });
      
      // Wait for all attachments to be processed
      const processedAttachments = await Promise.all(attachmentPromises);
      attachmentInfo = '\n\n📎 **Attachments:**\n' + processedAttachments.join('\n');
      
      console.log(`✅ Processed ${processedAttachments.length} attachment(s) - content ready for Letta`);
    }
  }

  // Build message content with optional conversation context
  let messageContent: string;
  
  if (USE_SENDER_PREFIX) {
    // Build base message with sender receipt and channel context
    const baseMessage = messageType === MessageType.MENTION
      ? `[${senderNameReceipt} sent a message mentioning you in ${channelContext}] ${message}${attachmentInfo}`
      : messageType === MessageType.REPLY
        ? `[${senderNameReceipt} replied to you in ${channelContext}] ${message}${attachmentInfo}`
        : messageType === MessageType.DM
          ? `[${senderNameReceipt} sent you a direct message] ${message}${attachmentInfo}`
          : `[${senderNameReceipt} sent a message in ${channelContext}] ${message}${attachmentInfo}`;
    
    // Prepend conversation context if available (autonomous mode)
    messageContent = conversationContext 
      ? `${conversationContext}\n\n${baseMessage}`
      : baseMessage;
  } else {
    messageContent = conversationContext 
      ? `${conversationContext}\n\n${message}${attachmentInfo}`
      : message + attachmentInfo;
  }

  // If LETTA_USE_SENDER_PREFIX, then we put the receipt in the front of the message
  // If it's false, then we put the receipt in the name field (the backend must handle it)
  const lettaMessage = {
    role: "user" as const,
    name: USE_SENDER_PREFIX ? undefined : senderNameReceipt,
    content: messageContent
  };

  // 📝 LOG: User message (incoming)
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

  // 📝 LOG: Full Letta input (for fine-tuning) - what Letta actually sees
  logLettaInput(
    messageContent,  // Full input including context
    conversationContext,
    [lettaMessage],  // Message history (what we send to Letta)
    channelId,
    channelName,
    senderId,
    senderName,
    AGENT_ID
  );

  // Typing indicator: pulse now and every 8 s until cleaned up
  void discordMessageObject.channel.sendTyping();
  const typingInterval = setInterval(() => {
    void discordMessageObject.channel
      .sendTyping()
      .catch(err => console.error('Error refreshing typing indicator:', err));
  }, 8000);

  // Track tool calls and returns for conversation_turn logging
  const toolCalls: Array<{ tool_name: string; arguments: any }> = [];
  const toolReturns: Array<{ tool_name: string; return_value: any }> = [];
  // Track reasoning chain for reasoning models (Gemma 3, etc.)
  const reasoningChain: Array<{ step: number; reasoning: string; timestamp: string }> = [];

  try {
    console.log(`🛜 Sending message to Letta server (agent=${AGENT_ID}): ${JSON.stringify(lettaMessage)}`);
    
    // 🔄 Wrap Letta API call with retry logic
    let response: Stream<LettaStreamingResponse> | null = null;
    try {
      response = await withRetry(
        async () => await client.agents.messages.createStream(AGENT_ID, {
          messages: [lettaMessage]
        }),
        MAX_API_RETRIES, // Controlled by env var (default: 1)
        'User message'
      );
    } catch (streamError) {
      const errorMsg = streamError instanceof Error ? streamError.message : String(streamError);
      
      // 🔧 FIX: ParseError beim Stream-Erstellen erkennen
      // LETTA kann einen error_message senden, den der SDK-Parser nicht verarbeiten kann
      // In diesem Fall versuchen wir einen non-stream Fallback
      const isParseError = /ParseError|Expected.*Received "error_message"|error_message/i.test(errorMsg);
      
      // ❌ CRITICAL FIX: Timeout-Fallback deaktiviert!
      // Warum: Wenn Stream timeout hat, wurde er bereits abgerechnet.
      // Fallback würde zu Doppelabrechnung führen!
      // 
      // Nur bei echten Server-Fehlern (502/503/504) ODER ParseError (wenn KEIN Timeout) Fallback versuchen,
      // NICHT bei Timeouts!
      const isServerError = streamError instanceof Error && 
        (streamError.message.includes('502') || 
         streamError.message.includes('503') || 
         streamError.message.includes('504'));
      
      // ⚠️ WICHTIG: Bei ParseError KEIN Fallback!
      // Warum: ParseError bedeutet, dass LETTA einen error_message gesendet hat.
      // Wir wissen NICHT, ob der Request bereits verarbeitet wurde oder nicht.
      // Fallback würde zu Doppelabrechnung führen!
      // 
      // NUR bei echten Server-Fehlern (502/503/504) Fallback, weil da wissen wir sicher,
      // dass der Request NICHT verarbeitet wurde.
      const isTimeout = /timeout/i.test(errorMsg);
      
      if (isParseError) {
        console.warn('⚠️  ParseError beim Stream-Erstellen - LETTA hat error_message gesendet. KEIN Fallback (verhindert Doppelabrechnung)');
        // ParseError = Request wurde möglicherweise schon verarbeitet, aber Stream konnte nicht geparst werden
        // Wir werfen den Fehler weiter, damit der User eine Fehlermeldung bekommt
        throw streamError;
      }
      
      if (isServerError) {
        console.warn('⚠️  Streaming failed (server error), attempting non-stream fallback:', streamError);
        try {
          // ⚠️ WICHTIG: Kein Retry bei Fallback (würde zu 3x Kosten führen!)
          const nonStreamResponse = await client.agents.messages.create(AGENT_ID, {
            messages: [lettaMessage]
          });
          
          // Extract text from non-stream response
          let agentMessageResponse = '';
          if (nonStreamResponse && typeof nonStreamResponse === 'object') {
            // Try different response shapes
            if ('output' in nonStreamResponse && typeof nonStreamResponse.output === 'string') {
              agentMessageResponse = nonStreamResponse.output;
            } else if ('messages' in nonStreamResponse && Array.isArray(nonStreamResponse.messages)) {
              const lastMessage = nonStreamResponse.messages[nonStreamResponse.messages.length - 1];
              if (lastMessage && 'content' in lastMessage && typeof lastMessage.content === 'string') {
                agentMessageResponse = lastMessage.content;
              }
            } else if ('content' in nonStreamResponse && typeof nonStreamResponse.content === 'string') {
              agentMessageResponse = nonStreamResponse.content;
            }
          }
          
          console.log(`ℹ️ Extracted text length from fallback non-stream: ${agentMessageResponse.length}`);
          
          // Log and return the response
          if (agentMessageResponse && agentMessageResponse.trim()) {
            logBotResponse(agentMessageResponse, channelId, channelName, senderId, senderName, undefined, isDM);
            logConversationTurn(messageContent, agentMessageResponse, conversationContext, [lettaMessage], channelId, channelName, senderId, senderName, AGENT_ID);
          }
          
          return agentMessageResponse || "";
        } catch (fallbackError) {
          console.error('❌ Non-stream fallback also failed:', fallbackError);
          throw streamError; // Throw original stream error
        }
      }
      throw streamError; // Re-throw if not a timeout
    }

    // Process stream and collect tool calls/returns/reasoning
    let agentMessageResponse = "";
    let streamProcessedSuccessfully = false; // Track if processStream completed without errors
    if (response) {
      try {
        agentMessageResponse = await processStream(
          response, 
          discordMessageObject, 
          undefined, 
          typingInterval,
          false,
          undefined,
          toolCalls,  // Pass by reference to collect tool calls
          toolReturns,  // Pass by reference to collect tool returns
          reasoningChain  // Pass by reference to collect reasoning chain
        ) || "";
        streamProcessedSuccessfully = true; // Mark that processStream completed successfully
        console.log(`✅ [sendMessage] processStream completed successfully - returned ${agentMessageResponse.length} chars`);
      } catch (streamProcessError) {
        // 🔧 FIX: Wenn processStream einen Fehler wirft, aber bereits eine Antwort gesammelt wurde,
        // sollten wir die Antwort trotzdem verwenden
        const errorMsg = streamProcessError instanceof Error ? streamProcessError.message : String(streamProcessError);
        console.warn(`⚠️  Error in processStream, but checking if we have collected response: ${errorMsg}`);
        
        // Wenn wir bereits eine Antwort haben (z.B. bei ParseError), verwenden wir sie
        // processStream gibt die Antwort zurück, auch bei ParseError (siehe Zeile 1161)
        // Aber wenn ein Fehler geworfen wird, haben wir die Antwort nicht
        // In diesem Fall werfen wir den Fehler weiter
        throw streamProcessError;
      }
    } else {
      // 🔥 CRITICAL: Wenn response = null, bedeutet das, dass createStream fehlgeschlagen ist
      // In diesem Fall wurde processStream nie aufgerufen, also streamProcessedSuccessfully = false
      // Der Fehler wurde bereits im catch-Block von createStream gefangen und weitergeworfen
      console.log(`⚠️ [sendMessage] createStream failed - response is null, processStream was never called`);
    }
    
    // 🔧 FIX: Check for special marker indicating Letta had an API error
    // We should inform the user about this error with a specific message
    if (agentMessageResponse === "__LETTA_API_ERROR__") {
      console.warn(`⚠️  [sendMessage] Letta API error (llm_api_error) - informing user`);
      return "⚠️ **Letta API Error**\n> Letta hatte einen internen API-Fehler (`llm_api_error`). Bitte versuche es nochmal!";
    }
    
    // 🔧 FIX: Check for special marker indicating Letta sent only reasoning, no assistant_message
    if (agentMessageResponse === "__LETTA_NO_RESPONSE__") {
      console.warn(`⚠️  [sendMessage] Letta sent only reasoning, but NO assistant_message! Informing user...`);
      return "Beep boop. I thought about your message but forgot to respond 🤔 - please send it again!";
    }
    
    // 🔧 FIX: Wenn wir eine Antwort haben (auch bei ParseError), verwenden wir sie!
    // ParseError bedeutet, dass Letta eine Antwort gesendet hat, aber der Stream-Parser fehlgeschlagen ist
    // In diesem Fall hat processStream die Antwort bereits zurückgegeben
    // 
    // 🔥 CRITICAL: Wenn processStream "" zurückgibt, bedeutet das normalerweise, dass die Nachricht bereits
    // via Stream gesendet wurde (hasSentViaStream=true). In diesem Fall sollten wir NICHT "Beep boop" senden!
    if (agentMessageResponse && agentMessageResponse.trim()) {
      // 📝 LOG: Bot response (outgoing) - if we got a response
      logBotResponse(
        agentMessageResponse,
        channelId,
        channelName,
        senderId,
        senderName,
        undefined, // messageId not available here
        isDM
      );
      
      // 📝 LOG: Complete conversation turn (for fine-tuning) - Input + Output + Tools + Reasoning
      logConversationTurn(
        messageContent,  // Full input
        agentMessageResponse,  // Full output
        conversationContext,
        [lettaMessage],  // Message history
        channelId,
        channelName,
        senderId,
        senderName,
        AGENT_ID,
        toolCalls.length > 0 ? toolCalls : undefined,
        toolReturns.length > 0 ? toolReturns : undefined,
        reasoningChain.length > 0 ? reasoningChain : undefined
      );
      
      // ✅ WICHTIG: Wenn wir eine Antwort haben, geben wir sie zurück, auch wenn danach ein Fehler kommt!
      return agentMessageResponse;
    }
    
  // 🔥 CRITICAL FIX: Wenn agentMessageResponse leer ist, könnte das bedeuten:
  // 1. Die Nachricht wurde bereits via Stream gesendet (hasSentViaStream=true) → return "" (keine Duplikate)
  // 2. Es gab wirklich keine Antwort (nur reasoning, keine assistant_message) → sende Nachricht an User!
  // 
  // WICHTIG: Wenn streamProcessedSuccessfully=true, bedeutet das, dass processStream erfolgreich geendet hat.
  // Wenn processStream "" zurückgibt UND erfolgreich war UND hasSentViaStream=true, wurde die Nachricht bereits via Stream gesendet.
  // ABER: Wenn hasSentViaStream=false, hat Letta NUR reasoning gesendet, aber keine assistant_message!
  if (streamProcessedSuccessfully) {
    console.log(`✅ [sendMessage] Stream processed successfully but returned empty - message was already sent via stream. NOT sending "Beep boop".`);
    return "";
  }
  
  // Wenn streamProcessedSuccessfully=false, bedeutet das, dass processStream nie aufgerufen wurde
  // (z.B. weil createStream fehlgeschlagen ist). In diesem Fall sollten wir NICHT "Beep boop" senden,
  // sondern der Fehler wird im catch-Block behandelt.
  console.log(`ℹ️ [sendMessage] processStream was not called (createStream likely failed) - will handle error in catch block`);
  return "";
  } catch (error) {
    // 🔥 CRITICAL FIX: Wenn createStream einen Timeout hat, wird response = null und processStream wird nie aufgerufen.
    // In diesem Fall ist streamProcessedSuccessfully = false, und wir landen hier im catch-Block.
    // ABER: Wenn der Timeout beim createStream passiert, wurde die Nachricht NICHT gesendet!
    // Wir sollten "Beep boop" senden, aber nur wenn es wirklich ein Timeout war.
    // 🔧 FIX: Wenn wir bereits eine Antwort haben (z.B. von processStream bei ParseError),
    // sollten wir sie verwenden statt "Beep boop"
    // ABER: processStream gibt die Antwort zurück, nicht wirft sie
    // Wenn wir hier sind, haben wir keine Antwort
    
    if (error instanceof Error && /timeout/i.test(error.message)) {
      console.error('⚠️  Letta request timed out.');
      // ✅ User bekommt IMMER eine Meldung bei Timeout (auch wenn SURFACE_ERRORS=false)
      return 'Beep boop. I timed out waiting for Letta ⏰ - please try again.';
    }
    
    // Check if it's a retryable error that failed after retries
    const err = error as RetryableError;
    if (err.statusCode && [502, 503, 504].includes(err.statusCode)) {
      console.error(`❌ Letta API unavailable (${err.statusCode}) after retries`);
      // ✅ User bekommt IMMER eine Meldung (auch wenn SURFACE_ERRORS=false)
      const retryCount = MAX_API_RETRIES === 1 ? '1 time' : `${MAX_API_RETRIES} times`;
      return `Beep boop. Letta's API is having issues (${err.statusCode}). I tried ${retryCount} but couldn't get through. Try again in a minute? 🔧`;
    }
    
    console.error(error);
    // ✅ User bekommt IMMER eine Meldung bei Fehlern
    return 'Beep boop. An error occurred while communicating with the Letta server. Please message me again later 👾';
  } finally {
    clearInterval(typingInterval);
  }
}

// Send task execution message to Letta (for task scheduler)
async function sendTaskMessage(
  task: { task_name?: string; description?: string; [key: string]: unknown },
  channel?: { send: (content: string) => Promise<any> },
  discordClient?: any
) {
  if (!AGENT_ID) {
    console.error('Error: LETTA_AGENT_ID is not set');
    return SURFACE_ERRORS
      ? `Beep boop. My configuration is not set up properly. Please message me after I get fixed 👾`
      : "";
  }

  const taskName = String(task.task_name || 'Unnamed Task');
  const actionType = String(task.action_type || '');
  const actionTarget = task.action_target;
  
  // 🔍 DEBUG: Log actionType to verify it's being passed correctly
  console.log(`🔍 [sendTaskMessage] actionType="${actionType}", actionTarget="${actionTarget}"`);
  
  // Extract channel info for logging
  let channelId: string | undefined;
  let channelName: string | undefined;
  if (channel && 'id' in channel && typeof channel.id === 'string') {
    channelId = channel.id;
  }
  if (channel && 'name' in channel && typeof (channel as any).name === 'string') {
    channelName = (channel as any).name;
  }
  
  // 🔍 Validate action_target: Must be a valid Discord Snowflake ID (numeric string, 17-19 digits)
  // If action_target is "self_task" (string) or invalid, treat it as not set
  const isValidSnowflake = (id: any): boolean => {
    if (!id || typeof id !== 'string') return false;
    // Discord Snowflake IDs are numeric strings with 17-19 digits
    return /^\d{17,19}$/.test(id);
  };
  
  const hasValidTarget = isValidSnowflake(actionTarget);
  
  // Determine if reasoning should be shown:
  // - self_task that lands in Heartbeat Log Channel -> show reasoning (regardless of how it got there)
  // - self_task in other channels -> NO reasoning
  const HEARTBEAT_LOG_CHANNEL_ID = process.env.HEARTBEAT_LOG_CHANNEL_ID || '';
  const DEFAULT_HEARTBEAT_LOG_CHANNEL_ID = process.env.DEFAULT_HEARTBEAT_CHANNEL_ID || '';
  const isHeartbeatLogChannel = !!(channel && 'id' in channel && (
    (HEARTBEAT_LOG_CHANNEL_ID && channel.id === HEARTBEAT_LOG_CHANNEL_ID) || 
    (DEFAULT_HEARTBEAT_LOG_CHANNEL_ID && channel.id === DEFAULT_HEARTBEAT_LOG_CHANNEL_ID)
  ));
  const isHeartbeatLogTask = actionType === 'self_task' && isHeartbeatLogChannel;
  
  // 🔥 FIX (Jan 2025): Keep action_target for user_reminder tasks
  // targetChannel is already set to DM channel in taskScheduler.ts
  // processStream will send directly to targetChannel, but Letta should still see action_target
  // in case it uses tools (which we intercept and redirect to targetChannel anyway)
  let taskDataForLetta = { ...task };
  // Don't remove action_target - Letta needs it to know where to send (even if we intercept tools)
  
  const taskContent = `[⏰ SCHEDULED TASK TRIGGERED]\n\nTask: ${taskName}\n\n${actionType === 'user_reminder' ? `📩 This is a reminder for user ${actionTarget}. The answer to this message will directly land in the user DM's.\n\n` : ''}Task Data: ${JSON.stringify(taskDataForLetta, null, 2)}`;
  
  const lettaMessage = {
    role: "user" as const,
    content: taskContent
  };

  // 📝 LOG: Task execution
  logTask(taskName, actionType, taskContent, channelId, channelName);

  try {
    console.log(`🛜 Sending task to Letta server (agent=${AGENT_ID})`);
    
    // ✅ SEQUENTIAL: Use queue to prevent parallel API calls with Heartbeats and other Tasks
    let response: Stream<LettaStreamingResponse> | null = null;
    try {
      response = await apiQueue.enqueue(
        async () => {
          // 🔄 Wrap Letta API call with retry logic
          return await withRetry(
            async () => await client.agents.messages.createStream(AGENT_ID, {
              messages: [lettaMessage]
            }),
            MAX_API_RETRIES, // Controlled by env var (default: 1)
            'Scheduled task'
          );
        },
        `Scheduled task: ${taskName}`
      ) as Stream<LettaStreamingResponse>;
    } catch (streamError) {
      const errorMsg = streamError instanceof Error ? streamError.message : String(streamError);
      
      // 🔧 FIX: ParseError beim Stream-Erstellen erkennen (auch für Tasks)
      const isParseError = /ParseError|Expected.*Received "error_message"|error_message/i.test(errorMsg);
      
      // ❌ CRITICAL FIX: Timeout-Fallback deaktiviert für Tasks!
      // Warum: Wenn Stream timeout hat, wurde er bereits abgerechnet.
      // Fallback würde zu Doppelabrechnung führen!
      const isServerError = streamError instanceof Error && 
        (streamError.message.includes('502') || 
         streamError.message.includes('503') || 
         streamError.message.includes('504'));
      
      // ⚠️ WICHTIG: Bei ParseError KEIN Fallback (auch für Tasks)!
      // Warum: ParseError bedeutet, dass LETTA einen error_message gesendet hat.
      // Wir wissen NICHT, ob der Request bereits verarbeitet wurde oder nicht.
      // Fallback würde zu Doppelabrechnung führen!
      const isTimeout = /timeout/i.test(errorMsg);
      
      if (isParseError) {
        console.warn('⚠️  ParseError beim Stream-Erstellen (task) - LETTA hat error_message gesendet. KEIN Fallback (verhindert Doppelabrechnung)');
        throw streamError;
      }
      
      if (isServerError) {
        console.warn('⚠️  Streaming failed (task, server error), attempting non-stream fallback:', streamError);
        try {
          // ⚠️ WICHTIG: Kein Retry bei Fallback (würde zu 3x Kosten führen!)
          const nonStreamResponse = await client.agents.messages.create(AGENT_ID, {
            messages: [lettaMessage]
          });
          
          // Extract text from non-stream response
          let taskResponse = '';
          if (nonStreamResponse && typeof nonStreamResponse === 'object') {
            // Try different response shapes
            if ('output' in nonStreamResponse && typeof nonStreamResponse.output === 'string') {
              taskResponse = nonStreamResponse.output;
            } else if ('messages' in nonStreamResponse && Array.isArray(nonStreamResponse.messages)) {
              const lastMessage = nonStreamResponse.messages[nonStreamResponse.messages.length - 1];
              if (lastMessage && 'content' in lastMessage && typeof lastMessage.content === 'string') {
                taskResponse = lastMessage.content;
              }
            } else if ('content' in nonStreamResponse && typeof nonStreamResponse.content === 'string') {
              taskResponse = nonStreamResponse.content;
            }
          }
          
          console.log(`ℹ️ Extracted text length from fallback non-stream (task): ${taskResponse.length}`);
          
          // Log and return the response
          if (taskResponse && taskResponse.trim()) {
            logTaskResponse(taskResponse, taskName, channelId, channelName);
          }
          
          return taskResponse || "";
        } catch (fallbackError) {
          console.error('❌ Non-stream fallback also failed (task):', fallbackError);
          throw streamError; // Throw original stream error
        }
      }
      throw streamError; // Re-throw if not a timeout
    }

    if (response) {
      // Pass actionType to processStream so it knows not to redirect task messages
      console.log(`🔍 [sendTaskMessage] Passing actionType="${actionType}" to processStream`);
      try {
        const taskResponse = (await processStream(response, channel, discordClient, undefined, isHeartbeatLogTask, actionType)) || "";
        
        // 📝 LOG: Task response
        if (taskResponse && taskResponse.trim()) {
          logTaskResponse(taskResponse, taskName, channelId, channelName);
        }
        
        return taskResponse;
      } catch (streamError) {
        // 🔧 FIX: Wenn processStream einen Fehler hat, aber bereits Daten gesammelt wurden,
        // sollten wir die gesammelten Daten zurückgeben statt "beep boop"
        const errMsg = streamError instanceof Error ? streamError.message : String(streamError);
        
        // Bei Timeout während Stream-Verarbeitung: Stream hat vielleicht schon Daten gesammelt
        // processStream gibt bereits gesammelte Daten zurück bei Timeout, also sollte das nicht passieren
        // Aber zur Sicherheit: Wenn es ein Timeout ist, versuchen wir trotzdem weiter
        if (/timeout/i.test(errMsg)) {
          console.warn('⚠️ Stream processing timed out in task - this should have been handled by processStream');
          // processStream sollte bereits gesammelte Daten zurückgeben, aber falls nicht:
          return 'Beep boop. I timed out waiting for Letta ⏰ – please try again.';
        }
        
        // Andere Fehler: weiterwerfen
        throw streamError;
      }
    }

    return "";
  } catch (error) {
    if (error instanceof Error && /timeout/i.test(error.message)) {
      console.error('⚠️  Letta request timed out.');
      // ✅ User bekommt IMMER eine Meldung bei Timeout (auch wenn SURFACE_ERRORS=false)
      return 'Beep boop. I timed out waiting for Letta ⏰ – please try again.';
    }
    
    // Check if it's a retryable error that failed after retries
    const err = error as RetryableError;
    if (err.statusCode && [502, 503, 504].includes(err.statusCode)) {
      console.error(`❌ Letta API unavailable (${err.statusCode}) after retries - task execution failed`);
      // ✅ User bekommt IMMER eine Meldung (auch wenn SURFACE_ERRORS=false)
      const retryCount = MAX_API_RETRIES === 1 ? '1 time' : `${MAX_API_RETRIES} times`;
      return `Beep boop. Letta's API is down (${err.statusCode}). I tried ${retryCount} but couldn't get through. Task will try on next cycle 🔧`;
    }
    
    console.error(error);
    // ✅ User bekommt IMMER eine Meldung bei Fehlern
    return 'Beep boop. An error occurred while communicating with the Letta server. Please message me again later 👾';
  }
}

export { sendMessage, sendTimerMessage, sendTaskMessage, MessageType };

// Temporary placeholder functions for server.ts compatibility
export function getLettaStats() { return "Stats not implemented"; }
export function getDailyStats() { return "Daily stats not implemented"; }
