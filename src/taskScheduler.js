"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.startTaskCheckerLoop = startTaskCheckerLoop;
const axios_1 = __importDefault(require("axios"));
const messages_1 = require("./messages");
const DISCORD_TOKEN = process.env.DISCORD_TOKEN || '';
const TASKS_CHANNEL_ID = process.env.TASKS_CHANNEL_ID || '';
const DISCORD_CHANNEL_ID = process.env.DISCORD_CHANNEL_ID || ''; // Default channel for task responses
const HEARTBEAT_LOG_CHANNEL_ID = process.env.HEARTBEAT_LOG_CHANNEL_ID; // Heartbeat log channel for self_tasks
const TIMEZONE = process.env.TIMEZONE || 'Europe/Berlin';
async function readTasksFromChannel() {
    try {
        if (!DISCORD_TOKEN || !TASKS_CHANNEL_ID)
            return [];
        const url = `https://discord.com/api/v10/channels/${TASKS_CHANNEL_ID}/messages?limit=100`;
        const headers = { Authorization: `Bot ${DISCORD_TOKEN}` };
        const response = await axios_1.default.get(url, { headers, timeout: 10000 });
        if (response.status !== 200) {
            console.warn(`❌ Failed to read tasks channel: ${response.status}`);
            return [];
        }
        const messages = response.data || [];
        const tasks = [];
        for (const msg of messages) {
            const content = String(msg?.content || '');
            let jsonStr = content;
            if (content.includes('```json')) {
                const parts = content.split('```json');
                if (parts[1])
                    jsonStr = parts[1].split('```')[0].trim();
            }
            else if (content.includes('```')) {
                jsonStr = content.split('```')[1]?.split('```')[0]?.trim() || content;
            }
            try {
                const task = JSON.parse(jsonStr);
                if (task && typeof task === 'object') {
                    task.message_id = msg.id;
                    tasks.push(task);
                }
            }
            catch (_e) {
                // ignore non-JSON messages
            }
        }
        return tasks;
    }
    catch (e) {
        // Extract error message cleanly
        const errorMsg = e instanceof Error ? e.message : String(e);
        const isTimeout = errorMsg.includes('timeout') || errorMsg.includes('ECONNABORTED');
        if (isTimeout) {
            console.log(`⚠️  Discord API timeout when reading tasks (non-critical, retrying next cycle)`);
        }
        else {
            console.error(`❌ Error reading tasks: ${errorMsg}`);
        }
        return [];
    }
}
/**
 * Parse a time string (HH:MM) in configured timezone and convert to UTC Date
 * @param timeStr Time in format "HH:MM" (e.g. "14:00")
 * @param referenceDate Reference date in UTC to attach the time to
 * @returns Date in UTC with the specified time (interpreted in configured timezone)
 */
function parseTimeInBerlinThenUTC(timeStr, referenceDate) {
    const [hour, minute] = timeStr.split(':').map(Number);
    // Get the date parts in configured timezone for the reference date
    const berlinDateStr = referenceDate.toLocaleString('en-CA', {
        timeZone: TIMEZONE,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    });
    // berlinDateStr is "YYYY-MM-DD"
    // Create ISO string with the desired Berlin time
    const berlinISO = `${berlinDateStr}T${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00`;
    // We need to find the UTC time that corresponds to this Berlin time
    // Strategy: Try different UTC times and see which one gives us the desired Berlin time
    // Start with an estimate: Berlin is typically UTC+1 (CET) or UTC+2 (CEST)
    // So if we want 14:00 Berlin, try 13:00 UTC (CET) or 12:00 UTC (CEST)
    // Try UTC+1 first (CET - winter time)
    // Create date string with timezone offset: if Berlin is UTC+1, then 14:00 Berlin = 13:00 UTC
    let testUTC = new Date(`${berlinISO}+01:00`);
    let testBerlin = new Intl.DateTimeFormat('en-US', {
        timeZone: TIMEZONE,
        hour: '2-digit',
        minute: '2-digit',
        hour12: false
    }).formatToParts(testUTC);
    let testHour = parseInt(testBerlin.find(p => p.type === 'hour')?.value || '0', 10);
    let testMin = parseInt(testBerlin.find(p => p.type === 'minute')?.value || '0', 10);
    // If it doesn't match, try UTC+2 (CEST - summer time)
    if (testHour !== hour || testMin !== minute) {
        testUTC = new Date(`${berlinISO}+02:00`);
        testBerlin = new Intl.DateTimeFormat('en-US', {
            timeZone: 'Europe/Berlin',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        }).formatToParts(testUTC);
        testHour = parseInt(testBerlin.find(p => p.type === 'hour')?.value || '0', 10);
        testMin = parseInt(testBerlin.find(p => p.type === 'minute')?.value || '0', 10);
    }
    // If still doesn't match, do binary search or iterative adjustment
    if (testHour !== hour || testMin !== minute) {
        // Calculate difference and adjust
        const hourDiff = hour - testHour;
        const minDiff = minute - testMin;
        testUTC.setUTCHours(testUTC.getUTCHours() + hourDiff);
        testUTC.setUTCMinutes(testUTC.getUTCMinutes() + minDiff);
    }
    return testUTC;
}
/**
 * Parse an ISO datetime string without timezone as Berlin time and convert to UTC
 * @param isoStr ISO string like "2025-01-15T20:30:00" (without timezone)
 * @returns Date in UTC
 */
function parseNaiveDateTimeAsBerlin(isoStr) {
    // Remove microseconds if present
    const cleanStr = isoStr.replace(/\.\d+$/, '');
    // Extract date and time parts
    const match = cleanStr.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})$/);
    if (!match) {
        // Fallback: try to parse as-is
        return new Date(cleanStr + 'Z');
    }
    const [, dateStr, hourStr, minuteStr] = match;
    // Use the existing parseTimeInBerlinThenUTC function
    // Create a reference date from the date part
    const refDate = new Date(dateStr + 'T00:00:00Z');
    return parseTimeInBerlinThenUTC(`${hourStr}:${minuteStr}`, refDate);
}
function checkDueTasks(tasks) {
    // Always use UTC for comparison to avoid timezone issues
    // next_run from Python tool is stored without timezone (naive datetime)
    // We interpret it as Berlin time and convert to UTC for comparison
    const now = new Date(); // This is already in UTC internally (JavaScript Date is always UTC)
    const due = [];
    for (const t of tasks) {
        if (t.active === false)
            continue;
        const nextRunStr = t.next_run;
        if (!nextRunStr)
            continue;
        let nextRun;
        // Check if it has timezone info (Z, +HH:MM, or -HH:MM after the time part)
        if (nextRunStr.includes('Z') || nextRunStr.match(/[+-]\d{2}:\d{2}$/)) {
            // Has timezone info - parse normally (already in UTC)
            nextRun = new Date(nextRunStr);
        }
        else {
            // No timezone info - interpret as Berlin time and convert to UTC
            // Format: "2025-01-15T20:30:00" -> treat as 20:30 Berlin time
            nextRun = parseNaiveDateTimeAsBerlin(nextRunStr);
        }
        if (!Number.isNaN(nextRun.getTime()) && nextRun <= now) {
            console.log(`🗓️  ✅ Task "${t.task_name}" is DUE! next_run=${nextRun.toISOString()}, now=${now.toISOString()}`);
            due.push(t);
        }
    }
    return due;
}
async function triggerLetta(task, client) {
    const taskName = String(task.task_name || 'Unnamed');
    const taskMsgId = task.message_id || 'no-id';
    const timestamp = new Date().toISOString();
    try {
        console.log(`🗓️  🚀 [${timestamp}] Triggering Letta for task: "${taskName}" (msg_id=${taskMsgId})`);
        console.log(`🗓️  📋 Task config: action_type="${task.action_type}", action_target="${task.action_target}"`);
        // Try to get a target channel for Letta's response
        let targetChannel;
        if (client) {
            // For channel_post: use the specified channel
            if (task.action_target && task.action_type === 'channel_post') {
                try {
                    const ch = await client.channels.fetch(task.action_target);
                    if (ch && 'send' in ch)
                        targetChannel = ch;
                    console.log(`🗓️  📢 Task action_type='channel_post' - using channel ${task.action_target}`);
                }
                catch { }
            }
            // For user_reminder: open DM channel to user (like "Guten Morgen" message)
            // This sends the reminder directly to the user's DMs
            if (task.action_target && task.action_type === 'user_reminder') {
                console.log(`🗓️  📩 Task action_type='user_reminder' - opening DM to user ${task.action_target}`);
                try {
                    const user = await client.users.fetch(task.action_target);
                    if (user) {
                        const dmChannel = await user.createDM();
                        targetChannel = dmChannel; // DM channels also have .send()
                        console.log(`🗓️  ✅ DM channel opened for user ${user.username}`);
                    }
                }
                catch (e) {
                    console.error(`🗓️  ❌ Failed to open DM channel for user ${task.action_target}:`, e);
                }
            }
            // For self_task: use action_target if specified and valid, otherwise Heartbeat Log Channel
            if (task.action_type === 'self_task') {
                // 🔍 Validate action_target: Must be a valid Discord Snowflake ID (numeric string, 17-19 digits)
                // If action_target is "self_task" (string) or invalid, treat it as not set
                const isValidSnowflake = (id) => {
                    if (!id || typeof id !== 'string')
                        return false;
                    // Ignore "self_task" string explicitly
                    if (id === 'self_task')
                        return false;
                    // Discord Snowflake IDs are numeric strings with 17-19 digits
                    return /^\d{17,19}$/.test(id);
                };
                const actionTarget = task.action_target;
                const hasValidTarget = isValidSnowflake(actionTarget);
                // 🔥 FIX (Jan 2025): Default to Heartbeat Log Channel if HEARTBEAT_LOG_CHANNEL_ID is undefined
                // Use action_target if valid, otherwise fallback to Heartbeat Log Channel
                const DEFAULT_HEARTBEAT_LOG_CHANNEL_ID = process.env.DEFAULT_HEARTBEAT_CHANNEL_ID || '';
                const heartbeatLogChannelId = HEARTBEAT_LOG_CHANNEL_ID || DEFAULT_HEARTBEAT_LOG_CHANNEL_ID;
                const preferredChannelId = hasValidTarget
                    ? actionTarget
                    : (heartbeatLogChannelId || DISCORD_CHANNEL_ID);
                const isCustomTarget = hasValidTarget;
                if (preferredChannelId) {
                    try {
                        const ch = await client.channels.fetch(preferredChannelId);
                        if (ch && 'send' in ch) {
                            targetChannel = ch;
                            const channelType = isCustomTarget ? 'custom target' : 'heartbeat log';
                            console.log(`🗓️  🤖 Task action_type='self_task' - using channel ${preferredChannelId} (${channelType})`);
                        }
                    }
                    catch (e) {
                        const errorMsg = e instanceof Error ? e.message : String(e);
                        console.error(`🗓️  ❌ Failed to fetch self_task channel ${preferredChannelId}: ${errorMsg}`);
                        // If custom target failed, try fallback to heartbeat log
                        if (isCustomTarget && heartbeatLogChannelId && heartbeatLogChannelId !== preferredChannelId) {
                            console.log(`🗓️  🔄 Attempting fallback to heartbeat log channel: ${heartbeatLogChannelId}`);
                            try {
                                const fallbackCh = await client.channels.fetch(heartbeatLogChannelId);
                                if (fallbackCh && 'send' in fallbackCh) {
                                    targetChannel = fallbackCh;
                                    console.log(`🗓️  ✅ Using fallback channel successfully`);
                                }
                            }
                            catch (fallbackErr) {
                                console.error(`🗓️  ❌ Fallback to heartbeat log also failed`);
                            }
                        }
                        // If preferredChannelId was heartbeat log and failed, try default
                        else if (!isCustomTarget && preferredChannelId !== DEFAULT_HEARTBEAT_LOG_CHANNEL_ID && preferredChannelId !== heartbeatLogChannelId && DEFAULT_HEARTBEAT_LOG_CHANNEL_ID) {
                            console.log(`🗓️  🔄 Attempting fallback to default heartbeat log channel: ${DEFAULT_HEARTBEAT_LOG_CHANNEL_ID}`);
                            try {
                                const fallbackCh = await client.channels.fetch(DEFAULT_HEARTBEAT_LOG_CHANNEL_ID);
                                if (fallbackCh && 'send' in fallbackCh) {
                                    targetChannel = fallbackCh;
                                    console.log(`🗓️  ✅ Using default heartbeat log channel successfully`);
                                }
                            }
                            catch (fallbackErr) {
                                console.error(`🗓️  ❌ Default heartbeat log channel also failed`);
                            }
                        }
                    }
                }
                else {
                    // Last resort: try default heartbeat log channel
                    if (DEFAULT_HEARTBEAT_LOG_CHANNEL_ID) {
                        console.log(`🗓️  ⚠️  No channel ID available, trying default heartbeat log channel: ${DEFAULT_HEARTBEAT_LOG_CHANNEL_ID}`);
                        try {
                            const fallbackCh = await client.channels.fetch(DEFAULT_HEARTBEAT_LOG_CHANNEL_ID);
                            if (fallbackCh && 'send' in fallbackCh) {
                                targetChannel = fallbackCh;
                                console.log(`🗓️  ✅ Using default heartbeat log channel as last resort`);
                            }
                        }
                        catch (fallbackErr) {
                            console.error(`🗓️  ❌ Default heartbeat log channel failed: ${fallbackErr instanceof Error ? fallbackErr.message : fallbackErr}`);
                        }
                    }
                }
            }
            // Final fallback to DISCORD_CHANNEL_ID (when no channel specified or all attempts failed)
            if (!targetChannel && DISCORD_CHANNEL_ID) {
                try {
                    const ch = await client.channels.fetch(DISCORD_CHANNEL_ID);
                    if (ch && 'send' in ch) {
                        targetChannel = ch;
                        console.log(`🗓️  📍 Using default channel as last resort: ${DISCORD_CHANNEL_ID}`);
                    }
                }
                catch { }
            }
        }
        // Send task to Grok via messages.ts infrastructure
        const taskPrompt = typeof task === 'string' ? task : task.content || task.task || taskName;
        const taskResponse = await (0, messages_1.sendTaskMessage)(taskName, taskPrompt);
        // 🔧 FIX: Check if we got a response from Letta
        if (!taskResponse || taskResponse.trim() === "") {
            console.warn(`🗓️  ⚠️  [${new Date().toISOString()}] Task triggered but Letta sent NO response: "${taskName}" (msg_id=${taskMsgId})`);
            // Send error message to channel
            if (targetChannel) {
                try {
                    await targetChannel.send(`⚠️ **Task Triggered: ${taskName}**\n> Letta empfing den Task, aber sendete keine Antwort. Möglicherweise:\n> • Response zu groß\n> • Stream-Timeout\n> • Letta benutzte nur archival_memory\n\nBitte check das Letta Dashboard für Details.`);
                }
                catch (sendError) {
                    console.error(`🗓️  ❌ Failed to send "no response" warning to channel:`, sendError);
                }
            }
            return false; // Task triggered but no response = failure
        }
        console.log(`🗓️  ✅ [${new Date().toISOString()}] Triggered Letta successfully: "${taskName}" (msg_id=${taskMsgId}) - received ${taskResponse.length} chars`);
        return true;
    }
    catch (e) {
        console.error(`🗓️  ❌ [${new Date().toISOString()}] Failed to trigger Letta for "${taskName}" (msg_id=${taskMsgId}):`, e?.message || e);
        return false;
    }
}
async function deleteTaskMessage(messageId) {
    try {
        if (!messageId || !DISCORD_TOKEN || !TASKS_CHANNEL_ID) {
            console.log(`🗓️  ⏭️  Skipping delete (missing params): msg_id=${messageId || 'none'}`);
            return false;
        }
        const timestamp = new Date().toISOString();
        console.log(`🗓️  🗑️  [${timestamp}] Attempting to delete task message: ${messageId}`);
        const url = `https://discord.com/api/v10/channels/${TASKS_CHANNEL_ID}/messages/${messageId}`;
        const headers = { Authorization: `Bot ${DISCORD_TOKEN}` };
        const resp = await axios_1.default.delete(url, { headers, timeout: 10000 });
        if (resp.status === 204) {
            console.log(`🗓️  ✅ [${new Date().toISOString()}] Successfully deleted task message: ${messageId}`);
            return true;
        }
        console.warn(`🗓️  ⚠️  [${new Date().toISOString()}] Failed to delete msg ${messageId}: HTTP ${resp.status}`);
        return false;
    }
    catch (e) {
        // Check if it's a 404 (already deleted) - this is actually OK in our dedup scenario
        if (e?.response?.status === 404) {
            console.log(`🗓️  ℹ️  [${new Date().toISOString()}] Task message ${messageId} already deleted (404) - this is OK`);
            return false; // Don't retry recurring update if message doesn't exist
        }
        console.error(`🗓️  ❌ [${new Date().toISOString()}] Error deleting message ${messageId}:`, e?.message || e);
        return false;
    }
}
async function updateRecurringTask(task) {
    try {
        if (!DISCORD_TOKEN || !TASKS_CHANNEL_ID)
            return false;
        const schedule = String(task.schedule || '');
        const now = new Date();
        let newNext = new Date(now);
        const hasTimeField = task.time && /^\d{1,2}:\d{2}$/.test(task.time);
        // Calculate next run based on schedule type
        if (schedule === 'secondly') {
            newNext.setUTCSeconds(now.getUTCSeconds() + 1);
        }
        else if (schedule === 'minutely') {
            newNext.setUTCMinutes(now.getUTCMinutes() + 1);
        }
        else if (schedule === 'hourly') {
            newNext.setUTCHours(now.getUTCHours() + 1);
        }
        else if (schedule === 'daily') {
            newNext.setUTCDate(now.getUTCDate() + 1);
            // If task has time field (e.g. "07:30"), use it as Berlin time
            if (hasTimeField && task.time) {
                newNext = parseTimeInBerlinThenUTC(task.time, newNext);
            }
        }
        else if (schedule === 'weekly') {
            newNext.setUTCDate(now.getUTCDate() + 7);
            // If task has time field (e.g. "18:00"), use it as Berlin time
            if (hasTimeField && task.time) {
                newNext = parseTimeInBerlinThenUTC(task.time, newNext);
            }
        }
        else if (schedule === 'monthly') {
            newNext.setUTCMonth(now.getUTCMonth() + 1);
            // If task has time field, use it as Berlin time
            if (hasTimeField && task.time) {
                newNext = parseTimeInBerlinThenUTC(task.time, newNext);
            }
        }
        else if (schedule === 'yearly') {
            newNext.setUTCFullYear(now.getUTCFullYear() + 1);
            // If task has time field, use it as Berlin time
            if (hasTimeField && task.time) {
                newNext = parseTimeInBerlinThenUTC(task.time, newNext);
            }
        }
        else if (/^every_\d+_minutes$/.test(schedule)) {
            const minutes = parseInt(schedule.split('_')[1] || '0', 10) || 0;
            newNext.setUTCMinutes(now.getUTCMinutes() + minutes);
        }
        else if (/^every_\d+_hours$/.test(schedule)) {
            const hours = parseInt(schedule.split('_')[1] || '0', 10) || 0;
            newNext.setUTCHours(now.getUTCHours() + hours);
        }
        else if (/^every_\d+_days$/.test(schedule)) {
            const days = parseInt(schedule.split('_')[1] || '0', 10) || 0;
            newNext.setUTCDate(now.getUTCDate() + days);
            // If task has time field, use it as Berlin time
            if (hasTimeField && task.time) {
                newNext = parseTimeInBerlinThenUTC(task.time, newNext);
            }
        }
        else if (/^every_\d+_weeks$/.test(schedule)) {
            const weeks = parseInt(schedule.split('_')[1] || '0', 10) || 0;
            newNext.setUTCDate(now.getUTCDate() + (weeks * 7));
            // If task has time field, use it as Berlin time
            if (hasTimeField && task.time) {
                newNext = parseTimeInBerlinThenUTC(task.time, newNext);
            }
        }
        else if (schedule.startsWith('tomorrow_at_')) {
            // Handle "tomorrow_at_07:30" format - extract time from schedule string
            const timeStr = schedule.replace('tomorrow_at_', '');
            if (/^\d{1,2}:\d{2}$/.test(timeStr)) {
                newNext.setUTCDate(now.getUTCDate() + 1);
                newNext = parseTimeInBerlinThenUTC(timeStr, newNext);
            }
            else {
                // Fallback: just add one day
                newNext.setUTCDate(now.getUTCDate() + 1);
            }
        }
        else if (schedule.startsWith('today_at_')) {
            // Handle "today_at_14:00" format - extract time from schedule string
            const timeStr = schedule.replace('today_at_', '');
            if (/^\d{1,2}:\d{2}$/.test(timeStr)) {
                // Keep same date, just set time
                newNext = parseTimeInBerlinThenUTC(timeStr, newNext);
                // If time already passed today, schedule for tomorrow
                if (newNext <= now) {
                    newNext.setUTCDate(newNext.getUTCDate() + 1);
                    newNext = parseTimeInBerlinThenUTC(timeStr, newNext);
                }
            }
            else {
                // Fallback: use current time
                newNext = new Date(now);
            }
        }
        else {
            console.warn(`🗓️  ⚠️  Unknown recurring schedule: ${schedule}`);
            return false;
        }
        const updated = { ...task };
        delete updated.message_id; // do not carry over old message id
        updated.next_run = newNext.toISOString();
        updated.active = true;
        const url = `https://discord.com/api/v10/channels/${TASKS_CHANNEL_ID}/messages`;
        const headers = { Authorization: `Bot ${DISCORD_TOKEN}`, 'Content-Type': 'application/json' };
        const taskType = 'Recurring';
        const actionType = String(updated.action_type || '');
        const actionTarget = String(updated.action_target || '');
        const actionDesc = actionType === 'user_reminder'
            ? `Discord DM → User ${actionTarget}`
            : actionType === 'channel_post'
                ? `Discord Channel → ${actionTarget}`
                : 'Internal Agent Task';
        const nextRunPretty = updated.next_run
            ? new Date(updated.next_run).toISOString().slice(0, 16).replace('T', ' ')
            : '';
        let formattedMessage = `📋 **Task: ${String(updated.task_name || '')}**\n` +
            `├─ Description: ${String(updated.description || '')}\n` +
            `├─ Schedule: ${String(updated.schedule || '')} (${taskType})\n` +
            `├─ Next Run: ${nextRunPretty}\n` +
            `└─ Action: ${actionDesc}\n\n` +
            `\`\`\`json\n${JSON.stringify(updated, null, 2)}\n\`\`\``;
        if (formattedMessage.length > 1900) {
            const jsonPreview = JSON.stringify(updated).slice(0, 1500);
            formattedMessage = `📋 **Task: ${String(updated.task_name || '')}**\n` +
                `Next Run: ${nextRunPretty}\n\n` +
                `\`\`\`json\n${jsonPreview}\n\`\`\``;
        }
        const payload = { content: formattedMessage };
        const resp = await axios_1.default.post(url, payload, { headers, timeout: 10000 });
        if (resp.status === 200 || resp.status === 201) {
            console.log(`🗓️  ✅ Updated recurring task: ${String(task.task_name || '')}, next run: ${newNext.toISOString()}`);
            return true;
        }
        console.warn(`🗓️  ❌ Failed to update recurring task: ${resp.status}`);
        return false;
    }
    catch (e) {
        console.error('🗓️  ❌ Error updating recurring task:', e);
        return false;
    }
}
// Singleton guard to prevent multiple loops
let isTaskCheckerRunning = false;
const processingTasks = new Set(); // Track tasks being processed
function startTaskCheckerLoop(client) {
    // SECURITY: Prevent multiple task checker loops from running in parallel
    if (isTaskCheckerRunning) {
        console.warn('⚠️  🗓️  Task Scheduler already running! Ignoring duplicate start call.');
        return;
    }
    isTaskCheckerRunning = true;
    console.log('🗓️  Task Scheduler started (singleton mode)');
    const LOOP_MS = 60000;
    async function tick() {
        try {
            const tasks = await readTasksFromChannel();
            if (tasks.length) {
                console.log(`🗓️  Found ${tasks.length} task(s) in channel`);
                const due = checkDueTasks(tasks);
                if (due.length) {
                    console.log(`🗓️  ${due.length} task(s) due for execution`);
                    // ✅ SEQUENTIAL PROCESSING: Process tasks one at a time to avoid parallel API calls
                    // This prevents multiple simultaneous requests that could cause timeouts and 7x billing
                    for (const t of due) {
                        const name = String(t.task_name || '');
                        const messageId = t.message_id || '';
                        const oneTime = !!t.one_time;
                        // SECURITY: Deduplication - prevent processing the same task twice
                        const taskKey = messageId || `${name}_${t.next_run}`;
                        if (processingTasks.has(taskKey)) {
                            console.log(`🗓️  ⏭️  Skipping task already being processed: ${name} (key=${taskKey})`);
                            continue;
                        }
                        // Mark task as being processed
                        processingTasks.add(taskKey);
                        console.log(`🗓️  🔒 Processing task: ${name} (key=${taskKey})`);
                        try {
                            // ✅ SEQUENTIAL: await each task before processing next one
                            // This ensures only one API call at a time for tasks
                            const ok = await triggerLetta(t, client);
                            if (ok) {
                                const deleted = await deleteTaskMessage(messageId);
                                if (deleted) {
                                    if (!oneTime) {
                                        await updateRecurringTask(t);
                                    }
                                    else {
                                        console.log(`🗓️  🗑️  One-time task completed and deleted: ${name}`);
                                    }
                                }
                            }
                        }
                        finally {
                            // Always remove from processing set when done
                            processingTasks.delete(taskKey);
                            console.log(`🗓️  🔓 Released task: ${name} (key=${taskKey})`);
                        }
                    }
                }
            }
        }
        catch (e) {
            console.error('🗓️  ❌ Error in task checker:', e);
        }
        finally {
            setTimeout(tick, LOOP_MS);
        }
    }
    setTimeout(tick, 2000); // small delay on start
}
