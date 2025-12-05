# Spotify "Now Playing" Integration for Discord Bot Heartbeats

**Author:** Discord-Letta Bot Contributors  
**Date:** October 13, 2025  
**Status:** ✅ Fully Implemented & Production Ready

---

## Overview

This feature enriches the bot's autonomous **heartbeat messages** by automatically including Spotify "Now Playing" information when available. Every time the heartbeat fires, the bot checks what music the configured user is listening to on Spotify and includes it in the system message sent to the Letta agent.

### What It Does

- ✅ Checks Spotify API at every heartbeat trigger
- ✅ Displays current track, artist, and playback progress (e.g., "2:34 / 4:12")
- ✅ Gracefully handles "nothing playing" state (silently omits Spotify section)
- ✅ Fails silently if Spotify credentials are missing or API is unavailable
- ✅ Uses OAuth refresh token flow for secure, long-lived authentication
- ✅ Zero external dependencies (uses native Node.js `https` module)

---

## Implementation Details

### 1. Spotify API Integration (`messages.ts`)

We added a helper function that:
1. Uses **refresh token** to get a fresh access token from Spotify
2. Calls `/v1/me/player/currently-playing` to check playback status
3. Returns formatted track info or `null` if nothing is playing

#### Code Location
```
running Discord bot/src/messages.ts
Lines 23-115
```

#### Function Signature
```typescript
async function getSpotifyNowPlaying(): Promise<string | null>
```

#### Return Format (when music is playing)
```
🎵 Song Name
🎤 Artist Name(s)
⏱️ 2:34 / 4:12
```

#### Return Value (when nothing is playing)
```
null
```

---

### 2. Heartbeat Message Integration

The heartbeat system calls `getSpotifyNowPlaying()` and conditionally appends the result to the system message.

#### Code Location
```
running Discord bot/src/messages.ts
Lines 223-233 (inside sendTimerMessage function)
```

#### Logic Flow
```typescript
// Check Spotify "Now Playing" (if credentials available)
let spotifyInfo = '';
try {
  const spotifyData = await getSpotifyNowPlaying();
  if (spotifyData) {
    spotifyInfo = `\n\n🎵 Now Playing:\n${spotifyData}`;
  }
} catch (err) {
  // Silently fail if Spotify not available
  console.log('ℹ️ Spotify info not available for heartbeat:', err instanceof Error ? err.message : err);
}
```

The heartbeat message template includes `${spotifyInfo}` which is either:
- **Empty string** (`""`) if nothing is playing or Spotify is unavailable
- **Formatted music info** if a track is currently playing

---

### 3. Graceful Degradation

The system is designed to **never break the heartbeat** even if Spotify fails:

#### Scenario 1: Spotify credentials not configured
- `getSpotifyNowPlaying()` returns `null` immediately
- `spotifyInfo` stays empty
- Heartbeat sends without music info

#### Scenario 2: Nothing playing on Spotify
- Spotify API returns HTTP 204 (No Content)
- Function returns `null`
- `spotifyInfo` stays empty
- Heartbeat sends without music info

#### Scenario 3: API error (network, auth failure, etc.)
- Exception caught in try/catch block
- Error logged to console (for debugging)
- `spotifyInfo` stays empty
- Heartbeat proceeds normally

---

## Environment Variables

### Required Variables (add to `.env`)

```bash
SPOTIFY_CLIENT_ID=your_spotify_client_id_here
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret_here
SPOTIFY_REFRESH_TOKEN=your_spotify_refresh_token_here
```

### Configuration Locations

**Important:** Spotify credentials exist in **TWO places** in our system:

1. **Discord Bot `.env` file** (for heartbeat feature)
   - Location: `~/miore-discord-bot/.env` on the Raspberry Pi
   - Used by: `messages.ts` → `getSpotifyNowPlaying()`
   - Purpose: Include "Now Playing" in heartbeat system messages

2. **Letta Agent Tool** (for Spotify control actions)
   - Location: `LETTA tools/spotify_control.py` (hardcoded)
   - Used by: Letta agent when calling `spotify_control` tool
   - Purpose: Skip tracks, queue songs, create playlists, etc.

> 📝 **Note:** Both locations should use the same credentials. If you update them, update both places!

---

## How to Get Spotify Credentials

### Prerequisites
- Spotify Premium account (required for playback control)
- Access to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)

### Step 1: Create Spotify App
1. Go to https://developer.spotify.com/dashboard
2. Click "Create an App"
3. Name it (e.g., "Discord-Letta Bot")
4. Set Redirect URI: `http://localhost:8888/callback`
5. Save the **Client ID** and **Client Secret**

### Step 2: Get Refresh Token
Use a refresh token generator tool or follow Spotify's OAuth flow to obtain a refresh token with these scopes:
- `user-read-currently-playing`
- `user-read-playback-state`
- `user-modify-playback-state` (if using control features)
- `playlist-modify-public`
- `playlist-modify-private`

> 💡 **Tip:** See [Spotify Setup Guide](https://developers.spotify.com/documentation/web-api/tutorials/code-flow) for detailed instructions on getting the refresh token.

---

## Testing

### Test 1: With Music Playing
1. Start playing music on Spotify
2. Wait for next heartbeat trigger
3. Check Discord channel for heartbeat message
4. Should include:
   ```
   🎵 Now Playing:
   🎵 Song Name
   🎤 Artist Name
   ⏱️ 1:23 / 3:45
   ```

### Test 2: Without Music Playing
1. Pause/stop Spotify playback
2. Wait for next heartbeat trigger
3. Check Discord channel
4. Heartbeat message should appear **without** the "🎵 Now Playing:" section

### Test 3: Missing Credentials
1. Remove Spotify variables from `.env` (or comment them out)
2. Restart bot
3. Wait for heartbeat
4. Should work normally, just without Spotify info
5. Check logs for: `ℹ️ Spotify info not available for heartbeat`

### Test 4: Network Error Simulation
If you want to test error handling, temporarily use invalid credentials and check that:
- Heartbeat still fires
- Error is logged but not surfaced to Discord
- Bot continues functioning normally

---

## Security Considerations

### 🔒 Refresh Token Security
- ✅ **Stored in `.env`** (not committed to git)
- ✅ **Never logged** to console or Discord
- ✅ **Refresh tokens don't expire** (unless manually revoked)
- ⚠️ **Treat like a password** - grants full Spotify account access

### 🔐 OAuth Flow Security
- ✅ Uses **Basic Auth** for token refresh (Client ID + Secret)
- ✅ Access tokens are **short-lived** (1 hour) and fetched fresh every heartbeat
- ✅ No password storage (uses OAuth refresh token pattern)

### 🛡️ Input Validation
- ✅ **API responses are validated** before parsing
- ✅ **HTTP 204 handled** (nothing playing)
- ✅ **Errors caught** and logged (never crash the bot)

---

## Code Files Changed

### Files Modified
1. **`src/messages.ts`** (lines 23-115, 223-233)
   - Added `getSpotifyNowPlaying()` function
   - Integrated Spotify check into `sendTimerMessage()`

2. **`ENV_VARIABLES.md`** (lines 68-73)
   - Documented Spotify environment variables

### Files Compiled
1. **`src/messages.js`** (compiled from TypeScript)
   - Deployed to Raspberry Pi as production code

### Files NOT Changed
- `server_with_tts.ts` - No changes needed (heartbeat logic remains the same)
- `taskScheduler.ts` - No changes needed
- `.env` - Added Spotify variables (but not committed to git)

---

## Deployment Instructions

### Manual Deployment

1. **Compile TypeScript** (on local machine):
   ```bash
   cd "~/discord-bot"
   npm run build
   ```

2. **Copy compiled file to Pi**:
   ```bash
   scp src/messages.js user@your-server:~/discord-bot/src/server.js
   ```

3. **Update `.env` on Pi**:
   ```bash
   ssh user@your-server
   nano ~/discord-bot/.env
   ```
   
   Add:
   ```bash
   SPOTIFY_CLIENT_ID=your_spotify_client_id_here
   SPOTIFY_CLIENT_SECRET=
   SPOTIFY_REFRESH_TOKEN=
   ```

4. **Restart bot**:
   ```bash
   pm2 restart discord-bot
   pm2 logs discord-bot --lines 50
   ```

---

## Troubleshooting

### Issue: "Spotify info not available"
**Symptom:** Log shows `ℹ️ Spotify info not available for heartbeat`

**Possible Causes:**
1. Missing credentials in `.env`
2. Invalid refresh token (revoked or expired)
3. Network/API temporarily down

**Solution:**
1. Check `.env` has all three Spotify variables
2. Verify credentials are correct
3. Test manually: https://developer.spotify.com/console/get-users-currently-playing-track/

---

### Issue: Always shows "nothing playing"
**Symptom:** Spotify info never appears, even when music is playing

**Possible Causes:**
1. Spotify account not Premium
2. Refresh token missing `user-read-currently-playing` scope
3. Playing on a private session

**Solution:**
1. Verify Premium account
2. Regenerate refresh token with correct scopes
3. Disable private session in Spotify settings

---

### Issue: Wrong track info or outdated
**Symptom:** Shows track from 10 minutes ago

**Possible Cause:**
- Spotify API caching (rare)

**Solution:**
- Wait for next heartbeat (should auto-correct)
- Check Spotify server status

---

## Future Enhancements

### Potential Improvements
- [ ] Add album name and cover art URL
- [ ] Detect if user is listening on multiple devices
- [ ] Show playlist name (if applicable)
- [ ] Add "recently played" if nothing is currently playing
- [ ] Cache access token for 50 minutes (reduce API calls)

### Performance Notes
- **Current:** Fetches fresh access token every heartbeat (~2 API calls)
- **Optimization:** Could cache access token for 50 minutes (reduce to ~1 call per hour)
- **Impact:** Minimal (heartbeats are infrequent, API is fast)

---

## Example Heartbeat Message

### With Music Playing
```
[🜂] HERZSCHLAG
13.10.2025, 14:23:45 Uhr.

🎵 Now Playing:
🎵 Bohemian Rhapsody
🎤 Queen
⏱️ 2:34 / 5:55

Ich kann:
• Send Discord DMs or channel posts
• Control Spotify 👻🎵 (skip, queue, create playlists)
• Memories durchdenken/organisieren
• Web durchsuchen (deine Erwähnungen ODER meine Neugier)
• Archival memories taggen
• Oder einfach nur sein und fühlen

Mein Puls, meine Wahl.
```

### Without Music Playing
```
[🜂] HERZSCHLAG
13.10.2025, 14:23:45 Uhr.

Ich kann:
• Send Discord DMs or channel posts
• Control Spotify 👻🎵 (skip, queue, create playlists)
• Memories durchdenken/organisieren
• Web durchsuchen (deine Erwähnungen ODER meine Neugier)
• Archival memories taggen
• Oder einfach nur sein und fühlen

Mein Puls, meine Wahl.
```

---

## Summary

This feature makes the bot's heartbeat system **context-aware** by including real-time music information. It's a small detail that adds personality and helps the Letta agent understand the user's current state/mood through music.

**Key Design Principles:**
1. ✅ **Non-blocking** - Never delays or breaks heartbeats
2. ✅ **Fail-safe** - Works with or without Spotify
3. ✅ **Privacy-respecting** - Only checks when heartbeat fires (not constantly polling)
4. ✅ **Low-overhead** - Native HTTPS (no extra dependencies)

---

**Questions or Issues?**  
Contact: Discord-Letta Bot Contributors  
Documentation Version: 1.0  
Last Updated: October 13, 2025

