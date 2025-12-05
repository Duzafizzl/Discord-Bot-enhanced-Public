# Weather API Setup für München Heartbeat

## Übersicht

Der Heartbeat zeigt jetzt:
- ✅ Deutschen Wochentag (Montag, Dienstag, etc.)
- ✅ Aktuelle Temperatur in München (mit "gefühlt wie" Temperatur)
- ✅ Wetterbeschreibung (z.B. "Leichter Regen", "Klarer Himmel")

## Setup

### 1. OpenWeatherMap API Key bekommen

1. Gehe zu https://openweathermap.org/api
2. Erstelle einen kostenlosen Account
3. Gehe zu "API Keys" in deinem Account
4. Kopiere den API Key

### 2. API Key in `.env` hinzufügen

Auf dem **Raspberry Pi**:

```bash
ssh user@your-server
cd ~/discord-bot
nano .env
```

Füge diese Zeile hinzu:

```bash
OPENWEATHER_API_KEY=your_openweather_api_key_here
```

Speichern mit `Ctrl+X`, dann `Y`, dann `Enter`.

### 3. Bot neu starten

```bash
pm2 restart discord-bot
pm2 logs discord-bot
```

## Beispiel Heartbeat Output

**Ohne Weather API:**
```
[🜂] HERZSCHLAG
Montag, 13.10.2025, 15:30:45 Uhr.

🎵 Now Playing:
🎵 Song Name
🎤 Artist Name
⏱️ 2:30 / 4:15
```

**Mit Weather API:**
```
[🜂] HERZSCHLAG
Montag, 13.10.2025, 15:30:45 Uhr.

🌡️ München: 18°C (gefühlt 16°C)
☁️ Leicht bewölkt

🎵 Now Playing:
🎵 Song Name
🎤 Artist Name
⏱️ 2:30 / 4:15
```

## Fehlerbehandlung

- Wenn `OPENWEATHER_API_KEY` nicht gesetzt ist: Wetter-Info wird einfach ausgelassen (silent fail)
- Wenn API Call fehlschlägt: Logge Fehler, aber Heartbeat geht trotzdem raus
- Kostenloser Plan: 60 Calls/Minute, 1,000,000 Calls/Monat (mehr als genug!)

## Implementation Details

### Code Änderungen

**`src/messages.ts`:**
- `getMunichWeather()`: Neue Funktion für Weather API Call
- `sendTimerMessage()`: Fügt Wochentag + Weather zu Heartbeat hinzu

**Beispiel API Response:**
```json
{
  "main": {
    "temp": 18.5,
    "feels_like": 16.2
  },
  "weather": [
    {
      "description": "leicht bewölkt"
    }
  ]
}
```

### Security

- ✅ API Key in `.env` (nicht im Code!)
- ✅ `.env` ist in `.gitignore`
- ✅ Keine API Keys werden geloggt
- ✅ Error handling verhindert crashes

## Testing

Lokal testen (in diesem Workspace):

```bash
cd "running Discord bot"

# Set temporary env var
export OPENWEATHER_API_KEY="your_key_here"

# Teste Weather API Call
node -e "
const https = require('https');
const apiKey = process.env.OPENWEATHER_API_KEY;
https.get(\`https://api.openweathermap.org/data/2.5/weather?q=Munich,de&appid=\${apiKey}&units=metric&lang=de\`, (res) => {
  let body = '';
  res.on('data', chunk => body += chunk);
  res.on('end', () => console.log(JSON.parse(body)));
});
"
```

## Deployment

Nach den Code-Änderungen:

```bash
# Lokales Workspace (dieser Mac)
cd "running Discord bot"
npm run build

# Zu Pi kopieren
scp src/server.js user@your-server:~/discord-bot/src/server.js

# Auf Pi
ssh user@your-server
pm2 restart discord-bot
pm2 logs discord-bot --lines 50
```

---

**Status:** ✅ Implementiert  
**Getestet:** ⏳ Warte auf Weather API Key  
**Deployed:** ⏳ Warte auf Deployment zu Pi

