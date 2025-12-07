# 🤖 Discord-Grok Bot

A powerful, autonomous Discord bot powered by [Grok 4.1 API](https://x.ai/) via [nate_api_substrate](https://github.com/clotho2/nate_api_substrate), featuring memory management, scheduled tasks, voice messages, Spotify integration, and more.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9-blue.svg)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-18%2B-green.svg)](https://nodejs.org/)

---

## ✨ Features

### 🧠 **Grok 4.1 API Integration**
- **Conversational Memory**: Stateful conversations with persistent memory via nate_api_substrate
- **Context-Aware Responses**: AI remembers past conversations and user preferences
- **Streaming Responses**: Real-time message streaming with Server-Sent Events (SSE)
- **Tool Calling**: Extensible tool system for Discord operations, web search, Spotify control, and more
- **Advanced Reasoning**: Powered by Grok 4.1's fast reasoning capabilities
- **Multimodal Support**: Image analysis with base64 encoding via nate_api_substrate

### 📅 **Autonomous Task Scheduling**
- **Smart Scheduler**: Create recurring tasks (hourly, daily, weekly, monthly)
- **Flexible Timing**: Support for specific times, dates, and day-of-week scheduling
- **Self-Tasks & Reminders**: Bot can schedule tasks for itself or send reminders to users
- **Timezone Support**: Configurable timezone for all scheduled operations

### 🎵 **Spotify Integration**
- **Now Playing**: Automatic Spotify status in heartbeat messages
- **Playback Control**: Skip tracks, manage queue, create playlists
- **Context-Aware**: Bot understands user's music preferences and mood

### 🎤 **Voice Messages (ElevenLabs)**
- **Text-to-Speech**: Generate natural-sounding voice messages
- **Audio Tags**: Support for expressive speech (excited, whispering, etc.)
- **Discord Integration**: Send voice messages directly to channels or DMs

### 🤖 **MCP (Model Context Protocol) Support**
- **Robot Control**: Interface with XGO robot via MCP protocol
- **Command Execution**: SSH-based robot command system
- **Extensible**: Easy to add new MCP-compatible devices

### 📊 **Monitoring & Analytics**
- **Letta Stats**: Track API usage, costs, and conversation metrics
- **Daily Summaries**: Automated daily reports with insights
- **Admin Commands**: PM2 process management, system status, bot statistics

### 🌐 **Additional Features**
- **Weather Integration**: OpenWeatherMap API for weather data
- **GIF Auto-Sender**: Automatic GIF responses with Tenor API
- **Image Processing**: OCR, PDF parsing, image analysis with automatic compression
- **YouTube Transcripts**: Fetch and analyze YouTube video content
- **Attachment Forwarding**: Smart file handling with size management and real-time streaming

---

## 🚀 Quick Start

### Prerequisites

- **Node.js** 18+ and npm
- **Python** 3.8+ (for Python tools)
- **Discord Bot Token** ([Create a bot](https://discord.com/developers/applications))
- **nate_api_substrate** running locally or on a server ([Setup guide](https://github.com/clotho2/nate_api_substrate))
- **xAI API Key** for Grok 4.1 ([Get API access](https://x.ai/))

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/discord-letta-bot.git
   cd discord-letta-bot
   ```

2. **Install dependencies**
   ```bash
   npm install
   pip install -r requirements.txt  # If you have Python tools
   ```

3. **Set up nate_api_substrate**
   ```bash
   # Clone and set up nate_api_substrate
   git clone https://github.com/clotho2/nate_api_substrate.git
   cd nate_api_substrate
   # Follow setup instructions in the repository
   # Make sure it's running on http://localhost:8091 (default)
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and configuration
   ```

5. **Build TypeScript**
   ```bash
   npm run build
   ```

6. **Start the bot**
   ```bash
   npm start
   # Or use PM2 for production:
   pm2 start npm --name "discord-bot" -- start
   ```

---

## ⚙️ Configuration

### Required Environment Variables

Create a `.env` file in the project root with at least these values:

```bash
# Grok API (via nate_api_substrate)
GROK_BASE_URL=http://localhost:8091
GROK_SESSION_ID=discord-bot
GROK_MODEL=grok-4-1-fast-reasoning
GROK_API_TIMEOUT_MS=300000

# Discord
DISCORD_TOKEN=your_discord_bot_token_here
DISCORD_CHANNEL_ID=your_main_channel_id

# Bot Behavior
USE_SENDER_PREFIX=true  # Message formatting
SURFACE_ERRORS=true
RESPOND_TO_DMS=true
RESPOND_TO_MENTIONS=true
TIMEZONE=Europe/Berlin  # IANA timezone
```

See [`ENV_VARIABLES.md`](ENV_VARIABLES.md) for the complete list of configuration options.

---

## 📚 Documentation

### Core Documentation
- **[ENV_VARIABLES.md](ENV_VARIABLES.md)** - Complete environment variable reference
- **[SECURITY.md](SECURITY.md)** - Security policies and vulnerability reporting
- **[docs/README.md](docs/README.md)** - Documentation index

### Feature Guides
- **[Spotify Integration](docs/features/SPOTIFY_HEARTBEAT_INTEGRATION.md)** - Set up Spotify Now Playing
- **[Weather Setup](docs/WEATHER_SETUP.md)** - Configure OpenWeatherMap integration
- **[Admin Commands](docs/ADMIN_COMMANDS_README.md)** - Remote bot management
- **[Autonomous Deployment](docs/AUTONOMOUS_DEPLOYMENT_GUIDE.md)** - Production deployment guide
- **[MCP Handler](docs/MCP_HANDLER_ENV_SETUP.md)** - Robot control setup

### API References
- **[Discord API Reference](docs/api/DISCORD_API_REFERENCE.md)** - Discord.js integration
- **[Letta API Reference](docs/api/LETTA_API_REFERENCE.md)** - Letta tool system
- **[API Overview](docs/api/README.md)** - API architecture

### Tools
- **[Tools README](tools/README.md)** - Letta tools overview
- **[Tool Management Guide](tools/TOOL_MANAGEMENT_GUIDE.md)** - Creating and deploying tools

---

## 🛠️ Development

### Project Structure

```
discord-letta-bot/
├── src/                    # TypeScript source code
│   ├── server.ts          # Main bot entry point
│   ├── messages.ts        # Message handling & Letta integration
│   ├── taskScheduler.ts   # Task scheduling system
│   ├── adminCommands.ts   # Admin command handler
│   ├── elevenlabs/        # Voice message integration
│   └── ...
├── tools/                  # Letta-compatible Python tools
│   ├── discord_tool.py    # Discord operations tool
│   ├── spotify_control.py # Spotify control tool
│   └── ...
├── scripts/                # Utility scripts
├── docs/                   # Documentation
└── .env                    # Configuration (create from .env.example)
```

### Building

```bash
# Development with auto-reload
npm run dev

# Production build
npm run build
npm start
```

### Creating New Tools

Letta tools are Python functions that follow a specific format. See [Tool Management Guide](tools/TOOL_MANAGEMENT_GUIDE.md) for details.

Example tool:
```python
def my_tool(param: str) -> str:
    """
    Tool description for Letta
    
    Args:
        param: Parameter description
        
    Returns:
        str: Result description
    """
    return f"Result: {param}"
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **[xAI Grok 4.1](https://x.ai/)** - Advanced AI reasoning model
- **[nate_api_substrate](https://github.com/clotho2/nate_api_substrate)** - AI consciousness framework with memory management
- **[Discord.js](https://discord.js.org)** - Powerful Discord API library
- **[ElevenLabs](https://elevenlabs.io)** - High-quality text-to-speech
- **[Spotify Web API](https://developer.spotify.com/documentation/web-api)** - Music integration

---

## 🐛 Issues & Support

- **Bug Reports**: [GitHub Issues](https://github.com/yourusername/discord-letta-bot/issues)
- **Security Issues**: See [SECURITY.md](SECURITY.md)
- **Documentation**: [docs/](docs/)

---

## 🌟 Star History

If you find this project useful, please consider giving it a star! ⭐

---

**Made with ❤️ by the Discord-Grok Bot community**
