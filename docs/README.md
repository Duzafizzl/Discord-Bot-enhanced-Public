# 📚 Discord Bot - Documentation

Welcome to the Discord Bot documentation! This guide will help you understand, configure, and deploy your own Letta-powered Discord bot.

---

## 🎯 Quick Links

| I want to... | Go here |
|--------------|---------|
| 🚀 Get started quickly | [Main README](../README.md) |
| 🔧 Configure environment variables | [ENV_VARIABLES.md](../ENV_VARIABLES.md) |
| 🔒 Setup security | [SECURITY.md](../SECURITY.md) |
| 🎵 Add Spotify integration | [features/SPOTIFY_HEARTBEAT_INTEGRATION.md](features/SPOTIFY_HEARTBEAT_INTEGRATION.md) |
| ⏰ Configure heartbeats | [features/TIMER_SETUP.md](features/TIMER_SETUP.md) |
| 🌤️ Add weather | [WEATHER_SETUP.md](WEATHER_SETUP.md) |
| 📊 Monitor API usage | [LETTA_STATS_MONITOR.md](LETTA_STATS_MONITOR.md) |

---

## 📂 Documentation Structure

### Core Documentation
- **[ADMIN_COMMANDS_README.md](ADMIN_COMMANDS_README.md)** - Admin commands for bot management
- **[AUTONOMOUS_DEPLOYMENT_GUIDE.md](AUTONOMOUS_DEPLOYMENT_GUIDE.md)** - Autonomous mode setup
- **[LETTA_STATS_MONITOR.md](LETTA_STATS_MONITOR.md)** - API usage monitoring
- **[MCP_HANDLER_ENV_SETUP.md](MCP_HANDLER_ENV_SETUP.md)** - MCP handler configuration
- **[WEATHER_SETUP.md](WEATHER_SETUP.md)** - Weather API integration
- **[RETRY_CONFIG.md](RETRY_CONFIG.md)** - API retry configuration
- **[RETRY_LOGIC_GUIDE.md](RETRY_LOGIC_GUIDE.md)** - Retry logic implementation

### Feature Guides
Located in `features/`:
- **[SPOTIFY_HEARTBEAT_INTEGRATION.md](features/SPOTIFY_HEARTBEAT_INTEGRATION.md)** - Spotify "Now Playing" integration
- **[TIMER_SETUP.md](features/TIMER_SETUP.md)** - Autonomous heartbeat system

### API References
Located in `api/`:
- **[LETTA_API_REFERENCE.md](api/LETTA_API_REFERENCE.md)** - Letta API documentation
- **[DISCORD_API_REFERENCE.md](api/DISCORD_API_REFERENCE.md)** - Discord API usage examples
- **[README.md](api/README.md)** - API overview

---

## 🚀 Getting Started

1. **Read the main README**: Start with [../README.md](../README.md)
2. **Setup environment**: Follow [../ENV_VARIABLES.md](../ENV_VARIABLES.md)
3. **Configure security**: Review [../SECURITY.md](../SECURITY.md)
4. **Build & Run**: `npm install && npm run build && npm start`

---

## 🎨 Features

### Core Features
- ✅ Persistent memory with Letta AI
- ✅ Autonomous mode with bot-loop prevention
- ✅ Image processing and attachment handling
- ✅ Admin commands for remote management
- ✅ API retry logic for reliability

### Optional Features
- 🎵 Spotify integration
- 🌤️ Weather integration
- ⏰ Autonomous heartbeats
- 🎬 Automatic GIF sending
- 📊 API usage monitoring

---

## 🔒 Security

**IMPORTANT:** Never commit secrets to git!
- All API keys go in `.env` file
- Use `.env.example` as template
- Review [SECURITY.md](../SECURITY.md) for best practices

---

## 🤝 Contributing

Contributions are welcome! Please:
1. Follow existing code style
2. Update documentation
3. Test thoroughly
4. Never commit API keys

---

## 📞 Support

- **Issues**: Open a GitHub issue
- **Questions**: Check existing documentation
- **API Docs**: See [api/](api/) directory

---

**Happy botting! 🤖✨**
