# Systemd Service Setup Guide

This guide will help you set up the Discord Grok Bot to run as a systemd service on your Linux server.

## Prerequisites

- Linux server with systemd (Ubuntu 16.04+, Debian 8+, CentOS 7+, etc.)
- Node.js 18+ installed
- Bot dependencies installed (`npm install`)
- Bot built (`npm run build`)
- `.env` file configured

## Installation Steps

### 1. Edit the Service File

Open `discord-grok-bot.service` and update these values:

```bash
# Change 'your_username' to your actual Linux username
User=your_username
Group=your_username

# Change to the actual path where the bot is located
WorkingDirectory=/home/your_username/wolfe-discord-bot-enhanced

# Update the path to your .env file
EnvironmentFile=/home/your_username/wolfe-discord-bot-enhanced/.env

# Update the log directory path
ReadWritePaths=/home/your_username/wolfe-discord-bot-enhanced/logs
```

**Example for user 'ubuntu':**
```bash
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/wolfe-discord-bot-enhanced
EnvironmentFile=/home/ubuntu/wolfe-discord-bot-enhanced/.env
ReadWritePaths=/home/ubuntu/wolfe-discord-bot-enhanced/logs
```

### 2. Copy Service File to Systemd Directory

```bash
sudo cp discord-grok-bot.service /etc/systemd/system/
```

### 3. Reload Systemd

```bash
sudo systemctl daemon-reload
```

### 4. Enable the Service (Auto-start on Boot)

```bash
sudo systemctl enable discord-grok-bot
```

### 5. Start the Service

```bash
sudo systemctl start discord-grok-bot
```

## Management Commands

### Check Service Status
```bash
sudo systemctl status discord-grok-bot
```

### View Logs (Real-time)
```bash
sudo journalctl -u discord-grok-bot -f
```

### View Recent Logs
```bash
sudo journalctl -u discord-grok-bot -n 100
```

### View Logs from Today
```bash
sudo journalctl -u discord-grok-bot --since today
```

### Restart Service
```bash
sudo systemctl restart discord-grok-bot
```

### Stop Service
```bash
sudo systemctl stop discord-grok-bot
```

### Disable Auto-start
```bash
sudo systemctl disable discord-grok-bot
```

## Troubleshooting

### Service Won't Start

1. **Check service status:**
   ```bash
   sudo systemctl status discord-grok-bot
   ```

2. **Check logs for errors:**
   ```bash
   sudo journalctl -u discord-grok-bot -n 50
   ```

3. **Verify file paths are correct:**
   ```bash
   cat /etc/systemd/system/discord-grok-bot.service
   ```

4. **Test the bot manually:**
   ```bash
   cd /path/to/wolfe-discord-bot-enhanced
   npm start
   ```

### Common Issues

**"Failed to load environment file"**
- Ensure `.env` file exists and has correct permissions
- Path in `EnvironmentFile=` must be absolute

**"Permission denied"**
- Check that the user specified in the service file owns the bot directory
- Verify `.env` file has correct permissions: `chmod 600 .env`

**"Cannot find module"**
- Ensure you ran `npm install` and `npm run build`
- Check that `node_modules` directory exists

**Bot starts but crashes**
- Check if nate_api_substrate is running: `curl http://localhost:8091/api/health`
- Verify environment variables in `.env` are correct
- Check logs: `sudo journalctl -u discord-grok-bot -n 100`

### Verify Environment Variables Are Loaded

```bash
sudo systemctl show discord-grok-bot --property=Environment
```

## Security Notes

1. **Protect your .env file:**
   ```bash
   chmod 600 /path/to/wolfe-discord-bot-enhanced/.env
   ```

2. **Limit service permissions:**
   The service file includes security hardening:
   - `NoNewPrivileges=true` - Prevents privilege escalation
   - `PrivateTmp=true` - Isolates /tmp directory
   - `ProtectSystem=strict` - Makes most of filesystem read-only
   - `ProtectHome=read-only` - Protects home directories

3. **Review logs regularly:**
   ```bash
   sudo journalctl -u discord-grok-bot --since "1 hour ago"
   ```

## Updating the Bot

When you update the bot code:

```bash
# Stop the service
sudo systemctl stop discord-grok-bot

# Pull updates
cd /path/to/wolfe-discord-bot-enhanced
git pull

# Install dependencies if needed
npm install

# Rebuild
npm run build

# Start the service
sudo systemctl start discord-grok-bot

# Check status
sudo systemctl status discord-grok-bot
```

## Using with PM2 Instead

If you prefer PM2 over systemd:

```bash
# Install PM2 globally
npm install -g pm2

# Start the bot
pm2 start npm --name "discord-grok-bot" -- start

# Save PM2 configuration
pm2 save

# Generate systemd service for PM2 (runs PM2 on boot)
pm2 startup systemd

# Follow the command it outputs
```

PM2 provides:
- Automatic restarts
- Built-in log rotation
- Cluster mode (if needed)
- Web-based monitoring

Choose systemd for simplicity or PM2 for more advanced features.
