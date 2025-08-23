# rt_autodl

A Python-based autodownloader for ruTorrent seedboxes that automatically transfers completed torrents via FTPS with intelligent progress tracking and robust error handling.

## Features

- **Automated Transfer**: Monitors ruTorrent for completed torrents and transfers them via FTPS
- **Smart File Management**: 
  - Size-aware duplicate detection (skips files with matching sizes)
  - Atomic downloads (.part files renamed on completion)
  - Maintains folder structure during transfers
- **Advanced Progress Tracking**: Rich terminal interface with persistent progress bars and 50/50 split layout
- **Rolling Progress History**: All completed transfers remain visible with success/failure indicators
- **High Performance**:
  - Multi-connection segmented transfers for large files
  - Concurrent file downloads
  - Optimized for large datasets
- **Robust Label Management**: Automatically relabels torrents after successful transfers
- **Flexible Configuration**: Support for multiple label mappings with individual destination directories
- **Multiple Output Modes**: Interactive UI, quiet mode for automation, and structured JSON logging
- **Security**: Built-in secret management with environment variables and keyring support

## Installation

### Requirements

- Python 3.7+
- ruTorrent with httprpc plugin enabled
- FTPS server access

### Dependencies

Install required Python packages:

```bash
pip install -r requirements.txt
```

Required packages:
- `pyrutorrent` - ruTorrent API client
- `pycurl` - High-performance FTPS transfers
- `rich` - Terminal progress bars and formatting
- `json-with-comments` - Configuration file parsing

## Configuration

Create a configuration file based on the template:

```bash
cp config/config.json.template config/config.json
```

### Basic Configuration

```json
{
  "label_mappings": [
    {
      "source": "autodl",
      "target": "downloaded", 
      "dest_dir": "/data/inbox"
    }
  ],
  
  "rutorrent": {
    "uri": "https://user:pass@seedbox.example.com/rutorrent/plugins/httprpc/action.php"
  },
  
  "sftp": {
    "backend": "ftps",
    "dest_dir": "/default/destination",
    
    "ftps_host": "seedbox.example.com",
    "ftps_user": "username",
    "ftps_password": "password",
    "ftps_port": 21,
    "ftps_pasv": true,
    "ftps_tls_verify": true,
    
    "ftp_root": "/downloads",
    "rtorrent_root": "/data/rtorrent",
    
    "ftps_blocksize": 262144,
    "ftps_segments": 4,
    "ftps_min_seg_size": 8388608,
    "ftps_file_concurrency": 1
  },
  
  "ui": {
    "wait": true
  }
}
```

### Advanced Options

- **Multiple Label Mappings**: Configure different source/target label pairs with individual destinations
- **Segmented Downloads**: Large files are split into multiple segments for faster transfers
- **Concurrent Downloads**: Process multiple files simultaneously
- **Secret Management**: Use environment variables (`env:VAR_NAME`) or keyring (`keyring:service/user`)
- **UI Configuration**: Control wait behavior and output modes

### Performance Tuning

- `ftps_segments`: Number of parallel segments per file (default: 4)
- `ftps_min_seg_size`: Minimum file size for segmentation (8MB default)
- `ftps_file_concurrency`: Number of files to download simultaneously
- `ftps_blocksize`: Transfer buffer size (256KB default)

### UI Configuration

Configure user interface behavior in the `"ui"` section:

```json
{
  "ui": {
    "wait": true  // Wait for keypress at end (default: true)
  }
}
```

**Wait Behavior Priority:**
1. `--no-wait` flag overrides all other settings
2. `--quiet` mode never waits (for automation)
3. Config `"ui.wait": false` disables waiting
4. Default: waits for user input

## Usage

### Basic Usage

```bash
python -m src.main --config config/config.json
```

### Command Line Options

- `--config`: Path to JSON configuration file (required)
- `--verbose`: Enable detailed logging
- `--dry-run`: Show planned actions without executing transfers
- `--quiet`: Quiet mode for cron jobs (minimal stdout, full disk logging)
- `--no-wait`: Don't wait for keypress at the end
- `--json-logs`: Output structured JSON logs to stdout
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR)

### Examples

```bash
# Standard operation with interactive UI
python -m src.main --config config/config.json

# Quiet mode for cron jobs
python -m src.main --config config/config.json --quiet

# Skip waiting at the end
python -m src.main --config config/config.json --no-wait

# Verbose mode for debugging
python -m src.main --config config/config.json --verbose

# Test configuration without transferring
python -m src.main --config config/config.json --dry-run

# JSON structured logging
python -m src.main --config config/config.json --json-logs
```

### Output Modes

#### Interactive Mode (Default)
- **Split Layout**: 50/50 split between progress bars and activity log
- **Rolling History**: All completed transfers remain visible with ✓/✗ indicators
- **Real-time Progress**: Live progress bars with transfer speeds and completion times
- **Wait for Exit**: Prompts user to press any key before exiting (configurable)

#### Quiet Mode (`--quiet`)
- **Minimal Output**: Only shows torrent names and final status (DOWNLOADING, OK, ERROR, SKIP)
- **Full Disk Logging**: Complete structured logs written to disk/JSON
- **Cron-Friendly**: No progress bars, no user interaction required
- **Auto-Exit**: Never waits for user input

#### JSON Mode (`--json-logs`)
- **Structured Output**: All logs in JSON format for parsing/monitoring
- **Progress Tracking**: Standard progress bars with JSON logging

### Automation & Cron Jobs

For automated execution (cron jobs, systemd timers, etc.), use quiet mode:

```bash
# Cron job example - runs every 30 minutes
*/30 * * * * /usr/bin/python3 -m src.main --config /path/to/config.json --quiet

# Systemd timer with logging
python3 -m src.main --config config.json --quiet >> /var/log/rt_autodl.log 2>&1
```

**Quiet Mode Output Format:**
- `DOWNLOADING filename` - Transfer started
- `OK filename` - Successfully completed
- `ERROR filename: reason` - Transfer failed
- `SKIP filename: reason` - Torrent skipped

## How It Works

1. **Discovery**: Connects to ruTorrent and queries for torrents with specified source labels
2. **Filtering**: Only processes completed torrents that haven't been transferred yet
3. **Planning**: Maps torrent files to FTPS paths and calculates transfer requirements
4. **Transfer**: Downloads files via FTPS with progress tracking and error handling
5. **Relabeling**: Updates torrent labels to indicate successful processing

## File Structure

```
rt_autodl/
├── src/
│   ├── main.py              # Main application entry point
│   ├── rt_autodl.py         # Legacy wrapper script
│   ├── config.py            # Configuration loading and validation
│   ├── ftps_client.py       # FTPS transfer implementation
│   ├── rutorrent_client.py  # ruTorrent API integration
│   ├── secrets.py           # Secret management utilities
│   ├── logger.py            # Structured logging system
│   ├── stats.py             # Statistics tracking
│   ├── connection_pool.py   # Connection pool management
│   └── utils.py             # Common utilities and helpers
├── config/
│   └── config.json.template # Configuration template
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Error Handling

- **Connection Issues**: Automatic retry logic for network failures
- **Incomplete Transfers**: Failed downloads are cleaned up automatically
- **Label Conflicts**: Robust relabeling with fallback mechanisms
- **File Conflicts**: Intelligent duplicate detection based on file sizes

## Troubleshooting

### Common Issues

1. **Connection Refused**: Verify ruTorrent URI and httprpc plugin is enabled
2. **FTPS Timeouts**: Adjust `ftps_timeout` setting or check network connectivity
3. **Permission Errors**: Ensure destination directories are writable
4. **SSL/TLS Errors**: Set `ftps_tls_verify: false` for self-signed certificates

### Debug Mode

Run with `--verbose --dry-run` to see detailed execution plans without making changes.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and feature requests, please create an issue in the project repository. 
