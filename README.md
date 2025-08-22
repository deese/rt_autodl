# rt_autodl

A Python-based autodownloader for ruTorrent seedboxes that automatically transfers completed torrents via FTPS with intelligent progress tracking and robust error handling.

## Features

- **Automated Transfer**: Monitors ruTorrent for completed torrents and transfers them via FTPS
- **Smart File Management**: 
  - Size-aware duplicate detection (skips files with matching sizes)
  - Atomic downloads (.part files renamed on completion)
  - Maintains folder structure during transfers
- **Advanced Progress Tracking**: Rich terminal interface with per-file progress bars
- **High Performance**:
  - Multi-connection segmented transfers for large files
  - Concurrent file downloads
  - Optimized for large datasets
- **Robust Label Management**: Automatically relabels torrents after successful transfers
- **Flexible Configuration**: Support for multiple label mappings with individual destination directories
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
  }
}
```

### Advanced Options

- **Multiple Label Mappings**: Configure different source/target label pairs with individual destinations
- **Segmented Downloads**: Large files are split into multiple segments for faster transfers
- **Concurrent Downloads**: Process multiple files simultaneously
- **Secret Management**: Use environment variables (`env:VAR_NAME`) or keyring (`keyring:service/user`)

### Performance Tuning

- `ftps_segments`: Number of parallel segments per file (default: 4)
- `ftps_min_seg_size`: Minimum file size for segmentation (8MB default)
- `ftps_file_concurrency`: Number of files to download simultaneously
- `ftps_blocksize`: Transfer buffer size (256KB default)

## Usage

### Basic Usage

```bash
python src/rt_autodl.py --config config/config.json
```

### Command Line Options

- `--config`: Path to JSON configuration file (required)
- `--verbose`: Enable detailed logging
- `--dry-run`: Show planned actions without executing transfers

### Examples

```bash
# Standard operation
python src/rt_autodl.py --config config/config.json

# Verbose mode for debugging
python src/rt_autodl.py --config config/config.json --verbose

# Test configuration without transferring
python src/rt_autodl.py --config config/config.json --dry-run
```

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
