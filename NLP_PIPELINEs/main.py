#!/usr/bin/env python3
"""
Main application file for Nepali Text Tokenizer
Run this file to start the tokenization process
"""

import sys
import argparse
import json
from pathlib import Path

# Import the tokenizer class (assuming the previous code is in nepali_tokenizer.py)
from Nepali_Tokenizer import NepaliTextTokenizer

def load_config(config_file="config.json"):
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Config file {config_file} not found. Using default settings.")
        return get_default_config()
    except json.JSONDecodeError as e:
        print(f"Error parsing config file: {e}")
        return get_default_config()

def get_default_config():
    """Return default configuration"""
    return {
        "server_url": "https://apinp.com",
        "api_endpoint": "/news/api.php",
        "output_dir": "tokenized_data",
        "monitoring_interval": 300,
        "max_iterations": None,
        "timeout": 10
    }

def create_sample_config():
    """Create a sample configuration file"""
    config = get_default_config()
    with open("config.json", 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)
    print("Sample config.json created. Please update with your server details.")

def run_single_processing(tokenizer, endpoint):
    """Run tokenization once"""
    print("🚀 Processing live data from server...")
    print("-" * 50)
    
    result = tokenizer.process_live_data(endpoint)
    
    if result["success"]:
        print("✅ SUCCESS!")
        print(f"📊 Processed {result['token_count']} tokens")
        print(f"💾 Saved to: {result['filepath']}")
        print(f"🔤 Sample tokens: {result['tokens'][:10]}...")
        
        # Show some statistics
        if result['tokens']:
            unique_tokens = len(set(result['tokens']))
            print(f"📈 Unique tokens: {unique_tokens}")
            print(f"📝 Average token length: {sum(len(token) for token in result['tokens'])/len(result['tokens']):.2f}")
    else:
        print("❌ FAILED!")
        print(f"🔴 Error: {result['error']}")
        return False
    
    return True

def run_continuous_monitoring(tokenizer, endpoint, interval, max_iterations):
    """Run continuous monitoring"""
    print("🔄 Starting continuous monitoring...")
    print(f"⏱️  Check interval: {interval} seconds")
    if max_iterations:
        print(f"🔢 Max iterations: {max_iterations}")
    else:
        print("♾️  Running indefinitely (Ctrl+C to stop)")
    print("-" * 50)
    
    try:
        tokenizer.continuous_monitoring(endpoint, interval, max_iterations)
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped by user")

def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(
        description="Nepali Text Tokenizer - Process live news articles from server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --single                    # Process once using config.json
  %(prog)s --monitor                   # Start continuous monitoring
  %(prog)s --single --config my.json   # Use custom config file
  %(prog)s --create-config             # Create sample config.json
  %(prog)s --monitor --interval 600    # Monitor every 10 minutes
        """
    )
    
    # Main operation modes
    parser.add_argument('--single', action='store_true', 
                       help='Process data once and exit')
    parser.add_argument('--monitor', action='store_true', 
                       help='Start continuous monitoring')
    parser.add_argument('--create-config', action='store_true', 
                       help='Create sample configuration file')
    
    # Configuration options
    parser.add_argument('--config', default='config.json', 
                       help='Configuration file path (default: config.json)')
    parser.add_argument('--server', 
                       help='Server URL (overrides config)')
    parser.add_argument('--endpoint', 
                       help='API endpoint (overrides config)')
    parser.add_argument('--interval', type=int, 
                       help='Monitoring interval in seconds (overrides config)')
    parser.add_argument('--max-iter', type=int, 
                       help='Maximum iterations for monitoring (overrides config)')
    parser.add_argument('--output-dir', 
                       help='Output directory (overrides config)')
    
    args = parser.parse_args()
    
    # Handle config creation
    if args.create_config:
        create_sample_config()
        return 0
    
    # Require at least one operation mode
    if not (args.single or args.monitor):
        print("❌ Error: Please specify either --single or --monitor")
        parser.print_help()
        return 1
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.server:
        config['server_url'] = args.server
    if args.endpoint:
        config['api_endpoint'] = args.endpoint
    if args.interval:
        config['monitoring_interval'] = args.interval
    if args.max_iter:
        config['max_iterations'] = args.max_iter
    if args.output_dir:
        config['output_dir'] = args.output_dir
    
    # Validate required configuration
    if not config.get('server_url') or config['server_url'] == "https://your-news-server.com":
        print("❌ Error: Please configure server_url in config.json or use --server")
        print("💡 Tip: Run --create-config to generate a sample configuration file")
        return 1
    
    # Display configuration
    print("🔧 Configuration:")
    print(f"   Server: {config['server_url']}")
    print(f"   Endpoint: {config['api_endpoint']}")
    print(f"   Output Dir: {config['output_dir']}")
    print()
    
    # Initialize tokenizer
    try:
        # Use server_url from config if present, else default to "https://apinp.com"
        server_url = config.get('server_url', "https://apinp.com")
        output_dir = config.get('output_dir', "tokenized_data")

        tokenizer = NepaliTextTokenizer(
            server_url=server_url,
            output_dir=output_dir
        )
    except Exception as e:
        print(f"❌ Error initializing tokenizer: {e}")
        return 1

# Run selected operation
    try:
        # Use api_endpoint from config or default to "/news/api.php"
        api_endpoint = config.get('api_endpoint', "/news/api.php")

        if args.single:
            success = run_single_processing(tokenizer, api_endpoint)
            return 0 if success else 1

        elif args.monitor:
            run_continuous_monitoring(
                tokenizer,
                api_endpoint,
                config.get('monitoring_interval', 300),
                config.get('max_iterations')
            )
            return 0

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)