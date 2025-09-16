#!/usr/bin/env python3
"""
Main CLI interface for Nepali Text Cleaner
Handles command line arguments and user interface
"""

import sys
import argparse
import logging

from Nepali_preprocessor import NepaliTextProcessor

from utils import (
    setup_logging,
    load_config,
    create_sample_config,
    validate_config
)

def parse_arguments():
    """Parse and validate command line arguments"""
    parser = argparse.ArgumentParser(
        description="Nepali Text Cleaner - Process Nepali news articles without tokenization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --single                           # Process live data once using config.json
  %(prog)s --monitor                          # Start continuous monitoring
  %(prog)s --batch --input-dir ./raw_data     # Process existing files
  %(prog)s --create-config                    # Create sample config.json
  %(prog)s --monitor --interval 600           # Monitor every 10 minutes
  %(prog)s --single --preserve-numbers        # Process with number preservation
        """
    )
    
    # Main operation modes
    parser.add_argument('--single', action='store_true', 
                       help='Process live data once and exit')
    parser.add_argument('--monitor', action='store_true', 
                       help='Start continuous monitoring')
    parser.add_argument('--batch', action='store_true', 
                       help='Process existing JSON files')
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
    parser.add_argument('--input-dir', 
                       help='Input directory for batch processing')
    
    # Cleaning options
    parser.add_argument('--preserve-numbers', action='store_true', 
                       help='Preserve numerical data during cleaning')
    parser.add_argument('--normalize-numerals', action='store_true',
                       help='Convert Devanagari numerals to Arabic')
    parser.add_argument('--remove-html', action='store_true', 
                       help='Remove HTML tags from text')
    parser.add_argument('--min-length', type=int, 
                       help='Minimum text length to keep')
    parser.add_argument('--max-length', type=int, 
                       help='Maximum text length to keep')
    
    return parser.parse_args()

def apply_config_overrides(config, args):
    """Apply command line argument overrides to configuration"""
    # Server and API overrides
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
    
    # Initialize cleaning config if not exists
    if not config.get('cleaning'):
        config['cleaning'] = {}
    
    # Cleaning overrides
    if args.preserve_numbers:
        config['cleaning']['preserve_numbers'] = True
    if args.normalize_numerals:
        config['cleaning']['normalize_devanagari_numerals'] = True
    if args.remove_html:
        config['cleaning']['remove_html_tags'] = True
    if args.min_length:
        config['cleaning']['min_text_length'] = args.min_length
    if args.max_length:
        config['cleaning']['max_text_length'] = args.max_length
    
    return config

def display_configuration(config, log_file):
    """Display current configuration to user"""
    print("Configuration:")
    print(f"   Server: {config['server_url']}")
    print(f"   Endpoint: {config['api_endpoint']}")
    print(f"   Output Dir: {config['output_dir']}")
    print(f"   Log File: {log_file}")
    
    cleaning_config = config.get('cleaning', {})
    if cleaning_config:
        print("   Cleaning Options:")
        print(f"     Preserve Numbers: {cleaning_config.get('preserve_numbers', True)}")
        print(f"     Normalize Numerals: {cleaning_config.get('normalize_devanagari_numerals', False)}")
        print(f"     Remove HTML: {cleaning_config.get('remove_html_tags', True)}")
        print(f"     Min/Max Length: {cleaning_config.get('min_text_length', 10)}/{cleaning_config.get('max_text_length', 10000)}")
    print()

def validate_operation_mode(args):
    """Validate that exactly one operation mode is specified"""
    modes = [args.single, args.monitor, args.batch, args.create_config]
    active_modes = sum(bool(mode) for mode in modes)
    
    if active_modes == 0:
        print("Error: Please specify one operation mode:")
        print("  --single      Process data once")
        print("  --monitor     Continuous monitoring")
        print("  --batch       Process existing files")
        print("  --create-config  Create sample config")
        return False
    elif active_modes > 1:
        print("Error: Please specify only one operation mode")
        return False
    
    return True

def main():
    """Main application entry point"""
    # Initialize logging
    log_file = setup_logging(log_prefix="nepali_text_cleaner")
    logger = logging.getLogger(__name__)
    logger.info("Starting Nepali Text Cleaner application")
    
    # Parse arguments
    args = parse_arguments()
    
    # Validate operation mode
    if not validate_operation_mode(args):
        return 1
    
    # Handle config creation
    if args.create_config:
        if create_sample_config():
            print("Sample config.json created successfully.")
            print("Please update with your server details.")
            print("Tip: Adjust 'cleaning' settings as needed")
            return 0
        else:
            print("Error: Failed to create sample config.json")
            return 1
    
    # Load and apply configuration
    config = load_config(args.config)
    config = apply_config_overrides(config, args)
    logger.info(f"Configuration loaded from {args.config}")
    
    # Validate configuration for live processing
    if not args.batch:
        validation_result = validate_config(config)
        if not validation_result.success:
            print(f"Error: Configuration validation failed - {validation_result.error}")
            print("Tip: Run --create-config to generate a sample configuration file")
            logger.error(f"Configuration validation failed: {validation_result.error}")
            return 1
    
    # Display configuration
    display_configuration(config, log_file)
    
    # Initialize processor
    try:
        processor = NepaliTextProcessor(config)
        
        # Initialize cleaner component
        init_result = processor.initialize_cleaner()
        if not init_result.success:
            print(f"Error: {init_result.error}")
            logger.error(init_result.error)
            return 1
        
    except Exception as e:
        error_msg = f"Error initializing processor: {e}"
        print(error_msg)
        logger.error(error_msg)
        return 1
    
    # Execute selected operation
    try:
        if args.batch:
            input_dir = args.input_dir or "raw_data"
            logger.info(f"Starting batch processing from {input_dir}")
            result = processor.run_batch_processing(input_dir)
            
        elif args.single:
            api_endpoint = config.get('api_endpoint', "/news/api.php")
            logger.info("Starting single processing mode")
            result = processor.run_single_processing(api_endpoint)
            
        elif args.monitor:
            api_endpoint = config.get('api_endpoint', "/news/api.php")
            interval = config.get('monitoring_interval', 300)
            max_iterations = config.get('max_iterations')
            logger.info(f"Starting continuous monitoring (interval: {interval}s)")
            result = processor.run_continuous_monitoring(api_endpoint, interval, max_iterations)
        
        if result.success:
            logger.info("Operation completed successfully")
            return 0
        else:
            logger.error(f"Operation failed: {result.error}")
            return 1
        
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        logger.info("Operation interrupted by user")
        return 0
    except Exception as e:
        error_msg = f"Unexpected error during operation: {e}"
        print(error_msg)
        logger.error(error_msg)
        return 1

if __name__ == "__main__":
    sys.exit(main())