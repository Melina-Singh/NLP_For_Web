#!/usr/bin/env python3
"""
Main application file for Nepali Text Tokenizer
Run this file to start the tokenization process
Now includes preprocessing capabilities
"""

import sys
import argparse
import time
import logging

# Import the tokenizer and preprocessor classes
from Nepali_Tokenizer import NepaliTextTokenizer
from Nepali_preprocessor import NepaliTextPreprocessor
from utils import (
    ProcessingResult,
    setup_logging,
    load_config,
    get_default_config,
    create_sample_config,
    validate_config
)

class NepaliTextProcessor:
    """Main application controller for Nepali text processing"""
    
    def __init__(self, config):
        self.config = config
        self.tokenizer = None
        self.preprocessor = None
        self.logger = logging.getLogger(__name__)
        
    def initialize_components(self):
        """Initialize tokenizer and preprocessor components"""
        result = ProcessingResult()
        
        try:
            # Initialize tokenizer
            self.tokenizer = NepaliTextTokenizer(
                server_url=self.config.get('server_url', "https://apinp.com"),
                output_dir=self.config.get('output_dir', "tokenized_data")
            )
            self.logger.info("Tokenizer initialized successfully")
            
            # Initialize preprocessor if needed
            preprocessing_config = self.config.get('preprocessing', {})
            if preprocessing_config.get('enabled', False) or preprocessing_config.get('auto_clean', False):
                self.preprocessor = NepaliTextPreprocessor(
                    input_dir=self.config.get('output_dir', 'tokenized_data'),
                    output_dir=self.config.get('cleaned_output_dir', 'cleaned_data'),
                    config=preprocessing_config
                )
                self.logger.info("Preprocessor initialized successfully")
            
            result.success = True
            
        except Exception as e:
            result.error = f"Component initialization failed: {e}"
            self.logger.error(result.error)
        
        return result
    
    def run_single_processing(self, endpoint):
        """Run tokenization once with optional preprocessing"""
        self.logger.info("Starting single processing mode")
        print("Starting single data processing...")
        print("-" * 50)
        
        # Run tokenization
        result = self.tokenizer.process_live_data(endpoint)
        
        if result.success:
            print("Tokenization completed successfully!")
            print(f"Processed {result.data['token_count']} tokens")
            print(f"Saved to: {result.data['filepath']}")
            
            if result.data['tokens']:
                unique_tokens = len(set(result.data['tokens']))
                avg_length = sum(len(token) for token in result.data['tokens']) / len(result.data['tokens'])
                print(f"Unique tokens: {unique_tokens}")
                print(f"Average token length: {avg_length:.2f}")
            
            # Run preprocessing if enabled
            if self.preprocessor and self.config.get('preprocessing', {}).get('auto_clean', False):
                print("\nStarting automatic preprocessing...")
                print("-" * 30)
                
                preprocess_result = self.preprocessor.process_json_file(result.data['filepath'])
                
                if preprocess_result.success:
                    print("Preprocessing completed successfully!")
                    print(f"Tokens: {preprocess_result.data['original_token_count']} -> {preprocess_result.data['processed_token_count']}")
                    print(f"Cleaned data saved to: {preprocess_result.data['output_file']}")
                    retention_rate = (preprocess_result.data['processed_token_count'] / preprocess_result.data['original_token_count']) * 100
                    print(f"Retention rate: {retention_rate:.2f}%")
                else:
                    print(f"Preprocessing failed: {preprocess_result.error}")
                    return ProcessingResult(success=False, error=preprocess_result.error)
        else:
            print(f"Tokenization failed: {result.error}")
            return result
        
        return ProcessingResult(success=True)
    
    def run_continuous_monitoring(self, endpoint, interval, max_iterations):
        """Run continuous monitoring with optional preprocessing"""
        self.logger.info("Starting continuous monitoring mode")
        print("Starting continuous monitoring...")
        print(f"Check interval: {interval} seconds")
        
        if max_iterations:
            print(f"Max iterations: {max_iterations}")
        else:
            print("Running indefinitely (Ctrl+C to stop)")
        
        auto_clean = self.config.get('preprocessing', {}).get('auto_clean', False)
        print(f"Auto-preprocessing: {'ENABLED' if auto_clean and self.preprocessor else 'DISABLED'}")
        print("-" * 50)
        
        iteration = 0
        
        try:
            while True:
                if max_iterations and iteration >= max_iterations:
                    print("Reached maximum iterations, stopping...")
                    break
                
                print(f"\nMonitoring iteration {iteration + 1}")
                
                # Run tokenization
                result = self.tokenizer.process_live_data(endpoint)
                
                if result.success:
                    print(f"Tokenized {result.data['token_count']} tokens")
                    self.logger.info(f"Iteration {iteration + 1}: Processed {result.data['token_count']} tokens successfully")
                    
                    # Run preprocessing if auto-clean is enabled
                    if auto_clean and self.preprocessor:
                        preprocess_result = self.preprocessor.process_json_file(result.data['filepath'])
                        
                        if preprocess_result.success:
                            retention_rate = (preprocess_result.data['processed_token_count'] / preprocess_result.data['original_token_count']) * 100
                            print(f"Cleaned: {preprocess_result.data['original_token_count']} -> {preprocess_result.data['processed_token_count']} tokens ({retention_rate:.1f}% retained)")
                            self.logger.info(f"Iteration {iteration + 1}: Preprocessing successful, retention rate: {retention_rate:.2f}%")
                        else:
                            print(f"Preprocessing failed: {preprocess_result.error}")
                            self.logger.warning(f"Iteration {iteration + 1}: Preprocessing failed: {preprocess_result.error}")
                else:
                    print(f"Tokenization failed: {result.error}")
                    self.logger.warning(f"Iteration {iteration + 1}: Processing failed: {result.error}")
                
                iteration += 1
                
                if max_iterations is None or iteration < max_iterations:
                    print(f"Waiting {interval} seconds for next check...")
                    time.sleep(interval)
                    
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
            self.logger.info("Monitoring stopped by user interrupt")
        
        return ProcessingResult(success=True)
    
    def run_preprocessing_only(self, input_dir=None, single_file=None):
        """Run preprocessing on existing tokenized files"""
        if not self.preprocessor:
            # Initialize preprocessor for preprocessing-only mode
            preprocessing_config = self.config.get('preprocessing', {})
            self.preprocessor = NepaliTextPreprocessor(
                input_dir=input_dir or self.config.get('output_dir', 'tokenized_data'),
                output_dir=self.config.get('cleaned_output_dir', 'cleaned_data'),
                config=preprocessing_config
            )
        
        if single_file:
            print(f"Preprocessing single file: {single_file}")
            print("-" * 50)
            
            result = self.preprocessor.process_json_file(single_file)
            
            if result.success:
                print("Preprocessing completed successfully!")
                print(f"Tokens: {result.data['original_token_count']} -> {result.data['processed_token_count']}")
                print(f"Saved to: {result.data['output_file']}")
                retention_rate = (result.data['processed_token_count'] / result.data['original_token_count']) * 100
                print(f"Retention rate: {retention_rate:.2f}%")
                return ProcessingResult(success=True)
            else:
                print(f"Preprocessing failed: {result.error}")
                return result
        else:
            print(f"Batch preprocessing files in: {input_dir or self.preprocessor.input_dir}")
            print("-" * 50)
            
            result = self.preprocessor.batch_process()
            
            if result.success:
                print(f"Successfully processed {result.data['files_processed']}/{result.data['total_files']} files")
                if result.data['total_original_tokens'] > 0:
                    overall_retention = (result.data['total_processed_tokens'] / result.data['total_original_tokens']) * 100
                    print(f"Overall retention rate: {overall_retention:.2f}%")
                return result
            else:
                print(f"Batch processing failed: {result.error}")
                return result

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Nepali Text Tokenizer - Process live news articles from server with optional preprocessing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --single                           # Process once using config.json
  %(prog)s --monitor                          # Start continuous monitoring
  %(prog)s --single --auto-clean              # Process once with auto-preprocessing
  %(prog)s --preprocess-only                  # Only run preprocessing on existing files
  %(prog)s --preprocess-only --file data.json # Preprocess single file
  %(prog)s --create-config                    # Create sample config.json
  %(prog)s --monitor --interval 600           # Monitor every 10 minutes
        """
    )
    
    # Main operation modes
    parser.add_argument('--single', action='store_true', 
                       help='Process data once and exit')
    parser.add_argument('--monitor', action='store_true', 
                       help='Start continuous monitoring')
    parser.add_argument('--preprocess-only', action='store_true', 
                       help='Only run preprocessing on existing tokenized files')
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
    parser.add_argument('--cleaned-output-dir', 
                       help='Cleaned data output directory (overrides config)')
    
    # Preprocessing options
    parser.add_argument('--auto-clean', action='store_true', 
                       help='Enable automatic preprocessing after tokenization')
    parser.add_argument('--preserve-numbers', action='store_true', default=True,
                       help='Preserve numerical data during preprocessing')
    parser.add_argument('--remove-stopwords', action='store_true',
                       help='Remove Nepali stopwords during preprocessing')
    parser.add_argument('--normalize-numerals', action='store_true',
                       help='Convert Devanagari numerals to Arabic during preprocessing')
    parser.add_argument('--file', 
                       help='Single file to preprocess (use with --preprocess-only)')
    
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
    if args.cleaned_output_dir:
        config['cleaned_output_dir'] = args.cleaned_output_dir
    
    # Preprocessing overrides
    if not config.get('preprocessing'):
        config['preprocessing'] = {}
    
    if args.auto_clean:
        config['preprocessing']['auto_clean'] = True
    if args.preserve_numbers:
        config['preprocessing']['preserve_numbers'] = True
    if args.remove_stopwords:
        config['preprocessing']['remove_stopwords'] = True
    if args.normalize_numerals:
        config['preprocessing']['normalize_devanagari_numerals'] = True
    
    return config

def display_configuration(config, log_file):
    """Display current configuration"""
    print("Configuration:")
    print(f"   Server: {config['server_url']}")
    print(f"   Endpoint: {config['api_endpoint']}")
    print(f"   Output Dir: {config['output_dir']}")
    
    if config.get('preprocessing', {}).get('enabled') or config.get('preprocessing', {}).get('auto_clean'):
        print(f"   Cleaned Output Dir: {config.get('cleaned_output_dir', 'cleaned_data')}")
        print(f"   Auto-preprocessing: {'ENABLED' if config.get('preprocessing', {}).get('auto_clean', False) else 'DISABLED'}")
    
    print(f"   Log File: {log_file}")
    print()

def main():
    """Main application entry point"""
    # Initialize logging first
    log_file = setup_logging(log_prefix="nepali_tokenizer")
    logger = logging.getLogger(__name__)
    logger.info("Starting Nepali Text Tokenizer application")
    
    args = parse_arguments()
    
    # Handle config creation
    if args.create_config:
        if create_sample_config():
            print("Sample config.json created successfully.")
            print("Please update with your server details.")
            print("Tip: Set 'preprocessing.enabled': true to enable automatic preprocessing")
        else:
            print("Error: Failed to create sample config.json")
            return 1
        return 0
    
    # Require at least one operation mode
    if not (args.single or args.monitor or args.preprocess_only):
        print("Error: Please specify either --single, --monitor, or --preprocess-only")
        return 1
    
    # Load and validate configuration
    config = load_config(args.config)
    logger.info(f"Loaded configuration from {args.config}")
    
    # Apply command line overrides
    config = apply_config_overrides(config, args)
    
    # Validate configuration for tokenization modes
    if not args.preprocess_only:
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
        
        # Initialize components unless it's preprocessing-only mode
        if not args.preprocess_only:
            init_result = processor.initialize_components()
            if not init_result.success:
                print(f"Error: {init_result.error}")
                return 1
        
    except Exception as e:
        print(f"Error initializing processor: {e}")
        logger.error(f"Failed to initialize processor: {e}")
        return 1
    
    # Run selected operation
    try:
        if args.preprocess_only:
            logger.info("Starting preprocessing-only mode")
            result = processor.run_preprocessing_only(single_file=args.file)
            
        elif args.single:
            logger.info("Starting single processing mode")
            api_endpoint = config.get('api_endpoint', "/news/api.php")
            result = processor.run_single_processing(api_endpoint)
            
        elif args.monitor:
            logger.info("Starting continuous monitoring mode")
            api_endpoint = config.get('api_endpoint', "/news/api.php")
            interval = config.get('monitoring_interval', 300)
            max_iterations = config.get('max_iterations')
            result = processor.run_continuous_monitoring(api_endpoint, interval, max_iterations)
        
        return 0 if result.success else 1
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())