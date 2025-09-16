#!/usr/bin/env python3
"""
Nepali Text Processor - Application orchestration and control
Unified controller for all processing modes (single, batch, monitoring)
"""

import time
import logging
import json
from pathlib import Path
from typing import Dict

from Nepali_text_cleaner import NepaliTextCleaner
from utils import ProcessingResult, load_json_data, calculate_text_statistics

class NepaliTextProcessor:
    """Main application controller for Nepali text processing operations"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.cleaner = None
        self.logger = logging.getLogger(__name__)
        
    def initialize_cleaner(self) -> ProcessingResult:
        """Initialize the text cleaner component"""
        result = ProcessingResult()
        
        try:
            cleaning_config = self.config.get('cleaning', {})
            self.cleaner = NepaliTextCleaner(
                server_url=self.config.get('server_url', "https://apinp.com"),
                output_dir=self.config.get('output_dir', "cleaned_data"),
                config=cleaning_config
            )
            self.logger.info("Text cleaner initialized successfully")
            result.success = True
            
        except Exception as e:
            result.error = f"Failed to initialize text cleaner: {e}"
            self.logger.error(result.error)
        
        return result
    
    def run_single_processing(self, endpoint: str) -> ProcessingResult:
        """Process live data once and exit"""
        self.logger.info("Starting single processing mode")
        print("Starting single data cleaning...")
        print("-" * 50)
        
        try:
            # Run cleaning
            result = self.cleaner.process_live_data(endpoint)
            
            if result.success:
                print("Data cleaning completed successfully!")
                print(f"Articles fetched: {result.data['articles_fetched']}")
                print(f"Articles cleaned: {result.data['articles_cleaned']}")
                print(f"Retention rate: {result.data['retention_rate']:.2f}%")
                print(f"Saved to: {result.data['filepath']}")
                
                # Display additional statistics
                self._display_file_statistics(result.data['filepath'])
                
            else:
                print(f"Data cleaning failed: {result.error}")
                return result
            
            return ProcessingResult(success=True)
            
        except Exception as e:
            error_msg = f"Error in single processing: {e}"
            self.logger.error(error_msg)
            return ProcessingResult(error=error_msg)
    
    def run_continuous_monitoring(self, endpoint: str, interval: int, max_iterations: int = None) -> ProcessingResult:
        """Run continuous monitoring with periodic cleaning"""
        self.logger.info("Starting continuous monitoring mode")
        print("Starting continuous monitoring...")
        print(f"Check interval: {interval} seconds")
        
        if max_iterations:
            print(f"Max iterations: {max_iterations}")
        else:
            print("Running indefinitely (Ctrl+C to stop)")
        print("-" * 50)
        
        iteration = 0
        
        try:
            while True:
                if max_iterations and iteration >= max_iterations:
                    print("Reached maximum iterations, stopping...")
                    break
                
                print(f"\nMonitoring iteration {iteration + 1}")
                
                # Run cleaning
                result = self.cleaner.process_live_data(endpoint)
                
                if result.success:
                    print(f"✓ Cleaned {result.data['articles_cleaned']} articles from {result.data['articles_fetched']} fetched")
                    print(f"  Retention rate: {result.data['retention_rate']:.1f}%")
                    print(f"  Saved to: {Path(result.data['filepath']).name}")
                    self.logger.info(f"Iteration {iteration + 1}: Successfully processed {result.data['articles_cleaned']} articles")
                else:
                    print(f"✗ Processing failed: {result.error}")
                    self.logger.warning(f"Iteration {iteration + 1}: Processing failed: {result.error}")
                
                iteration += 1
                
                if max_iterations is None or iteration < max_iterations:
                    print(f"Waiting {interval} seconds for next check...")
                    time.sleep(interval)
                    
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
            self.logger.info("Monitoring stopped by user interrupt")
        except Exception as e:
            error_msg = f"Error in continuous monitoring: {e}"
            self.logger.error(error_msg)
            return ProcessingResult(error=error_msg)
        
        return ProcessingResult(success=True)
    
    def run_batch_processing(self, input_dir: str) -> ProcessingResult:
        """Process existing JSON files containing raw articles"""
        self.logger.info("Starting batch processing mode")
        print(f"Processing files from: {input_dir}")
        print("-" * 50)
        
        try:
            input_path = Path(input_dir)
            
            if not input_path.exists():
                error_msg = f"Input directory does not exist: {input_dir}"
                self.logger.error(error_msg)
                return ProcessingResult(error=error_msg)
            
            # Find JSON files in the input directory
            json_files = list(input_path.glob("*.json"))
            
            if not json_files:
                error_msg = f"No JSON files found in: {input_dir}"
                self.logger.error(error_msg)
                return ProcessingResult(error=error_msg)
            
            print(f"Found {len(json_files)} JSON files to process")
            
            total_articles_processed = 0
            total_articles_cleaned = 0
            processed_files = 0
            failed_files = []
            
            for json_file in json_files:
                print(f"\nProcessing: {json_file.name}")
                
                # Load raw articles from file
                raw_data = load_json_data(str(json_file))
                if not raw_data:
                    print(f"  ✗ Failed to load {json_file.name}")
                    failed_files.append(json_file.name)
                    continue
                
                # Extract articles from loaded data
                articles = self._extract_articles_from_data(raw_data)
                if not articles:
                    print(f"  ✗ No valid articles found in {json_file.name}")
                    failed_files.append(json_file.name)
                    continue
                
                # Process articles
                process_result = self.cleaner.process_articles(articles)
                if not process_result.success:
                    print(f"  ✗ Processing failed: {process_result.error}")
                    failed_files.append(json_file.name)
                    continue
                
                # Save cleaned data with source filename reference
                output_filename = f"cleaned_{json_file.stem}_{time.strftime('%Y%m%d_%H%M%S')}.json"
                save_result = self.cleaner.save_cleaned_data(
                    process_result.data, 
                    filename=output_filename
                )
                
                if save_result.success:
                    articles_in_file = len(articles)
                    articles_cleaned = process_result.data['cleaned_count']
                    retention_rate = process_result.data['retention_rate']
                    
                    print(f"  ✓ Processed {articles_cleaned}/{articles_in_file} articles ({retention_rate:.1f}% retention)")
                    print(f"  ✓ Saved to: {output_filename}")
                    
                    total_articles_processed += articles_in_file
                    total_articles_cleaned += articles_cleaned
                    processed_files += 1
                else:
                    print(f"  ✗ Failed to save cleaned data: {save_result.error}")
                    failed_files.append(json_file.name)
            
            # Display final summary
            print("\n" + "=" * 60)
            print("BATCH PROCESSING SUMMARY")
            print(f"Files processed: {processed_files}/{len(json_files)}")
            print(f"Total articles processed: {total_articles_processed}")
            print(f"Total articles cleaned: {total_articles_cleaned}")
            
            if total_articles_processed > 0:
                overall_retention = (total_articles_cleaned / total_articles_processed) * 100
                print(f"Overall retention rate: {overall_retention:.2f}%")
            
            if failed_files:
                print(f"Failed files: {', '.join(failed_files)}")
            
            print("=" * 60)
            
            self.logger.info(f"Batch processing completed: {processed_files}/{len(json_files)} files processed")
            
            return ProcessingResult(
                success=True,
                data={
                    "files_processed": processed_files,
                    "total_files": len(json_files),
                    "total_articles_processed": total_articles_processed,
                    "total_articles_cleaned": total_articles_cleaned,
                    "failed_files": failed_files
                }
            )
            
        except Exception as e:
            error_msg = f"Error in batch processing: {e}"
            self.logger.error(error_msg)
            return ProcessingResult(error=error_msg)
    
    def _extract_articles_from_data(self, raw_data: dict) -> list:
        """Extract articles from loaded JSON data with flexible structure handling"""
        try:
            # Handle different possible data structures
            if isinstance(raw_data, list):
                return raw_data
            
            # Check for common keys that might contain article arrays
            for key in ['articles', 'data', 'items', 'content', 'posts']:
                if key in raw_data and isinstance(raw_data[key], list):
                    return raw_data[key]
            
            # If raw_data itself looks like an article, wrap it in a list
            if self._looks_like_article(raw_data):
                return [raw_data]
            
            # Check if there are article-like objects at the top level
            articles = []
            for key, value in raw_data.items():
                if isinstance(value, dict) and self._looks_like_article(value):
                    articles.append(value)
            
            return articles
            
        except Exception as e:
            self.logger.error(f"Error extracting articles from data: {e}")
            return []
    
    def _looks_like_article(self, item: dict) -> bool:
        """Check if a dictionary looks like an article"""
        if not isinstance(item, dict):
            return False
        
        # Check for common article fields
        text_fields = ['title', 'content', 'description', 'text', 'body']
        has_text_field = any(field in item and isinstance(item[field], str) and item[field].strip() 
                           for field in text_fields)
        
        return has_text_field
    
    def _display_file_statistics(self, filepath: str):
        """Display statistics about the processed file"""
        try:
            data = load_json_data(filepath)
            if not data or 'articles' not in data:
                return
            
            stats = calculate_text_statistics(data['articles'])
            
            print("\nFile Statistics:")
            print(f"  Total characters: {stats.get('total_characters', 0):,}")
            print(f"  Average length: {stats.get('average_length', 0):.0f} chars")
            print(f"  Length range: {stats.get('min_length', 0)}-{stats.get('max_length', 0)} chars")
            
        except Exception as e:
            self.logger.warning(f"Could not display file statistics: {e}")