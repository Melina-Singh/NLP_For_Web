#!/usr/bin/env python3
"""
Shared utilities for Nepali Raw Text Cleaning System
Centralized configuration, file operations, and common functionality
"""

import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

class ProcessingResult:
    """Standardized result format for all operations"""
    def __init__(self, success: bool = False, error: Optional[str] = None):
        self.success = success
        self.error = error
        self.data = {}
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "success": self.success,
            "error": self.error
        }
        result.update(self.data)
        return result

def setup_logging(log_dir: str = "logs", log_level: int = logging.INFO, 
                  log_prefix: str = "nepali_text_cleaner") -> str:
    """Set up logging to both file and console"""
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"{log_prefix}_{timestamp}.log")
    
    # Remove any existing handlers to avoid duplicates
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    return log_filename

def ensure_directory(directory_path: str) -> bool:
    """Create directory if it doesn't exist"""
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logging.getLogger(__name__).info(f"Created directory: {directory_path}")
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to create directory {directory_path}: {e}")
        return False

def generate_timestamp_filename(prefix: str, suffix: str = ".json") -> str:
    """Generate filename with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}{suffix}"

def save_json_data(data: Dict[str, Any], filepath: str) -> bool:
    """Save data to JSON file with error handling"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.getLogger(__name__).info(f"Data saved to: {filepath}")
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to save data to {filepath}: {e}")
        return False

def load_json_data(filepath: str) -> Optional[Dict[str, Any]]:
    """Load data from JSON file with error handling"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to load data from {filepath}: {e}")
        return None

def get_default_config() -> Dict[str, Any]:
    """Get complete default configuration"""
    return {
        "server_url": "https://apinp.com",
        "api_endpoint": "/news/api.php",
        "output_dir": "cleaned_data",
        "monitoring_interval": 300,
        "max_iterations": None,
        "timeout": 10,
        "cleaning": {
            "preserve_numbers": True,
            "normalize_unicode": True,
            "remove_extra_whitespace": True,
            "standardize_punctuation": True,
            "remove_urls": True,
            "remove_emails": True,
            "normalize_devanagari_numerals": False,
            "remove_html_tags": True,
            "remove_excessive_punctuation": True,
            "preserve_sentence_structure": True,
            "min_text_length": 10,
            "max_text_length": 10000
        }
    }

def load_config(config_file: str = "config.json") -> Dict[str, Any]:
    """Load configuration from JSON file with fallback to defaults"""
    logger = logging.getLogger(__name__)
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"Loaded configuration from {config_file}")
        return config
    except FileNotFoundError:
        logger.warning(f"Config file {config_file} not found. Using default settings.")
        return get_default_config()
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing config file: {e}")
        return get_default_config()

def create_sample_config(filename: str = "config.json") -> bool:
    """Create a sample configuration file"""
    config = get_default_config()
    
    config_with_comments = {
        "_comment": "Nepali Raw Text Cleaner Configuration File",
        "_instructions": {
            "server_url": "URL of your Nepali news server",
            "api_endpoint": "API endpoint that returns JSON array of articles",
            "output_dir": "Directory to save cleaned text files",
            "monitoring_interval": "Seconds between checks in monitoring mode",
            "max_iterations": "Maximum iterations for monitoring (null for infinite)",
            "cleaning": {
                "preserve_numbers": "Keep numerical data in context",
                "normalize_unicode": "Standardize Unicode representation",
                "remove_extra_whitespace": "Clean up spacing while preserving structure",
                "standardize_punctuation": "Convert Devanagari punctuation to standard",
                "remove_urls": "Remove web URLs from text",
                "remove_emails": "Remove email addresses from text",
                "normalize_devanagari_numerals": "Convert Devanagari numbers to Arabic",
                "remove_html_tags": "Strip HTML tags from content",
                "remove_excessive_punctuation": "Limit consecutive punctuation marks",
                "preserve_sentence_structure": "Maintain readability and flow",
                "min_text_length": "Minimum characters to keep an article",
                "max_text_length": "Maximum characters to keep an article"
            }
        }
    }
    config_with_comments.update(config)
    
    return save_json_data(config_with_comments, filename)

def validate_config(config: Dict[str, Any]) -> ProcessingResult:
    """Validate configuration completeness and correctness"""
    result = ProcessingResult()
    
    required_fields = ['server_url', 'api_endpoint', 'output_dir']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        result.error = f"Missing required configuration fields: {', '.join(missing_fields)}"
        return result
    
    if config.get('server_url') in ["https://your-news-server.com", "https://example.com"]:
        result.error = "Please configure a valid server_url"
        return result
    
    cleaning_config = config.get('cleaning', {})
    if cleaning_config:
        min_length = cleaning_config.get('min_text_length', 10)
        max_length = cleaning_config.get('max_text_length', 10000)
        
        if min_length >= max_length:
            result.error = "min_text_length must be less than max_text_length"
            return result
        
        if min_length < 1:
            result.error = "min_text_length must be at least 1"
            return result
    
    result.success = True
    return result

def create_metadata_dict(**kwargs) -> Dict[str, Any]:
    """Create standardized metadata dictionary"""
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "processing_type": "raw_text_cleaning",
        "system_version": "1.0.0",
        **kwargs
    }
    return metadata

def calculate_text_statistics(articles: list) -> Dict[str, Any]:
    """Calculate statistics for processed articles"""
    if not articles:
        return {}
    
    lengths = [len(article.get('cleaned_content', '')) for article in articles]
    
    return {
        "total_articles": len(articles),
        "total_characters": sum(lengths),
        "average_length": sum(lengths) / len(lengths),
        "min_length": min(lengths) if lengths else 0,
        "max_length": max(lengths) if lengths else 0,
        "median_length": sorted(lengths)[len(lengths) // 2] if lengths else 0
    }

def validate_article_structure(article: Dict[str, Any]) -> bool:
    """Validate that an article has the expected structure"""
    text_fields = ['title', 'description', 'content', 'text', 'body']
    has_text = any(article.get(field) and article[field].strip() for field in text_fields)
    return has_text

def log_processing_summary(logger: logging.Logger, 
                          articles_fetched: int, 
                          articles_processed: int, 
                          processing_time: float,
                          output_file: str = None):
    """Log a summary of processing results"""
    retention_rate = (articles_processed / articles_fetched * 100) if articles_fetched > 0 else 0
    
    logger.info("="*60)
    logger.info("PROCESSING SUMMARY")
    logger.info(f"Articles fetched: {articles_fetched}")
    logger.info(f"Articles processed: {articles_processed}")
    logger.info(f"Retention rate: {retention_rate:.2f}%")
    logger.info(f"Processing time: {processing_time:.2f}s")
    if output_file:
        logger.info(f"Output saved to: {output_file}")
    logger.info("="*60)