#!/usr/bin/env python3
"""
Shared utilities for Nepali Text Processing System
Centralizes common functionality to reduce code duplication
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
                  log_prefix: str = "nepali_processor") -> str:
    """
    Set up logging to both file and console
    
    Args:
        log_dir: Directory to store log files
        log_level: Logging level
        log_prefix: Prefix for log filename
        
    Returns:
        Path to the log file
    """
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
    logger.info("="*60)
    return log_filename

def ensure_directory(directory_path: str) -> bool:
    """
    Create directory if it doesn't exist
    
    Args:
        directory_path: Path to directory to create
        
    Returns:
        True if directory exists or was created successfully
    """
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logging.getLogger(__name__).info(f"Created directory: {directory_path}")
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to create directory {directory_path}: {e}")
        return False

def generate_timestamp_filename(prefix: str, suffix: str = ".json") -> str:
    """
    Generate filename with timestamp
    
    Args:
        prefix: Filename prefix
        suffix: File extension
        
    Returns:
        Filename with timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}{suffix}"

def save_json_data(data: Dict[str, Any], filepath: str) -> bool:
    """
    Save data to JSON file with error handling
    
    Args:
        data: Data to save
        filepath: Output file path
        
    Returns:
        True if saved successfully
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.getLogger(__name__).info(f"Data saved to: {filepath}")
        return True
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to save data to {filepath}: {e}")
        return False

def load_json_data(filepath: str) -> Optional[Dict[str, Any]]:
    """
    Load data from JSON file with error handling
    
    Args:
        filepath: Input file path
        
    Returns:
        Loaded data or None if failed
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to load data from {filepath}: {e}")
        return None

def get_default_config() -> Dict[str, Any]:
    """
    Get complete default configuration for the system
    
    Returns:
        Default configuration dictionary
    """
    return {
        "server_url": "https://apinp.com",
        "api_endpoint": "/news/api.php",
        "output_dir": "tokenized_data",
        "cleaned_output_dir": "cleaned_data",
        "monitoring_interval": 300,
        "max_iterations": None,
        "timeout": 10,
        "preprocessing": {
            "enabled": False,
            "auto_clean": False,
            "preserve_numbers": True,
            "normalize_unicode": True,
            "remove_whitespace": True,
            "standardize_punctuation": True,
            "remove_urls": True,
            "remove_emails": True,
            "language_filtering": True,
            "length_filtering": True,
            "min_token_length": 1,
            "max_token_length": 50,
            "remove_stopwords": False,
            "normalize_devanagari_numerals": False,
            "remove_duplicates": True,
            "remove_excessive_special_chars": True
        }
    }

def load_config(config_file: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from JSON file with fallback to defaults
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
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
    """
    Create a sample configuration file
    
    Args:
        filename: Configuration filename
        
    Returns:
        True if created successfully
    """
    config = get_default_config()
    return save_json_data(config, filename)

def validate_config(config: Dict[str, Any]) -> ProcessingResult:
    """
    Validate configuration completeness and correctness
    
    Args:
        config: Configuration to validate
        
    Returns:
        ProcessingResult with validation status
    """
    result = ProcessingResult()
    
    required_fields = ['server_url', 'api_endpoint', 'output_dir']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        result.error = f"Missing required configuration fields: {', '.join(missing_fields)}"
        return result
    
    if config.get('server_url') == "https://your-news-server.com":
        result.error = "Please configure a valid server_url"
        return result
    
    result.success = True
    return result

def create_metadata_dict(original_data: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
    """
    Create standardized metadata dictionary
    
    Args:
        original_data: Original data to include
        **kwargs: Additional metadata fields
        
    Returns:
        Metadata dictionary
    """
    metadata = {
        "timestamp": datetime.now().isoformat(),
        **kwargs
    }
    
    if original_data:
        metadata.update(original_data)
    
    return metadata