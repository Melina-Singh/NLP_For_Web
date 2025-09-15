import logging
import os
from datetime import datetime

def setup_logging(log_dir="logs", log_level=logging.INFO):
    """
    Set up logging configuration to write to both file and console
    Call this function at the start of your main.py
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"tokenizer_{timestamp}.log")
    
    # Configure logging to write to both file and console
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),  # File handler
            logging.StreamHandler()  # Console handler
        ]
    )
    
    # Log the setup
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    logger.info("="*60)
    
    return log_filename