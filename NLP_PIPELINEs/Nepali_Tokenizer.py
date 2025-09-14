
from indicnlp.tokenize import indic_tokenize
import json
import requests
import time
import logging
from datetime import datetime
from typing import Optional, List, Dict
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set path to Indic NLP Resources - change this path accordingly
INDIC_NLP_RESOURCES = r"D:/A Internship Documents/Project G/indic_nlp_resources"

class NepaliTextTokenizer:
    def __init__(self, server_url: str, output_dir: str = "tokenized_data"):
        self.server_url = server_url
        self.output_dir = output_dir
        self.ensure_output_directory()
        
    def ensure_output_directory(self):
        """Create output directory if it doesn't exist"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
    
    def fetch_live_articles(self, endpoint: str = "/news/api.php", timeout: int = 10) -> Optional[List[Dict]]:
        """
        Fetch multiple live articles from the Nepali news API

        Args:
            endpoint: API endpoint to fetch data from
            timeout: Request timeout in seconds

        Returns:
            List of articles (each is a dict) or None if failed
        """
        try:
            url = f"{self.server_url}{endpoint}"
            logger.info(f"Fetching data from: {url}")

            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                logger.error("Unexpected data format: Expected a list of articles")
                return None
            
            logger.info(f"Successfully fetched {len(data)} articles")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from server: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def tokenize_text(self, text: str) -> List[str]:
        """
        Tokenize Nepali text using Indic NLP
        
        Args:
            text: Input text to tokenize
            
        Returns:
            List of tokens
        """
        try:
            if not text or not text.strip():
                logger.warning("Empty text provided for tokenization")
                return []
                
            tokens = list(indic_tokenize.trivial_tokenize(text.strip()))
            logger.info(f"Tokenized text into {len(tokens)} tokens")
            return tokens
            
        except Exception as e:
            logger.error(f"Error tokenizing text: {e}")
            return []
    
    def save_tokens(self, tokens: List[str], filename: Optional[str] = None) -> str:
        """
        Save tokens to JSON file
        
        Args:
            tokens: List of tokens to save
            filename: Optional custom filename
            
        Returns:
            Path to saved file
        """
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"article_tokens_{timestamp}.json"
                
            filepath = os.path.join(self.output_dir, filename)
            
            # Create metadata
            metadata = {
                "timestamp": datetime.now().isoformat(),
                "token_count": len(tokens),
                "tokens": tokens
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Tokens saved to: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving tokens: {e}")
            return ""
    
    def process_live_data(self, endpoint: str = "/news/api.php") -> Dict:
        """
        Complete pipeline: fetch multiple articles, tokenize titles/descriptions, and save tokens

        Args:
            endpoint: API endpoint to fetch from

        Returns:
            Dictionary with processing results
        """
        result = {
            "success": False,
            "tokens": [],
            "filepath": "",
            "error": None
        }

        try:
            articles = self.fetch_live_articles(endpoint)
            if not articles:
                result["error"] = "Failed to fetch articles from server"
                return result

            all_tokens = []
            for article in articles:
                # Use 'title' and/or 'description' for tokenization
                text_to_tokenize = article.get('title', '') + " " + article.get('description', '')
                tokens = self.tokenize_text(text_to_tokenize)
                all_tokens.extend(tokens)

            if not all_tokens:
                result["error"] = "No tokens generated from articles"
                return result

            filepath = self.save_tokens(all_tokens)

            if not filepath:
                result["error"] = "Failed to save tokens"
                return result

            result.update({
                "success": True,
                "tokens": all_tokens,
                "filepath": filepath,
                "token_count": len(all_tokens)
            })

            logger.info(f"Successfully processed live data: {len(all_tokens)} tokens")
            return result

        except Exception as e:
            logger.error(f"Error in processing pipeline: {e}")
            result["error"] = str(e)
            return result

    
    def continuous_monitoring(self, endpoint: str = "/api/articles", 
                            interval: int = 300, max_iterations: int = None):
        """
        Continuously monitor server for new data
        
        Args:
            endpoint: API endpoint to monitor
            interval: Time interval between checks (seconds)
            max_iterations: Maximum number of iterations (None for infinite)
        """
        iteration = 0
        logger.info(f"Starting continuous monitoring (interval: {interval}s)")
        
        try:
            while True:
                if max_iterations and iteration >= max_iterations:
                    logger.info("Reached maximum iterations, stopping...")
                    break
                
                logger.info(f"Monitoring iteration {iteration + 1}")
                result = self.process_live_data(endpoint)
                
                if result["success"]:
                    logger.info(f"Processed {result['token_count']} tokens successfully")
                else:
                    logger.warning(f"Processing failed: {result['error']}")
                
                iteration += 1
                
                if max_iterations is None or iteration < max_iterations:
                    logger.info(f"Waiting {interval} seconds for next check...")
                    time.sleep(interval)
                    
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {e}")

# Remove main function - will be in separate file