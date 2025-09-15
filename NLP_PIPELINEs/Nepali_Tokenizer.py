#!/usr/bin/env python3
"""
Nepali Text Tokenizer
Handles fetching and tokenizing Nepali news articles
"""

from indicnlp.tokenize import indic_tokenize
import requests
import time
import logging
from typing import Optional, List, Dict
import os
from utils import ProcessingResult, ensure_directory, generate_timestamp_filename, save_json_data, create_metadata_dict

# Set up module logger
logger = logging.getLogger(__name__)

# Set path to Indic NLP Resources - change this path accordingly
INDIC_NLP_RESOURCES = r"D:/A Internship Documents/Project G/indic_nlp_resources"

class NepaliTextTokenizer:
    def __init__(self, server_url: str, output_dir: str = "tokenized_data"):
        self.server_url = server_url
        self.output_dir = output_dir
        
        if not ensure_directory(self.output_dir):
            raise RuntimeError(f"Failed to create output directory: {self.output_dir}")
    
    def fetch_live_articles(self, endpoint: str = "/news/api.php", timeout: int = 10) -> ProcessingResult:
        """
        Fetch multiple live articles from the Nepali news API

        Args:
            endpoint: API endpoint to fetch data from
            timeout: Request timeout in seconds

        Returns:
            ProcessingResult with articles data or error
        """
        result = ProcessingResult()
        
        try:
            url = f"{self.server_url}{endpoint}"
            logger.info(f"Fetching data from: {url}")

            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                result.error = "Unexpected data format: Expected a list of articles"
                return result
            
            logger.info(f"Successfully fetched {len(data)} articles")
            result.success = True
            result.data = {"articles": data}
            return result

        except requests.exceptions.RequestException as e:
            result.error = f"Network error: {e}"
            logger.error(result.error)
        except ValueError as e:  # JSON decode error
            result.error = f"Invalid JSON response: {e}"
            logger.error(result.error)
        except Exception as e:
            result.error = f"Unexpected error: {e}"
            logger.error(result.error)
        
        return result

    def tokenize_text(self, text: str) -> ProcessingResult:
        """
        Tokenize Nepali text using Indic NLP
        
        Args:
            text: Input text to tokenize
            
        Returns:
            ProcessingResult with tokens or error
        """
        result = ProcessingResult()
        
        try:
            if not text or not text.strip():
                logger.warning("Empty text provided for tokenization")
                result.success = True
                result.data = {"tokens": []}
                return result
                
            tokens = list(indic_tokenize.trivial_tokenize(text.strip()))
            logger.info(f"Tokenized text into {len(tokens)} tokens")
            
            result.success = True
            result.data = {"tokens": tokens}
            return result
            
        except Exception as e:
            result.error = f"Tokenization failed: {e}"
            logger.error(result.error)
            return result
    
    def save_tokens(self, tokens: List[str], filename: Optional[str] = None) -> ProcessingResult:
        """
        Save tokens to JSON file
        
        Args:
            tokens: List of tokens to save
            filename: Optional custom filename
            
        Returns:
            ProcessingResult with file path or error
        """
        result = ProcessingResult()
        
        try:
            if not filename:
                filename = generate_timestamp_filename("article_tokens")
                
            filepath = os.path.join(self.output_dir, filename)
            
            # Create metadata
            metadata = create_metadata_dict(
                token_count=len(tokens),
                tokens=tokens
            )
            
            if save_json_data(metadata, filepath):
                result.success = True
                result.data = {"filepath": filepath}
                logger.info(f"Tokens saved successfully")
            else:
                result.error = "Failed to save tokens to file"
            
            return result
            
        except Exception as e:
            result.error = f"Error saving tokens: {e}"
            logger.error(result.error)
            return result
    
    def process_live_data(self, endpoint: str = "/news/api.php") -> ProcessingResult:
        """
        Complete pipeline: fetch multiple articles, tokenize titles/descriptions, and save tokens

        Args:
            endpoint: API endpoint to fetch from

        Returns:
            ProcessingResult with processing results
        """
        # Fetch articles
        fetch_result = self.fetch_live_articles(endpoint)
        if not fetch_result.success:
            return fetch_result

        articles = fetch_result.data["articles"]
        
        # Tokenize all articles
        all_tokens = []
        for article in articles:
            # Use 'title' and/or 'description' for tokenization
            text_to_tokenize = article.get('title', '') + " " + article.get('description', '')
            
            tokenize_result = self.tokenize_text(text_to_tokenize)
            if tokenize_result.success:
                all_tokens.extend(tokenize_result.data["tokens"])

        if not all_tokens:
            result = ProcessingResult()
            result.error = "No tokens generated from articles"
            return result

        # Save tokens
        save_result = self.save_tokens(all_tokens)
        if not save_result.success:
            return save_result

        # Return comprehensive result
        result = ProcessingResult(success=True)
        result.data = {
            "tokens": all_tokens,
            "filepath": save_result.data["filepath"],
            "token_count": len(all_tokens),
            "articles_processed": len(articles)
        }

        logger.info(f"Successfully processed live data: {len(all_tokens)} tokens from {len(articles)} articles")
        return result
    
    def continuous_monitoring(self, endpoint: str = "/api/articles", 
                            interval: int = 300, max_iterations: int = None) -> ProcessingResult:
        """
        Continuously monitor server for new data
        
        Args:
            endpoint: API endpoint to monitor
            interval: Time interval between checks (seconds)
            max_iterations: Maximum number of iterations (None for infinite)
            
        Returns:
            ProcessingResult with monitoring summary
        """
        result = ProcessingResult()
        iteration = 0
        successful_iterations = 0
        
        logger.info(f"Starting continuous monitoring (interval: {interval}s)")
        
        try:
            while True:
                if max_iterations and iteration >= max_iterations:
                    logger.info("Reached maximum iterations, stopping...")
                    break
                
                logger.info(f"Monitoring iteration {iteration + 1}")
                process_result = self.process_live_data(endpoint)
                
                if process_result.success:
                    logger.info(f"Processed {process_result.data['token_count']} tokens successfully")
                    successful_iterations += 1
                else:
                    logger.warning(f"Processing failed: {process_result.error}")
                
                iteration += 1
                
                if max_iterations is None or iteration < max_iterations:
                    logger.info(f"Waiting {interval} seconds for next check...")
                    time.sleep(interval)
                    
            result.success = True
            result.data = {
                "total_iterations": iteration,
                "successful_iterations": successful_iterations,
                "success_rate": (successful_iterations / iteration * 100) if iteration > 0 else 0
            }
                    
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
            result.success = True
            result.data = {
                "total_iterations": iteration,
                "successful_iterations": successful_iterations,
                "stopped_by_user": True
            }
        except Exception as e:
            result.error = f"Error in continuous monitoring: {e}"
            logger.error(result.error)

        return result