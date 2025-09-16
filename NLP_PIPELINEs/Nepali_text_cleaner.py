#!/usr/bin/env python3
"""
Nepali Text Cleaner - Core text cleaning functionality
Pure business logic for cleaning Nepali text without CLI dependencies
"""

import re
import unicodedata
import logging
import requests
from typing import List, Dict, Optional
from pathlib import Path

from utils import ProcessingResult, ensure_directory, save_json_data, create_metadata_dict, generate_timestamp_filename

logger = logging.getLogger(__name__)

class NepaliTextCleaner:
    """Core text cleaning functionality for Nepali content"""
    
    def __init__(self, server_url: str, output_dir: str = "cleaned_data", config: Optional[Dict] = None):
        self.server_url = server_url
        self.output_dir = output_dir
        self.config = config or self._get_default_cleaning_config()
        
        if not ensure_directory(self.output_dir):
            raise RuntimeError(f"Failed to create output directory: {self.output_dir}")
        
        self._initialize_components()
        
    def _get_default_cleaning_config(self) -> Dict:
        """Default cleaning configuration"""
        return {
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
    
    def _initialize_components(self):
        """Initialize cleaning components"""
        # Devanagari to Arabic numeral mapping
        self.devanagari_to_arabic = {
            '०': '0', '१': '1', '२': '2', '३': '3', '४': '4',
            '५': '5', '६': '6', '७': '7', '८': '8', '९': '9'
        }
        
        # Compile regex patterns for efficiency
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for better performance"""
        # URL pattern
        self.url_pattern = re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        )
        
        # Email pattern
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
        # HTML tags
        self.html_pattern = re.compile(r'<[^>]+>')
        
        # Excessive punctuation (3+ consecutive punctuation marks)
        self.excessive_punct_pattern = re.compile(r'[^\w\s\u0900-\u097F]{3,}')
        
        # Multiple whitespace
        self.whitespace_pattern = re.compile(r'\s+')
        
        # Number patterns (to preserve)
        self._compile_number_patterns()
    
    def _compile_number_patterns(self):
        """Compile number-related regex patterns"""
        self.number_patterns = [
            # Percentages: 50%, ५०%
            re.compile(r'[\d०-९]+\.?[\d०-९]*\s*%'),
            # Currency: रु. 100, Rs. 100
            re.compile(r'(?:रु\.?|Rs\.?)\s*[\d०-९,]+\.?[\d०-९]*'),
            # Measurements: 5 km, ५ किमी
            re.compile(r'[\d०-९]+\.?[\d०-९]*\s*(?:किमी|मिटर|लिटर|kg|km|m|l|cm|mm)'),
            # Dates: 2024, २०२४
            re.compile(r'(?:19|20|२०)[\d०-९]{2}'),
            # General numbers with decimal: 45.6, ४५.६
            re.compile(r'[\d०-९]+\.[\d०-९]+'),
            # Numbers with commas: 1,234, १,२३४
            re.compile(r'[\d०-९]{1,3}(?:,[\d०-९]{3})*')
        ]
    
    def fetch_live_articles(self, endpoint: str = "/news/api.php", timeout: int = 10) -> ProcessingResult:
        """Fetch multiple live articles from the Nepali news API"""
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
    
    def is_number_context(self, text: str, start: int, end: int) -> bool:
        """Check if text segment is in a number context"""
        if not self.config["preserve_numbers"]:
            return False
            
        segment = text[start:end].strip()
        for pattern in self.number_patterns:
            if pattern.search(segment):
                return True
        return False
    
    def normalize_unicode(self, text: str) -> str:
        """Normalize Unicode representation"""
        if not self.config["normalize_unicode"]:
            return text
        return unicodedata.normalize('NFC', text)
    
    def remove_html_tags(self, text: str) -> str:
        """Remove HTML tags"""
        if not self.config["remove_html_tags"]:
            return text
        return self.html_pattern.sub(' ', text)
    
    def remove_urls(self, text: str) -> str:
        """Remove URLs from text"""
        if not self.config["remove_urls"]:
            return text
        return self.url_pattern.sub(' ', text)
    
    def remove_emails(self, text: str) -> str:
        """Remove email addresses from text"""
        if not self.config["remove_emails"]:
            return text
        return self.email_pattern.sub(' ', text)
    
    def normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace while preserving sentence structure"""
        if not self.config["remove_extra_whitespace"]:
            return text
        
        # Replace multiple whitespace with single space
        text = self.whitespace_pattern.sub(' ', text)
        
        # Clean up whitespace around punctuation while preserving sentence structure
        if self.config["preserve_sentence_structure"]:
            # Fix spacing around Devanagari punctuation
            text = re.sub(r'\s*।\s*', '। ', text)  # Devanagari full stop
            text = re.sub(r'\s*?\s*', '? ', text)  # Question mark
            text = re.sub(r'\s*!\s*', '! ', text)  # Exclamation
            text = re.sub(r'\s*,\s*', ', ', text)  # Comma
        
        return text.strip()
    
    def standardize_punctuation(self, text: str) -> str:
        """Standardize punctuation"""
        if not self.config["standardize_punctuation"]:
            return text
            
        # Convert Devanagari punctuation to standard forms
        text = text.replace('॥', '।')  # Double danda to single danda
        
        # Remove excessive punctuation but preserve emphasis
        if self.config["remove_excessive_punctuation"]:
            # Replace 3+ consecutive punctuation with 2
            text = re.sub(r'([।.!?]){3,}', r'\1\1', text)
            text = self.excessive_punct_pattern.sub('', text)
        
        return text
    
    def normalize_devanagari_numerals(self, text: str) -> str:
        """Convert Devanagari numerals to Arabic numerals"""
        if not self.config["normalize_devanagari_numerals"]:
            return text
            
        for devanagari, arabic in self.devanagari_to_arabic.items():
            text = text.replace(devanagari, arabic)
        
        return text
    
    def filter_by_length(self, text: str) -> bool:
        """Filter text by length"""
        length = len(text.strip())
        return self.config["min_text_length"] <= length <= self.config["max_text_length"]
    
    def clean_text(self, text: str) -> Optional[str]:
        """Apply all cleaning steps to text"""
        if not text or not text.strip():
            return None
            
        # Phase 1: Basic Cleaning
        text = self.normalize_unicode(text)
        text = self.remove_html_tags(text)
        text = self.remove_urls(text)
        text = self.remove_emails(text)
        
        # Phase 2: Punctuation and Whitespace
        text = self.standardize_punctuation(text)
        text = self.normalize_whitespace(text)
        
        # Phase 3: Optional Normalization
        text = self.normalize_devanagari_numerals(text)
        
        # Phase 4: Length Filtering
        if not self.filter_by_length(text):
            logger.debug(f"Text filtered by length: {len(text)} chars")
            return None
        
        cleaned_text = text.strip()
        return cleaned_text if cleaned_text else None
    
    def process_articles(self, articles: List[Dict]) -> ProcessingResult:
        """Process multiple articles and clean their content"""
        result = ProcessingResult()
    
        try:
            cleaned_articles = []
            original_count = 0
            cleaned_count = 0
            
            for article in articles:
                # Extract text from article (title + description + content)
                text_parts = []
                
                if article.get('title'):
                    text_parts.append(article['title'])
                if article.get('description'):
                    text_parts.append(article['description'])
                if article.get('content'):
                    text_parts.append(article['content'])
                
                if not text_parts:
                    continue
                
                # Combine all text
                combined_text = ' '.join(text_parts)
                original_count += 1
                
                # Clean the text
                cleaned_text = self.clean_text(combined_text)
                
                if cleaned_text:
                    cleaned_article = {
                        'original_id': article.get('id', f'article_{original_count}'),
                        'original_title': article.get('title', ''),
                        # 'cleaned_content': cleaned_text,  # Added the actual cleaned content
                        'original_length': len(combined_text),
                        # 'cleaned_length': len(cleaned_text),  # Now both lengths are consistent
                        'metadata': {
                            'published_date': article.get('published_date'),
                            'category': article.get('category'),
                            'source': article.get('source')
                        }
                    }
                    cleaned_articles.append(cleaned_article)
                    cleaned_count += 1
            
            logger.info(f"Processed {original_count} articles, {cleaned_count} successfully cleaned")
            
            result.success = True
            result.data = {
                "articles": cleaned_articles,
                "original_count": original_count,
                "cleaned_count": cleaned_count,
                "retention_rate": (cleaned_count / original_count * 100) if original_count > 0 else 0
            }
            
        except Exception as e:
            result.error = f"Error processing articles: {e}"
            logger.error(result.error)
        
        return result
    
    def save_cleaned_data(self, processed_data: Dict, filename: Optional[str] = None) -> ProcessingResult:
        """Save cleaned data to JSON file"""
        result = ProcessingResult()
        
        try:
            if not filename:
                filename = generate_timestamp_filename("cleaned_articles")
                
            filepath = Path(self.output_dir) / filename
            
            # Create metadata
            metadata = create_metadata_dict(
                cleaning_config=self.config,
                total_articles=processed_data.get('cleaned_count', 0),
                retention_rate=processed_data.get('retention_rate', 0),
                articles=processed_data.get('articles', [])
            )
            
            if save_json_data(metadata, str(filepath)):
                result.success = True
                result.data = {"filepath": str(filepath)}
                logger.info(f"Cleaned data saved successfully to {filepath}")
            else:
                result.error = "Failed to save cleaned data to file"
            
            return result
            
        except Exception as e:
            result.error = f"Error saving cleaned data: {e}"
            logger.error(result.error)
            return result
    
    def process_live_data(self, endpoint: str = "/news/api.php") -> ProcessingResult:
        """Complete pipeline: fetch articles, clean content, and save"""
        # Fetch articles
        fetch_result = self.fetch_live_articles(endpoint)
        if not fetch_result.success:
            return fetch_result

        articles = fetch_result.data["articles"]
        
        # Process and clean articles
        process_result = self.process_articles(articles)
        if not process_result.success:
            return process_result

        # Save cleaned data
        save_result = self.save_cleaned_data(process_result.data)
        if not save_result.success:
            return save_result

        # Return comprehensive result
        result = ProcessingResult(success=True)
        result.data = {
            "filepath": save_result.data["filepath"],
            "articles_fetched": len(articles),
            "articles_cleaned": process_result.data["cleaned_count"],
            "retention_rate": process_result.data["retention_rate"]
        }

        logger.info(f"Successfully processed live data: {process_result.data['cleaned_count']} cleaned articles from {len(articles)} fetched")
        return result