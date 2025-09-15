#!/usr/bin/env python3
"""
Nepali Text Preprocessor
Handles cleaning and preprocessing of tokenized Nepali text data
Preserves numerical information while removing noise
"""

import re
import unicodedata
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import argparse

from utils import (
    ProcessingResult, 
    ensure_directory, 
    save_json_data, 
    load_json_data,
    create_metadata_dict,
    generate_timestamp_filename
)

logger = logging.getLogger(__name__)

class NepaliTextPreprocessor:
    def __init__(self, input_dir: str = "tokenized_data", output_dir: str = "cleaned_data", config: Optional[Dict] = None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.config = config or self._get_default_preprocessing_config()
        
        if not ensure_directory(self.output_dir):
            raise RuntimeError(f"Failed to create output directory: {self.output_dir}")
        
        # Initialize preprocessing components
        self._initialize_components()
        
    def _get_default_preprocessing_config(self) -> Dict:
        """Default preprocessing configuration"""
        return {
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
    
    def _initialize_components(self):
        """Initialize preprocessing components"""
        # Devanagari to Arabic numeral mapping
        self.devanagari_to_arabic = {
            '०': '0', '१': '1', '२': '2', '३': '3', '४': '4',
            '५': '5', '६': '6', '७': '7', '८': '8', '९': '9'
        }
        
        # Load Nepali stop words
        self.nepali_stopwords = self._get_nepali_stopwords()
        
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
        
        # Excessive special characters (3+ consecutive special chars)
        self.excessive_special_pattern = re.compile(r'[^\w\s\u0900-\u097F]{3,}')
        
        # Number patterns (to preserve)
        self._compile_number_patterns()
        
        # Devanagari script range
        self.devanagari_pattern = re.compile(r'[\u0900-\u097F]+')
        
        # English words
        self.english_pattern = re.compile(r'[a-zA-Z]+')
        
        # Whitespace normalization
        self.whitespace_pattern = re.compile(r'\s+')
    
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
            re.compile(r'[\d०-९]{1,3}(?:,[\d०-९]{3})*'),
            # Simple integers: 123, १२३
            re.compile(r'[\d०-९]+')
        ]
    
    def _get_nepali_stopwords(self) -> set:
        """Get Nepali stopwords set"""
        return {
            "अब", "अगाडि", "अझै", "अक्सर", "अलग", "आठ", "आजको", "आठ", "आदि", "आत्म", "आफू",
            "आफूलाई", "आफैलाई", "आफ्नो", "आफ्नै", "आयो", "उदाहरण", "उन", "उनको", "उनले",
            "उप", "उहाँलाई", "एउटै", "एक", "एकदम", "औं", "कतै", "कम से कम", "कसरी", "कसै",
            "कसैले", "कहाँ", "कहाँबाट", "कहिलेकाहीं", "कहिल्यै", "कहीं", "का", "कि", "किन",
            "किनभने", "कुनै", "कुरा", "कृपया", "के", "केवल", "केहि", "केही", "को", "कोही", "क्रमशः",
            "गए", "गरि", "गरी", "गरेका", "गरेको", "गरेर", "गरौं", "गर्छ", "गर्छु", "गर्दै",
            "गर्न", "गर्नु", "गर्नुपर्छ", "गर्ने", "गर्यौं", "गैर", "चाँडै", "चार", "चाले",
            "चाहनुहुन्छ", "चाहन्छु", "चाहिए", "छ", "छन्", "छु", "छैन", "छौँ", "छौं", "जताततै",
            "जब", "जबकि", "जसको", "जसबाट", "जसमा", "जसलाई", "जसले", "जस्तै", "जस्तो",
            "जस्तोसुकै", "जहाँ", "जान", "जाहिर", "जुन", "जे", "जो", "ठीक", "त", "तत्काल",
            "तथा", "तदनुसार", "तपाइँको", "तपाईं", "तर", "तल", "तापनि", "तिनी", "तिनीहरू",
            "तिनीहरूको", "तिनीहरूलाई", "तिनीहरूले", "तिमी", "तिर", "ती", "तीन", "तुरुन्तै",
            "तेस्रो", "त्यसकारण", "त्यसपछि", "त्यसमा", "त्यसैले", "त्यहाँ", "त्यो", "थिए",
            "थिएन", "थिएनन्", "थियो", "दिए", "दिनुभएको", "दिनुहुन्छ", "दुई", "देख", "देखि",
            "देखिन्छ", "देखियो", "देखे", "देखेको", "देखेर", "देख्न", "दोश्रो", "दोस्रो",
            "धेरै", "न", "नजिकै", "नत्र", "नयाँ", "नि", "निम्ति", "निम्न", "निम्नानुसार",
            "निर्दिष्ट", "नै", "नौ", "पक्का", "पक्कै", "पछि", "पछिल्लो", "पटक", "पनि", "पर्छ",
            "पर्थ्यो", "पर्याप्त", "पहिले", "पहिलो", "पहिल्यै", "पाँच", "पाँचौं", "पूर्व",
            "प्रति", "प्रत्येक", "प्लस", "फेरि", "बने", "बन्द", "बन्न", "बरु", "बाटो", "बारे",
            "बाहिर", "बाहेक", "बीच", "बीचमा", "भए", "भएको", "भन", "भने", "भनेर", "भन्छन्",
            "भन्छु", "भन्दा", "भन्नुभयो", "भन्ने", "भर", "भित्र", "भित्री", "म", "मलाई", "मा",
            "मात्र", "माथि", "मुख्य", "मेरो", "यति", "यथोचित", "यदि", "यद्यपि", "यस", "यसको",
            "यसपछि", "यसबाहेक", "यसरी", "यसो", "यस्तो", "यहाँ", "यहाँसम्म", "या", "यी", "यो",
            "र", "रही", "रहेका", "रहेको", "राखे", "राख्छ", "राम्रो", "रूप", "लगभग", "लाई",
            "लागि", "ले", "वरिपरि", "वास्तवमा", "वाहेक", "विरुद्ध", "विशेष", "शायद", "सँग",
            "सँगै", "सक्छ", "सट्टा", "सधैं", "सबै", "सबैलाई", "समय", "सम्भव", "सम्म", "सही",
            "साँच्चै", "सात", "साथ", "साथै", "सायद", "सारा", "सो", "सोध्न", "सोही", "स्पष्ट",
            "हरे", "हरेक", "हामी", "हामीलाई", "हाम्रो", "हुँ", "हुन", "हुने", "हुनेछ", "हुन्",
            "हुन्छ", "हो", "होइन", "होइनन्", "होला", "होस्", "नै","पो","कि’", "म"
        }
    
    def is_number_token(self, token: str) -> bool:
        """Check if token is a number or number with context"""
        if not self.config["preserve_numbers"]:
            return False
            
        for pattern in self.number_patterns:
            if pattern.fullmatch(token.strip()):
                return True
        return False
    
    def normalize_unicode(self, text: str) -> str:
        """Normalize Unicode representation"""
        if not self.config["normalize_unicode"]:
            return text
        return unicodedata.normalize('NFC', text)
    
    def normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace"""
        if not self.config["remove_whitespace"]:
            return text
        return self.whitespace_pattern.sub(' ', text).strip()
    
    def standardize_punctuation(self, text: str) -> str:
        """Standardize punctuation while preserving numbers"""
        if not self.config["standardize_punctuation"]:
            return text
            
        # Only apply if it's not a number token
        if self.is_number_token(text):
            return text
            
        # Basic punctuation standardization
        text = text.replace('।', '.')  # Devanagari full stop to period
        text = text.replace('॥', '.')  # Double danda to period
        
        return text
    
    def remove_noise(self, token: str) -> Optional[str]:
        """Remove various types of noise"""
        # Skip if it's a number
        if self.is_number_token(token):
            return token
            
        # Remove URLs
        if self.config["remove_urls"] and self.url_pattern.search(token):
            logger.debug(f"Removed URL: {token}")
            return None
            
        # Remove emails
        if self.config["remove_emails"] and self.email_pattern.search(token):
            logger.debug(f"Removed email: {token}")
            return None
            
        # Remove excessive special characters
        if self.config["remove_excessive_special_chars"]:
            if self.excessive_special_pattern.search(token):
                logger.debug(f"Removed excessive special chars: {token}")
                return None
        
        return token
    
    def filter_by_language(self, token: str) -> bool:
        """Filter tokens by language (keep Nepali, numbers, and some English)"""
        if not self.config["language_filtering"]:
            return True
            
        # Always keep numbers
        if self.is_number_token(token):
            return True
            
        # Keep if contains Devanagari script
        if self.devanagari_pattern.search(token):
            return True
            
        # Keep English words that might be important
        if self.english_pattern.fullmatch(token) and len(token) > 2:
            return True
            
        # Keep mixed script tokens (common in Nepali text)
        if self.devanagari_pattern.search(token) and self.english_pattern.search(token):
            return True
            
        return False
    
    def filter_by_length(self, token: str) -> bool:
        """Filter tokens by length"""
        if not self.config["length_filtering"]:
            return True
            
        # Always keep numbers regardless of length
        if self.is_number_token(token):
            return True
            
        length = len(token)
        return self.config["min_token_length"] <= length <= self.config["max_token_length"]
    
    def remove_stopwords(self, token: str) -> bool:
        """Check if token should be removed as stopword"""
        if not self.config["remove_stopwords"]:
            return True
            
        # Never remove numbers
        if self.is_number_token(token):
            return True
            
        return token.lower() not in self.nepali_stopwords
    
    def normalize_devanagari_numerals(self, token: str) -> str:
        """Convert Devanagari numerals to Arabic numerals"""
        if not self.config["normalize_devanagari_numerals"]:
            return token
            
        for devanagari, arabic in self.devanagari_to_arabic.items():
            token = token.replace(devanagari, arabic)
        
        return token
    
    def preprocess_token(self, token: str) -> Optional[str]:
        """Apply all preprocessing steps to a single token"""
        original_token = token
        
        # Phase 1: Basic Cleaning
        token = self.normalize_unicode(token)
        token = self.normalize_whitespace(token)
        token = self.standardize_punctuation(token)
        
        # Phase 2: Content Filtering
        token = self.remove_noise(token)
        if token is None:
            return None
            
        if not self.filter_by_language(token):
            logger.debug(f"Filtered by language: {original_token}")
            return None
            
        if not self.filter_by_length(token):
            logger.debug(f"Filtered by length: {original_token}")
            return None
        
        # Phase 3: Semantic Cleaning
        if not self.remove_stopwords(token):
            logger.debug(f"Removed stopword: {original_token}")
            return None
            
        # Number normalization (if enabled)
        token = self.normalize_devanagari_numerals(token)
        
        return token if token.strip() else None
    
    def preprocess_tokens(self, tokens: List[str]) -> ProcessingResult:
        """Preprocess a list of tokens"""
        result = ProcessingResult()
        
        try:
            logger.info(f"Starting preprocessing of {len(tokens)} tokens")
            
            processed_tokens = []
            removed_count = 0
            
            for token in tokens:
                processed_token = self.preprocess_token(token)
                if processed_token is not None:
                    processed_tokens.append(processed_token)
                else:
                    removed_count += 1
            
            # Remove duplicates if enabled
            if self.config["remove_duplicates"]:
                original_length = len(processed_tokens)
                processed_tokens = list(dict.fromkeys(processed_tokens))  # Preserves order
                duplicates_removed = original_length - len(processed_tokens)
                logger.info(f"Removed {duplicates_removed} duplicate tokens")
            
            logger.info(f"Preprocessing complete: {len(processed_tokens)} tokens remaining, {removed_count} removed")
            
            result.success = True
            result.data = {
                "tokens": processed_tokens,
                "original_count": len(tokens),
                "processed_count": len(processed_tokens),
                "removed_count": removed_count
            }
            
        except Exception as e:
            result.error = f"Error during preprocessing: {e}"
            logger.error(result.error)
        
        return result
    
    def process_json_file(self, input_filepath: str, output_filepath: Optional[str] = None) -> ProcessingResult:
        """Process a single JSON file containing tokenized data"""
        result = ProcessingResult()
        
        try:
            # Load input data
            data = load_json_data(input_filepath)
            if not data:
                result.error = f"Failed to load data from {input_filepath}"
                return result
            
            if 'tokens' not in data:
                result.error = "JSON file does not contain 'tokens' field"
                return result
            
            original_tokens = data['tokens']
            
            # Preprocess tokens
            preprocess_result = self.preprocess_tokens(original_tokens)
            if not preprocess_result.success:
                result.error = preprocess_result.error
                return result
            
            processed_tokens = preprocess_result.data["tokens"]
            
            # Create output data with metadata
            output_data = create_metadata_dict(
                preprocessing_config=self.config,
                original_token_count=len(original_tokens),
                processed_token_count=len(processed_tokens),
                removed_token_count=len(original_tokens) - len(processed_tokens),
                source_file=input_filepath,
                tokens=processed_tokens
            )
            
            # Determine output filepath
            if not output_filepath:
                input_filename = Path(input_filepath).stem
                output_filepath = str(Path(self.output_dir) / generate_timestamp_filename(f"cleaned_{input_filename}"))
            
            # Save processed data
            if not save_json_data(output_data, output_filepath):
                result.error = f"Failed to save processed data to {output_filepath}"
                return result
            
            result.success = True
            result.data = {
                "input_file": input_filepath,
                "output_file": output_filepath,
                "original_token_count": len(original_tokens),
                "processed_token_count": len(processed_tokens)
            }
            
            logger.info(f"Successfully processed {input_filepath}")
            
        except Exception as e:
            result.error = f"Error processing {input_filepath}: {e}"
            logger.error(result.error)
        
        return result
    
    def batch_process(self, file_pattern: str = "*.json") -> ProcessingResult:
        """Process all JSON files in the input directory"""
        result = ProcessingResult()
        
        try:
            input_path = Path(self.input_dir)
            json_files = list(input_path.glob(file_pattern))
            
            if not json_files:
                result.error = f"No JSON files found in {self.input_dir}"
                logger.warning(result.error)
                return result
            
            logger.info(f"Found {len(json_files)} files to process")
            
            results = []
            for json_file in json_files:
                logger.info(f"Processing {json_file}")
                file_result = self.process_json_file(str(json_file))
                results.append(file_result)
            
            # Summary statistics
            successful = [r for r in results if r.success]
            total_original = sum(r.data.get("original_token_count", 0) for r in successful)
            total_processed = sum(r.data.get("processed_token_count", 0) for r in successful)
            
            logger.info("="*60)
            logger.info("BATCH PROCESSING SUMMARY")
            logger.info(f"Files processed: {len(successful)}/{len(results)}")
            logger.info(f"Total original tokens: {total_original:,}")
            logger.info(f"Total processed tokens: {total_processed:,}")
            if total_original > 0:
                logger.info(f"Overall retention rate: {(total_processed/total_original*100):.2f}%")
            logger.info("="*60)
            
            result.success = True
            result.data = {
                "files_processed": len(successful),
                "total_files": len(results),
                "results": results,
                "total_original_tokens": total_original,
                "total_processed_tokens": total_processed
            }
            
        except Exception as e:
            result.error = f"Error in batch processing: {e}"
            logger.error(result.error)
        
        return result

def main():
    """Command line interface for the preprocessor"""
    parser = argparse.ArgumentParser(
        description="Nepali Text Preprocessor - Clean tokenized Nepali text data",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--input-dir', default='tokenized_data',
                       help='Input directory containing JSON files')
    parser.add_argument('--output-dir', default='cleaned_data',
                       help='Output directory for cleaned files')
    parser.add_argument('--config', 
                       help='Configuration JSON file')
    parser.add_argument('--single-file',
                       help='Process single file instead of batch')
    parser.add_argument('--preserve-numbers', action='store_true', default=True,
                       help='Preserve numerical data')
    parser.add_argument('--remove-stopwords', action='store_true',
                       help='Remove Nepali stopwords')
    parser.add_argument('--normalize-numerals', action='store_true',
                       help='Convert Devanagari numerals to Arabic')
    
    args = parser.parse_args()
    
    # Load configuration
    config = None
    if args.config:
        config = load_json_data(args.config)
        if not config:
            logger.error(f"Failed to load configuration from {args.config}")
            return 1
        logger.info(f"Loaded configuration from {args.config}")
    
    # Initialize preprocessor
    try:
        preprocessor = NepaliTextPreprocessor(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            config=config
        )
        
        # Override config with command line arguments
        if args.preserve_numbers:
            preprocessor.config['preserve_numbers'] = True
        if args.remove_stopwords:
            preprocessor.config['remove_stopwords'] = True
        if args.normalize_numerals:
            preprocessor.config['normalize_devanagari_numerals'] = True
            
    except Exception as e:
        logger.error(f"Failed to initialize preprocessor: {e}")
        return 1
    
    # Process files
    try:
        if args.single_file:
            result = preprocessor.process_json_file(args.single_file)
            if result.success:
                print(f"Successfully processed {args.single_file}")
                print(f"Tokens: {result.data['original_token_count']} -> {result.data['processed_token_count']}")
            else:
                print(f"Failed to process {args.single_file}: {result.error}")
                return 1
        else:
            result = preprocessor.batch_process()
            if result.success:
                print(f"Successfully processed {result.data['files_processed']}/{result.data['total_files']} files")
            else:
                print(f"Batch processing failed: {result.error}")
                return 1
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())