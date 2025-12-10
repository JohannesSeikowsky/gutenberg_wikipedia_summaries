# -*- coding: utf-8 -*-
"""
Wikipedia Article Scraper
Fetches Wikipedia article content from URLs in all_book_wiki_links.txt
and saves them as plain text files with language codes in filenames.
"""

import os
import json
import time
import re
import logging
from urllib.parse import unquote
from datetime import datetime
from collections import defaultdict
import requests

# Configuration
REQUEST_DELAY = 2.0  # Seconds between requests
REQUEST_TIMEOUT = 30  # Seconds
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 45]  # Exponential backoff in seconds
USER_AGENT = "WikiBookScraper/1.0 (Educational project; Contact: joseikowsky@gmail.com)"

# File paths
INPUT_FILE = "all_book_wiki_links.txt"
ARTICLES_DIR = "articles"
PROGRESS_FILE = "progress.json"
ERROR_LOG = "errors.log"
SUMMARY_FILE = "summary.txt"


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_directories():
    """Create necessary directories if they don't exist."""
    os.makedirs(ARTICLES_DIR, exist_ok=True)


def extract_language_code(url):
    """Extract language code from Wikipedia URL.

    Args:
        url: Wikipedia URL (e.g., https://en.wikipedia.org/wiki/...)

    Returns:
        Language code (e.g., 'en', 'fr', 'de') or 'unknown'
    """
    match = re.search(r'https?://([a-z]{2,3})\.wikipedia\.org', url)
    if match:
        return match.group(1)
    return 'unknown'


def extract_page_title(url):
    """Extract page title from Wikipedia URL.

    Args:
        url: Wikipedia URL

    Returns:
        Decoded page title or None if invalid
    """
    try:
        # Remove trailing whitespace and carriage returns
        url = url.strip().rstrip('\r\n')

        # Extract the part after /wiki/
        match = re.search(r'/wiki/(.+)$', url)
        if match:
            # URL decode the title
            title = unquote(match.group(1))
            return title
        return None
    except Exception as e:
        logging.error(f"Error extracting page title from {url}: {e}")
        return None


def fetch_wikipedia_content(url, lang, page_title, retry_count=0):
    """Fetch Wikipedia article content using the Wikipedia API.

    Args:
        url: Original Wikipedia URL
        lang: Language code
        page_title: Page title
        retry_count: Current retry attempt

    Returns:
        Tuple of (success: bool, content: str or error_message: str)
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"

    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'extracts',
        'explaintext': True,
        'redirects': 1,
        'titles': page_title
    }

    headers = {
        'User-Agent': USER_AGENT
    }

    try:
        response = requests.get(
            api_url,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

        # Handle rate limiting
        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAYS[retry_count]
                logging.warning(f"Rate limited. Waiting {wait_time}s before retry {retry_count + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                return fetch_wikipedia_content(url, lang, page_title, retry_count + 1)
            else:
                return False, "Rate limited - max retries exceeded"

        # Handle service unavailable
        if response.status_code == 503:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAYS[retry_count]
                logging.warning(f"Service unavailable. Waiting {wait_time}s before retry {retry_count + 1}/{MAX_RETRIES}")
                time.sleep(wait_time)
                return fetch_wikipedia_content(url, lang, page_title, retry_count + 1)
            else:
                return False, "Service unavailable - max retries exceeded"

        # Check for HTTP errors
        response.raise_for_status()

        # Parse JSON response
        data = response.json()

        # Extract page content
        pages = data.get('query', {}).get('pages', {})

        if not pages:
            return False, "Empty API response"

        # Get the first (and should be only) page
        page = next(iter(pages.values()))

        # Check if page exists
        if 'missing' in page:
            return False, "Page does not exist (404)"

        # Get the extract (article content)
        content = page.get('extract', '')

        if not content:
            return False, "Empty article content"

        return True, content

    except requests.exceptions.Timeout:
        return False, f"Request timeout after {REQUEST_TIMEOUT}s"
    except requests.exceptions.ConnectionError:
        return False, "Connection error"
    except requests.exceptions.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, "Invalid JSON response"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def save_article(content, filepath):
    """Save article content to a file.

    Args:
        content: Article text content
        filepath: Path to save the file

    Returns:
        True if successful, False otherwise
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception as e:
        logging.error(f"Error saving file {filepath}: {e}")
        return False


def load_progress():
    """Load progress from JSON file.

    Returns:
        Dictionary of completed book_id|url combinations
    """
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Could not load progress file: {e}. Starting fresh.")
            return {}
    return {}


def save_progress_entry(progress, book_id, url, status, filename=None):
    """Save a single progress entry and write to file.

    Args:
        progress: Progress dictionary
        book_id: Book ID
        url: Wikipedia URL
        status: 'success' or 'failed'
        filename: Output filename (if successful)
    """
    key = f"{book_id}|{url}"
    progress[key] = {
        'status': status,
        'filename': filename,
        'timestamp': datetime.now().isoformat()
    }

    # Write progress to file
    try:
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving progress: {e}")


def log_error(book_id, url, error_message):
    """Log an error to the error log file.

    Args:
        book_id: Book ID
        url: Wikipedia URL
        error_message: Error description
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] book_id={book_id}, url={url}, error={error_message}\n"

    try:
        with open(ERROR_LOG, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        logging.error(f"Error writing to error log: {e}")


def generate_summary(stats):
    """Generate and save a summary report.

    Args:
        stats: Dictionary with statistics
    """
    summary = f"""Wikipedia Article Scraper - Summary Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Total Books Processed: {stats['total_books']}
Total URLs Processed: {stats['total_urls']}
Successful Downloads: {stats['successful']}
Failed Downloads: {stats['failed']}
Skipped (already done): {stats['skipped']}

Success Rate: {stats['successful'] / max(stats['total_urls'] - stats['skipped'], 1) * 100:.2f}%

Failed Book IDs:
"""

    if stats['failed_book_ids']:
        for book_id in sorted(set(stats['failed_book_ids'])):
            summary += f"  - {book_id}\n"
    else:
        summary += "  None\n"

    try:
        with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
            f.write(summary)
        logging.info(f"Summary saved to {SUMMARY_FILE}")
    except Exception as e:
        logging.error(f"Error saving summary: {e}")


def process_line(line, progress, stats, language_counters):
    """Process a single line from the input file.

    Args:
        line: Line from input file
        progress: Progress dictionary
        stats: Statistics dictionary
        language_counters: Dictionary tracking file counts per book_id and language
    """
    line = line.strip()
    if not line:
        return

    # Split by comma to get book_id and URLs
    parts = line.split(',', 1)
    if len(parts) != 2:
        logging.warning(f"Invalid line format: {line}")
        return

    book_id = parts[0].strip()
    urls_part = parts[1].strip()

    # Split URLs by whitespace
    urls = urls_part.split()

    logging.info(f"[{stats['processed_books']}/{stats['total_books']}] Processing book_id={book_id} ({len(urls)} URL(s))")

    for url in urls:
        url = url.strip()
        if not url:
            continue

        # Check if already processed
        key = f"{book_id}|{url}"
        if key in progress:
            logging.info(f"  [SKIP] {url} (already processed)")
            stats['skipped'] += 1
            stats['total_urls'] += 1
            continue

        stats['total_urls'] += 1

        # Extract language and page title
        lang = extract_language_code(url)
        page_title = extract_page_title(url)

        if not page_title:
            error_msg = "Could not extract page title"
            logging.error(f"  [FAIL] {url}: {error_msg}")
            log_error(book_id, url, error_msg)
            save_progress_entry(progress, book_id, url, 'failed')
            stats['failed'] += 1
            stats['failed_book_ids'].append(book_id)
            continue

        # Generate filename with language code
        # Track counter for this book_id + language combination
        lang_key = f"{book_id}_{lang}"
        language_counters[lang_key] += 1

        if language_counters[lang_key] == 1:
            filename = f"{book_id}_{lang}.txt"
        else:
            filename = f"{book_id}_{lang}_{language_counters[lang_key]}.txt"

        filepath = os.path.join(ARTICLES_DIR, filename)

        # Fetch article content
        success, result = fetch_wikipedia_content(url, lang, page_title)

        if success:
            # Save to file
            if save_article(result, filepath):
                logging.info(f"  [OK] {filename}")
                save_progress_entry(progress, book_id, url, 'success', filename)
                stats['successful'] += 1
            else:
                error_msg = "Failed to save file"
                logging.error(f"  [FAIL] {filename}: {error_msg}")
                log_error(book_id, url, error_msg)
                save_progress_entry(progress, book_id, url, 'failed')
                stats['failed'] += 1
                stats['failed_book_ids'].append(book_id)
        else:
            logging.error(f"  [FAIL] {url}: {result}")
            log_error(book_id, url, result)
            save_progress_entry(progress, book_id, url, 'failed')
            stats['failed'] += 1
            stats['failed_book_ids'].append(book_id)

        # Rate limiting delay
        time.sleep(REQUEST_DELAY)


def main():
    """Main function to orchestrate the scraping process."""
    setup_logging()
    create_directories()

    logging.info("=" * 60)
    logging.info("Wikipedia Article Scraper Started")
    logging.info("=" * 60)

    # Load progress
    progress = load_progress()
    logging.info(f"Loaded progress: {len(progress)} entries already processed")

    # Initialize statistics
    stats = {
        'total_books': 0,
        'processed_books': 0,
        'total_urls': 0,
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'failed_book_ids': []
    }

    # Track filename counters per book_id and language
    language_counters = defaultdict(int)

    # Count total lines first
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            stats['total_books'] = sum(1 for line in f if line.strip())
        logging.info(f"Total books to process: {stats['total_books']}")
    except FileNotFoundError:
        logging.error(f"Input file not found: {INPUT_FILE}")
        return
    except Exception as e:
        logging.error(f"Error reading input file: {e}")
        return

    # Process each line
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                stats['processed_books'] += 1
                process_line(line, progress, stats, language_counters)
    except KeyboardInterrupt:
        logging.info("\nProcess interrupted by user. Progress has been saved.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

    # Generate summary
    logging.info("=" * 60)
    logging.info("Processing Complete")
    logging.info("=" * 60)
    logging.info(f"Total Books: {stats['total_books']}")
    logging.info(f"Total URLs: {stats['total_urls']}")
    logging.info(f"Successful: {stats['successful']}")
    logging.info(f"Failed: {stats['failed']}")
    logging.info(f"Skipped: {stats['skipped']}")

    generate_summary(stats)


if __name__ == "__main__":
    main()
