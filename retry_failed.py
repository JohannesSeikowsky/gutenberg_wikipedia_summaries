# -*- coding: utf-8 -*-
"""
Retry failed Wikipedia article downloads with enhanced diagnostics.
Reads failed entries from progress.json, attempts to re-download with detailed error analysis.
"""

import json
import os
import time
import re
import logging
import requests
from urllib.parse import unquote
from datetime import datetime
from collections import defaultdict

# Configuration
REQUEST_DELAY = 2.0
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 45]
USER_AGENT = "WikiBookScraper/1.0 (Educational project; Contact: joseikowsky@gmail.com)"

# File paths
PROGRESS_FILE = "progress.json"
ARTICLES_DIR = "articles"
RETRY_LOG = "retry_diagnostics.log"
RETRY_RESULTS = "retry_results.json"


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_progress():
    """Load progress from JSON file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_progress(progress):
    """Save progress to JSON file."""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2)


def extract_language_code(url):
    """Extract language code from Wikipedia URL."""
    match = re.search(r'https?://([a-z\-]{2,7})\.wikipedia\.org', url)
    if match:
        return match.group(1)
    return 'unknown'


def extract_page_title(url):
    """Extract page title from Wikipedia URL."""
    try:
        url = url.strip()
        match = re.search(r'/wiki/(.+)$', url)
        if match:
            return unquote(match.group(1))
        return None
    except Exception as e:
        logging.error(f"Error extracting page title from {url}: {e}")
        return None


def diagnose_and_fetch(url, lang, page_title, retry_count=0):
    """Fetch article with detailed diagnostics."""
    api_url = f"https://{lang}.wikipedia.org/w/api.php"

    # First, get page info with extracts
    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'extracts|info|pageprops',
        'explaintext': True,
        'titles': page_title,
        'inprop': 'url'
    }

    headers = {'User-Agent': USER_AGENT}

    try:
        response = requests.get(api_url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)

        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAYS[retry_count]
                logging.warning(f"Rate limited. Waiting {wait_time}s")
                time.sleep(wait_time)
                return diagnose_and_fetch(url, lang, page_title, retry_count + 1)
            return False, "Rate limited - max retries exceeded", {}

        if response.status_code == 503:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAYS[retry_count]
                logging.warning(f"Service unavailable. Waiting {wait_time}s")
                time.sleep(wait_time)
                return diagnose_and_fetch(url, lang, page_title, retry_count + 1)
            return False, "Service unavailable - max retries exceeded", {}

        response.raise_for_status()
        data = response.json()

        pages = data.get('query', {}).get('pages', {})
        if not pages:
            return False, "Empty API response", {"api_returned_no_pages": True}

        page = next(iter(pages.values()))
        page_id = page.get('pageid', 'N/A')

        diagnostics = {
            "page_id": page_id,
            "lang": lang,
            "page_title": page_title
        }

        # Check for missing page
        if 'missing' in page:
            diagnostics['missing'] = True
            return False, "Page does not exist (404)", diagnostics

        # Check for invalid title
        if 'invalid' in page:
            reason = page.get('invalidreason', 'unknown')
            diagnostics['invalid'] = reason
            return False, f"Invalid page title: {reason}", diagnostics

        # Get extract
        content = page.get('extract', '')
        diagnostics['extract_length'] = len(content)

        if not content:
            # Check if it's a disambiguation page
            pageprops = page.get('pageprops', {})
            if 'disambiguation' in pageprops:
                diagnostics['is_disambiguation'] = True
                return False, "Disambiguation page (no article content)", diagnostics

            # Try getting raw content
            rev_params = {
                'action': 'query',
                'format': 'json',
                'prop': 'revisions',
                'titles': page_title,
                'rvprop': 'size|content',
                'rvslots': 'main'
            }
            rev_response = requests.get(api_url, params=rev_params, headers=headers, timeout=REQUEST_TIMEOUT)
            rev_data = rev_response.json()
            rev_pages = rev_data.get('query', {}).get('pages', {})
            rev_page = next(iter(rev_pages.values()))

            revisions = rev_page.get('revisions', [])
            if revisions:
                size = revisions[0].get('size', 0)
                diagnostics['raw_size'] = size

                if size == 0:
                    return False, "Page exists but has zero content", diagnostics
                else:
                    # Check if it's a redirect
                    content_text = revisions[0].get('slots', {}).get('main', {}).get('*', '')
                    if content_text.startswith('#REDIRECT'):
                        diagnostics['is_redirect'] = True
                        return False, "Page is a redirect", diagnostics

                    return False, "Page has content but extract failed (likely special page type)", diagnostics
            else:
                return False, "No revisions found", diagnostics

        return True, content, diagnostics

    except requests.exceptions.ConnectionError:
        return False, "Connection error", {"connection_error": True}
    except requests.exceptions.Timeout:
        return False, f"Request timeout after {REQUEST_TIMEOUT}s", {"timeout": True}
    except Exception as e:
        return False, f"Unexpected error: {str(e)}", {"exception": str(e)}


def save_article(content, filepath):
    """Save article content to a file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception as e:
        logging.error(f"Error saving file {filepath}: {e}")
        return False


def main():
    """Main retry logic."""
    setup_logging()

    logging.info("=" * 60)
    logging.info("Retry Failed Wikipedia Articles")
    logging.info("=" * 60)

    # Load progress
    progress = load_progress()
    logging.info(f"Loaded progress: {len(progress)} total entries")

    # Find failed entries
    failed_entries = [(key, value) for key, value in progress.items()
                      if value.get('status') == 'failed']

    logging.info(f"Found {len(failed_entries)} failed entries to retry")

    if not failed_entries:
        logging.info("No failed entries to retry!")
        return

    # Track results
    results = {
        'total': len(failed_entries),
        'successful': 0,
        'still_failed': 0,
        'details': []
    }

    # Track filename counters
    language_counters = defaultdict(int)

    # Scan existing files to initialize counters
    if os.path.exists(ARTICLES_DIR):
        for filename in os.listdir(ARTICLES_DIR):
            match = re.match(r'(\d+)_([a-z\-]+)(?:_(\d+))?\.txt', filename)
            if match:
                book_id, lang, counter = match.groups()
                lang_key = f"{book_id}_{lang}"
                count = int(counter) if counter else 1
                language_counters[lang_key] = max(language_counters[lang_key], count)

    # Process each failed entry
    for idx, (key, old_value) in enumerate(failed_entries, 1):
        book_id, url = key.split('|', 1)

        logging.info(f"[{idx}/{len(failed_entries)}] Retrying book_id={book_id}")
        logging.info(f"  URL: {url}")

        # Extract language and page title
        lang = extract_language_code(url)
        page_title = extract_page_title(url)

        if not page_title:
            error_msg = "Could not extract page title"
            logging.error(f"  [FAIL] {error_msg}")
            results['still_failed'] += 1
            results['details'].append({
                'book_id': book_id,
                'url': url,
                'status': 'failed',
                'error': error_msg,
                'diagnostics': {'title_extraction_failed': True}
            })
            continue

        # Attempt fetch with diagnostics
        success, result, diagnostics = diagnose_and_fetch(url, lang, page_title)

        detail_entry = {
            'book_id': book_id,
            'url': url,
            'lang': lang,
            'page_title': page_title,
            'diagnostics': diagnostics
        }

        if success:
            # Generate filename
            lang_key = f"{book_id}_{lang}"
            language_counters[lang_key] += 1

            if language_counters[lang_key] == 1:
                filename = f"{book_id}_{lang}.txt"
            else:
                filename = f"{book_id}_{lang}_{language_counters[lang_key]}.txt"

            filepath = os.path.join(ARTICLES_DIR, filename)

            # Save article
            if save_article(result, filepath):
                logging.info(f"  [SUCCESS] Saved to {filename}")
                progress[key] = {
                    'status': 'success',
                    'filename': filename,
                    'timestamp': datetime.now().isoformat()
                }
                results['successful'] += 1
                detail_entry['status'] = 'success'
                detail_entry['filename'] = filename
            else:
                logging.error(f"  [FAIL] Could not save file")
                results['still_failed'] += 1
                detail_entry['status'] = 'failed'
                detail_entry['error'] = 'File save failed'
        else:
            logging.error(f"  [FAIL] {result}")
            for key_d, val_d in diagnostics.items():
                logging.info(f"    {key_d}: {val_d}")
            results['still_failed'] += 1
            detail_entry['status'] = 'failed'
            detail_entry['error'] = result

        results['details'].append(detail_entry)

        # Rate limiting delay
        time.sleep(REQUEST_DELAY)

    # Save updated progress
    save_progress(progress)

    # Save results
    with open(RETRY_RESULTS, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)

    # Summary
    logging.info("=" * 60)
    logging.info("Retry Complete")
    logging.info("=" * 60)
    logging.info(f"Total retried: {results['total']}")
    logging.info(f"Successful: {results['successful']}")
    logging.info(f"Still failed: {results['still_failed']}")
    logging.info(f"Detailed results saved to {RETRY_RESULTS}")


if __name__ == "__main__":
    main()
