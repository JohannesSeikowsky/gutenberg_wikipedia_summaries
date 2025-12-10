#!/usr/bin/env python3
"""Validate Wikipedia articles against book metadata using Claude API."""

import anthropic
import csv
import json
import os
import time
import re
import logging
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

# Configuration
REQUEST_DELAY = 2.5
ARTICLES_DIR = 'articles'
CSV_PATH = 'pg_catalog.csv'
PROGRESS_FILE = 'validation_progress.json'
MATCHES_FILE = 'validated_matches.csv'
MISMATCHES_FILE = 'validation_mismatches.csv'
ERROR_LOG = 'validation_errors.log'
SUMMARY_FILE = 'validation_summary.txt'
ARTICLE_EXCERPT_LENGTH = 2000

# Prompt template for Claude
VALIDATION_PROMPT_TEMPLATE = """Does this Wikipedia article match the work listed below?

WORK:
- Title: {title}
- Author(s): {authors}

WIKIPEDIA ARTICLE (first {excerpt_length} chars):
{article_excerpt}

Is the Wikipedia article about this work? Ignore edition details (translations, volumes, annotations).

Respond:
VERDICT: [YES/NO]
CONFIDENCE: [HIGH/MEDIUM/LOW]
REASONING: [one very short sentence]
"""


def setup_logging():
    """Configure logging for error tracking."""
    logging.basicConfig(
        filename=ERROR_LOG,
        level=logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Suppress httpx/anthropic HTTP request logs
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('anthropic').setLevel(logging.WARNING)


def load_book_metadata(csv_path):
    """Load book metadata from CSV into dict keyed by book_id."""
    metadata = {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        for row in reader:
            if len(row) < 7:
                continue

            book_id = row[0]
            title = row[3] if row[3] else "No title available"
            authors = row[5] if row[5] else "Unknown"

            metadata[book_id] = {
                'title': title,
                'authors': authors
            }

    print(f"Loaded metadata for {len(metadata)} books")
    return metadata


def load_wikipedia_url_mapping(links_file):
    """Load Wikipedia URLs from all_book_wiki_links.txt into dict keyed by book_id."""
    url_mapping = {}
    lang_pattern = r'https?://([a-z]{2,3})\.wikipedia\.org'

    with open(links_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split(',', 1)
            if len(parts) != 2:
                continue

            book_id = parts[0].strip()
            urls = parts[1].strip().split()

            url_list = []
            for url in urls:
                url = url.strip()
                lang_match = re.search(lang_pattern, url)
                lang = lang_match.group(1) if lang_match else 'unknown'
                url_list.append((url, lang))

            url_mapping[book_id] = url_list

    return url_mapping


def find_wikipedia_url(url_mapping, book_id, language):
    """Find Wikipedia URL for book_id matching the language."""
    if book_id not in url_mapping:
        return "URL_NOT_FOUND"

    urls = url_mapping[book_id]

    # Single URL case
    if len(urls) == 1:
        return urls[0][0]

    # Multiple URLs - match by language
    for url, lang in urls:
        if lang == language:
            return url

    # No language match - return first URL and log warning
    logging.warning(f"No language match for book_id={book_id}, lang={language}. Using first URL.")
    return urls[0][0]


def parse_article_filename(filename):
    """Extract book_id and language from filename."""
    pattern = r'^(\d+)_([a-z]{2,3})(?:_\d+)?\.txt$'
    match = re.match(pattern, filename)

    if match:
        return match.group(1), match.group(2)
    return None, None


def validate_article_with_claude(article_text, book_title, authors, api_client):
    """Call Claude API to validate article matches book."""
    article_excerpt = article_text[:ARTICLE_EXCERPT_LENGTH]

    prompt = VALIDATION_PROMPT_TEMPLATE.format(
        title=book_title,
        authors=authors,
        excerpt_length=ARTICLE_EXCERPT_LENGTH,
        article_excerpt=article_excerpt
    )

    max_retries = 3
    delays = [5, 15, 45]

    for attempt in range(max_retries):
        try:
            message = api_client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=500,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            response_text = message.content[0].text
            return parse_claude_response(response_text)

        except anthropic.RateLimitError as e:
            if attempt < max_retries - 1:
                sleep_time = delays[attempt]
                print(f"Rate limit hit, sleeping {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delays[attempt])
            else:
                raise


def parse_claude_response(response_text):
    """Parse Claude's response for verdict, confidence, and reasoning."""
    verdict_match = re.search(r'VERDICT:\s*(YES|NO)', response_text, re.IGNORECASE)
    confidence_match = re.search(r'CONFIDENCE:\s*(HIGH|MEDIUM|LOW)', response_text, re.IGNORECASE)
    reasoning_match = re.search(r'REASONING:\s*(.+)', response_text, re.IGNORECASE | re.DOTALL)

    is_match = verdict_match.group(1).upper() == 'YES' if verdict_match else False
    confidence = confidence_match.group(1).upper() if confidence_match else 'MEDIUM'
    reasoning = reasoning_match.group(1).strip() if reasoning_match else 'No reasoning provided'

    # Clean up reasoning (remove extra whitespace/newlines)
    reasoning = ' '.join(reasoning.split())

    return {
        'is_match': is_match,
        'confidence': confidence,
        'reasoning': reasoning
    }


def load_validation_progress():
    """Load validation progress from JSON file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_progress_entry(progress, filename, verdict, timestamp):
    """Save progress entry and write to file atomically."""
    progress[filename] = {
        'status': 'completed',
        'verdict': verdict,
        'timestamp': timestamp
    }

    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2)


def write_validation_result(file_handle, book_id, filename, language, title, authors, confidence, reasoning, wikipedia_url):
    """Write validation result to CSV file."""
    import csv
    import io

    # Escape quotes in fields
    def escape_field(text):
        return text.replace('"', '""')

    # Write using CSV format with quotes
    gutenberg_url = f"https://www.gutenberg.org/ebooks/{book_id}"
    # Truncate authors to 20 characters
    authors_truncated = authors[:20] + "..." if len(authors) > 20 else authors
    # Replace newlines in title and reasoning with spaces
    title_clean = title.replace('\n', ' ').replace('\r', ' ')
    reasoning_clean = reasoning.replace('\n', ' ').replace('\r', ' ')
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_ALL)
    writer.writerow([gutenberg_url, reasoning_clean, wikipedia_url, confidence, title_clean, authors_truncated, filename, book_id])

    file_handle.write(output.getvalue())
    file_handle.flush()


def generate_summary(stats, start_time):
    """Generate summary report with statistics."""
    end_time = datetime.now()
    duration = end_time - start_time

    total = stats['total_processed']
    matches = stats['matches']
    mismatches = stats['mismatches']
    errors = stats['errors']

    summary = f"""Wikipedia Article Validation - Summary Report
Generated: {end_time.strftime('%Y-%m-%d %H:%M:%S')}

Total Articles Processed: {total}
Validated Matches: {matches} ({100*matches/total:.1f}%)
Validation Mismatches: {mismatches} ({100*mismatches/total:.1f}%)
Errors/Skipped: {errors} ({100*errors/total:.1f}%)

Confidence Distribution (Matches):
  HIGH: {stats['match_confidence']['HIGH']} ({100*stats['match_confidence']['HIGH']/matches:.1f}% of matches)
  MEDIUM: {stats['match_confidence']['MEDIUM']} ({100*stats['match_confidence']['MEDIUM']/matches:.1f}% of matches)
  LOW: {stats['match_confidence']['LOW']} ({100*stats['match_confidence']['LOW']/matches:.1f}% of matches)

Confidence Distribution (Mismatches):
  HIGH: {stats['mismatch_confidence']['HIGH']} ({100*stats['mismatch_confidence']['HIGH']/mismatches:.1f}% of mismatches)
  MEDIUM: {stats['mismatch_confidence']['MEDIUM']} ({100*stats['mismatch_confidence']['MEDIUM']/mismatches:.1f}% of mismatches)
  LOW: {stats['mismatch_confidence']['LOW']} ({100*stats['mismatch_confidence']['LOW']/mismatches:.1f}% of mismatches)

Processing Time: {duration}
"""

    return summary


def main():
    """Main processing loop."""
    start_time = datetime.now()

    # Setup
    setup_logging()
    print("Wikipedia Article Validation System")
    print("=" * 50)

    # Load environment variables from .env file
    load_dotenv()

    # Get API key
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not found in environment or .env file")
        return

    # Initialize API client
    client = anthropic.Anthropic(api_key=api_key)

    # Load book metadata
    print("\nLoading book metadata from CSV...")
    try:
        metadata_dict = load_book_metadata(CSV_PATH)
    except Exception as e:
        print(f"ERROR: Failed to load CSV: {e}")
        logging.error(f"Failed to load CSV: {e}")
        return

    # Load Wikipedia URL mapping
    print("Loading Wikipedia URL mapping...")
    try:
        url_mapping = load_wikipedia_url_mapping('all_book_wiki_links.txt')
        print(f"Loaded URLs for {len(url_mapping)} books")
    except Exception as e:
        print(f"ERROR: Failed to load URL mapping: {e}")
        logging.error(f"Failed to load URL mapping: {e}")
        return

    # Load progress
    print("Loading validation progress...")
    progress_dict = load_validation_progress()
    print(f"Found {len(progress_dict)} previously processed articles")

    # Initialize output files
    print("Initializing output files...")
    matches_file = open(MATCHES_FILE, 'w', encoding='utf-8')
    mismatches_file = open(MISMATCHES_FILE, 'w', encoding='utf-8')

    # Write headers
    import csv
    import io

    header_output = io.StringIO()
    header_writer = csv.writer(header_output, quoting=csv.QUOTE_ALL)
    header_writer.writerow(['gutenberg_url', 'reasoning', 'wikipedia_url', 'confidence', 'title', 'authors', 'filename', 'book_id'])

    matches_file.write(header_output.getvalue())
    mismatches_file.write(header_output.getvalue())

    # Get list of article files
    print(f"Scanning {ARTICLES_DIR} directory...")
    article_files = sorted([f for f in os.listdir(ARTICLES_DIR) if f.endswith('.txt')])
    print(f"Found {len(article_files)} article files")

    # Initialize statistics
    stats = {
        'total_processed': 0,
        'matches': 0,
        'mismatches': 0,
        'errors': 0,
        'match_confidence': defaultdict(int),
        'mismatch_confidence': defaultdict(int)
    }

    # Process each article
    print(f"\nStarting validation (this will take ~{len(article_files) * REQUEST_DELAY / 3600:.1f} hours)...")
    print("-" * 50)

    for i, filename in enumerate(article_files):
        # Skip if already processed
        if filename in progress_dict:
            continue

        # Parse filename
        book_id, language = parse_article_filename(filename)
        if not book_id:
            error_msg = f"filename={filename}, error=Could not parse filename format"
            logging.info(error_msg)
            stats['errors'] += 1
            continue

        # Lookup metadata
        if book_id not in metadata_dict:
            error_msg = f"filename={filename}, error=Book ID {book_id} not found in CSV metadata"
            logging.info(error_msg)
            stats['errors'] += 1
            continue

        metadata = metadata_dict[book_id]
        title = metadata['title']
        authors = metadata['authors']

        # Read article text
        article_path = os.path.join(ARTICLES_DIR, filename)
        try:
            with open(article_path, 'r', encoding='utf-8') as f:
                article_text = f.read()
        except UnicodeDecodeError:
            try:
                with open(article_path, 'r', encoding='latin-1') as f:
                    article_text = f.read()
                logging.info(f"filename={filename}, warning=Used latin-1 encoding fallback")
            except Exception as e:
                error_msg = f"filename={filename}, error=Failed to read file: {e}"
                logging.info(error_msg)
                stats['errors'] += 1
                continue
        except Exception as e:
            error_msg = f"filename={filename}, error=Failed to read file: {e}"
            logging.info(error_msg)
            stats['errors'] += 1
            continue

        # Lookup Wikipedia URL
        wikipedia_url = find_wikipedia_url(url_mapping, book_id, language)

        # Validate with Claude
        try:
            result = validate_article_with_claude(article_text, title, authors, client)

            # Write result to appropriate file
            if result['is_match']:
                write_validation_result(matches_file, book_id, filename, language, title, authors,
                                       result['confidence'], result['reasoning'], wikipedia_url)
                stats['matches'] += 1
                stats['match_confidence'][result['confidence']] += 1
                verdict = 'match'
            else:
                write_validation_result(mismatches_file, book_id, filename, language, title, authors,
                                       result['confidence'], result['reasoning'], wikipedia_url)
                stats['mismatches'] += 1
                stats['mismatch_confidence'][result['confidence']] += 1
                verdict = 'mismatch'

            # Update progress
            timestamp = datetime.now().isoformat()
            save_progress_entry(progress_dict, filename, verdict, timestamp)

            # Update stats
            stats['total_processed'] += 1

            # Progress update
            if (i + 1) % 10 == 0:
                print(f"Processed {stats['total_processed']}/{len(article_files)} articles "
                      f"(Matches: {stats['matches']}, Mismatches: {stats['mismatches']}, Errors: {stats['errors']})")

        except Exception as e:
            error_msg = f"filename={filename}, error=API error: {e}"
            logging.info(error_msg)
            stats['errors'] += 1
            print(f"ERROR processing {filename}: {e}")

        # Rate limiting
        time.sleep(REQUEST_DELAY)

    # Close output files
    matches_file.close()
    mismatches_file.close()

    # Generate summary
    print("\n" + "=" * 50)
    print("Validation complete! Generating summary...")
    summary = generate_summary(stats, start_time)

    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write(summary)

    print(summary)
    print(f"\nResults written to:")
    print(f"  - Matches: {MATCHES_FILE}")
    print(f"  - Mismatches: {MISMATCHES_FILE}")
    print(f"  - Summary: {SUMMARY_FILE}")
    print(f"  - Errors: {ERROR_LOG}")


if __name__ == '__main__':
    main()
