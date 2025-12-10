#!/usr/bin/env python3
"""Add Wikipedia URLs to existing validation results files."""

import re
import os
import shutil


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

    # No language match - return first URL and print warning
    print(f"Warning: No language match for book_id={book_id}, lang={language}. Using first URL.")
    return urls[0][0]


def update_results_file(input_file, url_mapping):
    """Add wikipedia_url column to existing results file."""
    if not os.path.exists(input_file):
        print(f"File {input_file} does not exist, skipping.")
        return

    # Create backup
    backup_file = input_file + '.backup'
    shutil.copy(input_file, backup_file)
    print(f"Created backup: {backup_file}")

    # Read all lines
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Process lines
    updated_lines = []
    for i, line in enumerate(lines):
        line = line.rstrip('\n')

        # Handle header
        if i == 0:
            updated_lines.append(line + '\twikipedia_url\n')
            continue

        # Skip empty lines
        if not line.strip():
            updated_lines.append(line + '\n')
            continue

        # Parse TSV columns: title, confidence, reasoning, book_id, language, filename
        parts = line.split('\t')
        if len(parts) < 6:
            print(f"Warning: Line {i+1} has fewer than 6 columns, skipping: {line}")
            updated_lines.append(line + '\n')
            continue

        book_id = parts[3]
        language = parts[4]

        # Lookup Wikipedia URL
        wikipedia_url = find_wikipedia_url(url_mapping, book_id, language)

        # Append URL to line
        updated_lines.append(line + '\t' + wikipedia_url + '\n')

    # Write updated file
    with open(input_file, 'w', encoding='utf-8') as f:
        f.writelines(updated_lines)

    print(f"Updated {input_file}: {len(lines)-1} data rows processed")


def main():
    """Main function."""
    print("Adding Wikipedia URLs to existing validation results")
    print("=" * 50)

    # Load URL mapping
    print("\nLoading Wikipedia URL mapping...")
    url_mapping = load_wikipedia_url_mapping('all_book_wiki_links.txt')
    print(f"Loaded URLs for {len(url_mapping)} books")

    # Update both results files
    print("\nUpdating validated_matches.txt...")
    update_results_file('validated_matches.txt', url_mapping)

    print("\nUpdating validation_mismatches.txt...")
    update_results_file('validation_mismatches.txt', url_mapping)

    print("\n" + "=" * 50)
    print("Done! Original files backed up with .backup extension")


if __name__ == '__main__':
    main()
