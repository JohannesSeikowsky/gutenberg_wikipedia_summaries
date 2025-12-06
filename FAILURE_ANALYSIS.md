# Wikipedia Article Scraper - Failure Analysis Report

**Generated**: 2025-12-06
**Retry run**: 142 failed entries re-attempted

## Executive Summary

Out of 142 failed Wikipedia article downloads, we successfully recovered **4 articles** (2.8%). The remaining 138 failures (97.2%) are mostly **permanent failures** due to Wikipedia redirects that cannot be resolved with the current scraping approach.

## Results Breakdown

### Overall Statistics
- **Total retried**: 142
- **Successful on retry**: 4 (2.8%)
- **Still failed**: 138 (97.2%)
- **Original failure rate**: 137/12,069 (1.1%)
- **Final failure rate**: 133/12,069 (1.1%)

### Successfully Recovered Articles
1. `book_id=1735` → `1735_en.txt` (Sophist dialogue)
2. `book_id=6637` → `6637_de.txt` (Der Alpenkönig und der Menschenfeind)
3. `book_id=75544` → `75544_simple.txt` (Peer Gynt)
4. `book_id=76366` → `76366_simple.txt` (Heart of Stone)

These 4 were **connection errors** that succeeded on retry.

## Root Cause Analysis

### 1. Redirects (127 failures - 89.4%)

**Problem**: Wikipedia pages that redirect to other pages. The API's `prop=extracts` doesn't follow redirects, returning empty content.

**Examples**:
- `Peter_and_Wendy` → redirects to `Peter Pan`
- `Civil_Disobedience_(Thoreau)` → redirects to `Resistance to Civil Government`
- `The_Ambassadors` → redirects to disambiguation or different title
- `Lang's_Fairy_Books` → redirects to `Andrew Lang's Fairy Books`

**Why this happens**: Wikipedia restructures and redirects pages over time. Old URLs may redirect to:
- Renamed pages (preferred canonical titles)
- Consolidated pages
- Disambiguation pages
- Different language/format versions

**Potential solution**: Add `redirects=1` parameter to API call to automatically follow redirects.

### 2. Special Page Types (7 failures - 4.9%)

**Problem**: Pages with content that cannot be extracted as plain text via the `extracts` API.

**Characteristics**:
- Page exists (`page_id` is valid)
- Has content (`raw_size` > 0)
- Extract returns empty string

**Why this happens**: Certain page types (lists, indexes, collections) don't have traditional article text.

### 3. Invalid URLs (2 failures - 1.4%)

**Problem**: URLs with carriage return (`\r`) characters from the source file.

**Examples**:
- `http://en.wikipedia.org/wiki/Faust,_Part_2\r` (404)
- `http://en.wikipedia.org/wiki/Faust,_Part_1\r` (404)

**Why this happens**: Line ending issues in `all_book_wiki_links.txt` (likely Windows-style CRLF).

**Solution**: Strip whitespace more aggressively in URL preprocessing.

### 4. Connection Errors (4 occurrences, 4 recovered)

**Problem**: Temporary network issues or Wikipedia API unavailability.

**Result**: All 4 succeeded on retry! This validates our retry mechanism.

## Recommendations

### Immediate Fixes

1. **Handle redirects** - Modify `get_wiki_articles.py` to add `redirects=1` parameter:
   ```python
   params = {
       'action': 'query',
       'format': 'json',
       'prop': 'extracts',
       'explaintext': True,
       'redirects': 1,  # <-- ADD THIS
       'titles': page_title
   }
   ```

2. **Fix URL preprocessing** - Strip all whitespace including `\r\n`:
   ```python
   def extract_page_title(url):
       url = url.strip().rstrip('\r\n')  # More aggressive stripping
       ...
   ```

### Long-term Improvements

3. **Input validation** - Validate URLs before scraping to catch malformed entries early

4. **Redirect following report** - Log when redirects occur so users know the actual page fetched differs from the URL

## Impact of Implementing Redirect Handling

If we add `redirects=1` to the API call, we can expect to recover **~127 additional articles** (89% of current failures), bringing our success rate from **98.9%** to **99.9%**.

## Files Generated

- `retry_results.json` - Detailed results of each retry attempt with diagnostics
- `progress.json` - Updated with 4 new successes
- `articles/` - 4 new article files

## Next Steps

1. Implement redirect handling in `get_wiki_articles.py`
2. Fix URL whitespace stripping
3. Clean source file `all_book_wiki_links.txt` to remove carriage returns
4. Re-run scraper on remaining 133 failed entries
5. Document edge cases that truly cannot be scraped (if any remain)
