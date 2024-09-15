LinkedInJobScraper changelog
8/23/2024 - TODO: convert csv to persistence in duckdb
5/21/2024 - problem identified: unauthenticated search does not support pagination or item offsets, making search results repetitive and unexhaustive

# LinkedIn Job Scraper

This project consists of a LinkedIn job scraper that directly stores data in a DuckDB database.

## LinkedInJobScraper Changelog
- 3/15/2024 - Converted CSV storage to DuckDB persistence
- 5/21/2023 - Problem identified: unauthenticated search does not support pagination or item offsets, making search results repetitive and unexhaustive

## LinkedIn Job Scraper

The `LinkedinJobScraper.py` script scrapes job listings from LinkedIn for software engineering and engineering management positions in the United States. It performs the following tasks:

- Scrapes job data from LinkedIn's job search API
- Stores the data directly in a DuckDB database
- Performs basic analysis on the collected data

### Features:
- Incremental scraping with duplicate detection
- Company and keyword-based filtering
- Exponential backoff for rate limiting (only on 429 responses)
- Debug levels for granular logging
- Direct insertion into DuckDB database
- Memory-efficient processing of large datasets

### Usage:
Run the script with Python:
