LinkedInJobScraper changelog
8/23/2024 - TODO: convert csv to persistence in duckdb
5/21/2024 - problem identified: unauthenticated search does not support pagination or item offsets, making search results repetitive and unexhaustive

# LinkedIn Job Scraper and Database Importer

This project consists of two main components:

1. A LinkedIn job scraper (`LinkedinJobScraper.py`)
2. A bulk CSV to DuckDB importer (`bulkInsertCsvIntoDuckDB.py`)

## LinkedIn Job Scraper

The `LinkedinJobScraper.py` script scrapes job listings from LinkedIn for software engineering and engineering management positions in the United States. It performs the following tasks:

- Scrapes job data from LinkedIn's job search API
- Stores the data in a CSV file
- Performs basic analysis on the collected data

### Features:
- Incremental scraping with duplicate detection
- Company and keyword-based filtering
- Exponential backoff for rate limiting
- Debug levels for granular logging

### Usage:
Run the script with Python:
