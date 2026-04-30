# -*- coding: utf-8 -*-
"""Compatibility wrapper for scraping Numbeo health-care data."""

from datetime import datetime, timezone

from scrape import NumbeoScraper, write_json


def main() -> None:
    countries = ["Malaysia", "Singapore", "Australia"]
    scraper = NumbeoScraper("health-care", city_limit=0)
    results = {
        "metadata": {
            "schema_version": "2.0",
            "category": "health-care",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "city_limit": 0,
        },
        "countries": {},
    }

    for country in countries:
        print(f"Processing {country}...")
        results["countries"][country] = scraper.scrape_country(country)

    write_json("healthcare.json", results)


if __name__ == "__main__":
    main()
