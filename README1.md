# Numbeo Web Scraper

This project scrapes Numbeo country and city tables and writes the result to JSON.

## Setup

```bash
python -m pip install -r requirements.txt
```

## Cost of Living

```bash
python scrape.py --category cost-of-living --countries Malaysia Singapore Australia --output results.json
```

## Health Care

```bash
python scrape.py --category health-care --countries Malaysia Singapore Australia --output healthcare.json
```

## All Supported Numbeo Sections

Use `--category all` to scrape every supported section into one file.

```bash
python scrape.py --category all --countries Malaysia France Japan --output numbeo_all.json
```

Use `--split-output` to store each country/category in its own JSON file.

```bash
python scrape.py --category all --countries Malaysia France Japan --output numbeo_data --split-output
```

Use `--all-countries` to discover every country currently listed by Numbeo.

```bash
python scrape.py --category all --all-countries --output numbeo_data --split-output
```

For full all-country runs, split output writes each country/category file as soon as that page is scraped. This is safer for long Colab runs because completed files remain on disk if the runtime disconnects.

For a quick test before a long run:

```bash
python scrape.py --category all --all-countries --country-limit 2 --output numbeo_data_test --split-output
```

That creates a folder like:

```text
numbeo_data/
  manifest.json
  countries/
    malaysia/
      cost-of-living.json
      crime.json
      health-care.json
      pollution.json
      property-prices.json
      quality-of-life.json
      traffic.json
```

If those files already exist, the scraper skips them on the next run so we can resume without repeating finished pages.

Supported sections:

- `cost-of-living`
- `property-prices`
- `crime`
- `health-care`
- `pollution`
- `traffic`
- `quality-of-life`

## Include Cities

Use `--city-limit` to scrape city pages for each country.

```bash
python scrape.py --category health-care --countries Malaysia --city-limit 5 --output malaysia_healthcare.json
```

`--city-limit 0` only scrapes country-level data. This is the default and is friendlier to Numbeo.

## JSON Shape

```json
{
  "metadata": {
    "schema_version": "2.0",
    "category": "health-care",
    "scraped_at": "2026-04-30T00:00:00+00:00",
    "city_limit": 0
  },
  "countries": {
    "Malaysia": {
      "source_url": "https://www.numbeo.com/health-care/country_result.jsp?country=Malaysia",
      "indices": [
        {
          "name": "Health Care System Index",
          "value_display": "70.71",
          "value": 70.71,
          "group": "Index"
        }
      ],
      "metrics": [],
      "data": {
        "Component of health care surveyed": [
          {
            "item": "Skill and competency of medical staff",
            "value_display": "66.29 High",
            "value": 66.29,
            "rating": "High",
            "raw": {
              "cells": ["Skill and competency of medical staff", "66.29 High", "High"],
              "range_display": "High"
            }
          }
        ]
      },
      "tables": [],
      "city_rankings": [],
      "cities": {}
    }
  }
}
```

Cost-of-living rows use the same `item`, `value_display`, and numeric `value` fields, plus `unit` and a structured `range` when Numbeo provides one.

```json
{
  "item": "Meal at an Inexpensive Restaurant",
  "value_display": "15.00 RM",
  "value": 15.0,
  "unit": "RM",
  "range": {
    "display": "10.00 - 23.00",
    "min": 10.0,
    "max": 23.0,
    "unit": "RM"
  }
}
```

When `--category all` is used, the output nests each section under `countries.<country>.categories.<category>`.

## Python Usage

```python
from scrape import NumbeoScraper

scraper = NumbeoScraper("health-care", city_limit=2)
malaysia = scraper.scrape_country("Malaysia")
```

The old `API(base_url, country).get_result()` import style is still supported for existing code.
