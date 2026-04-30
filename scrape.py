# -*- coding: utf-8 -*-
"""Scrape Numbeo country and city tables into JSON.

Examples:
    python scrape.py --category health-care --countries Malaysia Singapore --city-limit 2
    python scrape.py --category cost-of-living --countries France Japan --output results.json
"""

from __future__ import annotations

import argparse
import json
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup


COUNTRY_LIST_URL = "https://www.numbeo.com/cost-of-living/"
CATEGORIES = {
    "cost-of-living": "https://www.numbeo.com/cost-of-living/",
    "crime": "https://www.numbeo.com/crime/",
    "health-care": "https://www.numbeo.com/health-care/",
    "pollution": "https://www.numbeo.com/pollution/",
    "property-prices": "https://www.numbeo.com/property-investment/",
    "quality-of-life": "https://www.numbeo.com/quality-of-life/",
    "traffic": "https://www.numbeo.com/traffic/",
}
ALL_CATEGORY_CHOICE = "all"
RATING_LABELS = {"Very Low", "Low", "Moderate", "High", "Very High"}
CURRENCY_FIXES = {
    "â‚¬": "€",
    "Â¥": "¥",
    "Ą": "¥",
}


class NumbeoScrapeError(RuntimeError):
    """Raised when a page cannot be fetched after all retries."""


def clean_text(value: str) -> str:
    """Normalize whitespace from table cells."""
    cleaned = " ".join(value.split())
    for bad, good in CURRENCY_FIXES.items():
        cleaned = cleaned.replace(bad, good)
    return cleaned


def parse_number(value: str) -> float | None:
    cleaned = value.replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def split_number_and_suffix(value: str) -> tuple[float | None, str | None]:
    match = re.match(r"^\s*([+-]?\d+(?:,\d{3})*(?:\.\d+)?)\s*(.*?)\s*$", value)
    if not match:
        return None, None

    number = parse_number(match.group(1))
    suffix = match.group(2).strip()

    if number is None:
        return None, suffix or None

    return number, suffix or None


def parse_range(value: str, unit: str | None = None) -> dict[str, Any]:
    parts = re.split(r"\s+-\s+", value, maxsplit=1)
    range_data: dict[str, Any] = {"display": value}

    if len(parts) != 2:
        return range_data

    low, low_unit = split_number_and_suffix(parts[0])
    high, high_unit = split_number_and_suffix(parts[1])

    if low is not None:
        range_data["min"] = low
    if high is not None:
        range_data["max"] = high

    range_unit = unit or low_unit or high_unit
    if range_unit:
        range_data["unit"] = range_unit

    return range_data


def normalize_metric(name: str, value_display: str) -> dict[str, Any]:
    metric: dict[str, Any] = {
        "name": clean_text(name).rstrip(":"),
        "value_display": clean_text(value_display),
    }
    value_text = metric["value_display"]

    if value_text.endswith("%"):
        metric["unit"] = "%"
        value_text = value_text[:-1]

    value, suffix = split_number_and_suffix(value_text)
    if value is not None:
        metric["value"] = value
    if suffix:
        metric["unit"] = metric.get("unit") or suffix

    return metric


def normalized_cell(value: str) -> Any:
    cleaned = clean_text(value)
    number = parse_number(cleaned.rstrip("%"))
    if number is None:
        return cleaned
    if cleaned.endswith("%"):
        return {"value": number, "unit": "%", "display": cleaned}
    return number


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "item"


def unique_non_empty(items: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_items: list[str] = []
    for item in items:
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        unique_items.append(cleaned)
    return unique_items


class TableExtractor:
    """Extract a Numbeo data table into JSON-friendly sections."""

    def __init__(self, page: BeautifulSoup, table: Any | None = None, title: str = "") -> None:
        self.table = table or page.find("table", {"class": "data_wide_table"})
        self.title = title

    def extract(self) -> dict[str, list[dict[str, Any]]] | None:
        if not self.table:
            return None

        data: dict[str, list[dict[str, Any]]] = {}
        section = self.title or "Summary"

        for row in self.table.find_all("tr"):
            header = row.find("th")
            if header:
                section = clean_text(header.get_text(" "))
                data.setdefault(section, [])
                continue

            cells = [clean_text(cell.get_text(" ")) for cell in row.find_all("td")]
            cells = [cell for cell in cells if cell]
            if not cells:
                continue

            data.setdefault(section, []).append(self._normalize_row(cells))

        return data or None

    @staticmethod
    def _normalize_row(cells: list[str]) -> dict[str, Any]:
        row: dict[str, Any] = {"raw": {"cells": cells}}

        if cells:
            row["item"] = cells[0]

        if len(cells) >= 2:
            row["value_display"] = cells[1]
            value, suffix = split_number_and_suffix(cells[1])

            if value is not None:
                row["value"] = value
            else:
                row["value"] = cells[1]

            if suffix and suffix not in RATING_LABELS:
                row["unit"] = suffix

            if len(cells) >= 3 and cells[2] in RATING_LABELS:
                row["rating"] = cells[2]
            elif len(cells) >= 3:
                row["range"] = parse_range(cells[2], row.get("unit"))
            else:
                row.pop("range", None)

        if len(cells) >= 3:
            row["raw"]["range_display"] = cells[2]

        if len(cells) > 3:
            row["extra"] = cells[3:]

        return row


def nearest_heading(table: Any) -> str:
    node = table.previous_sibling
    while node:
        if getattr(node, "name", None) in {"h1", "h2", "h3"}:
            return clean_text(node.get_text(" "))
        node = node.previous_sibling
    return ""


def extract_data_tables(page: BeautifulSoup) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []

    for index, table in enumerate(page.find_all("table", {"class": "data_wide_table"}), start=1):
        title = nearest_heading(table) or f"Data Table {index}"
        sections = TableExtractor(page, table=table, title=title).extract() or {}
        tables.append(
            {
                "title": title,
                "sections": sections,
            }
        )

    return tables


def merge_table_sections(tables: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {}

    for table in tables:
        for section, rows in table["sections"].items():
            merged.setdefault(section, []).extend(rows)

    return merged


def extract_indices(page: BeautifulSoup) -> list[dict[str, Any]]:
    indices: list[dict[str, Any]] = []

    for table in page.find_all("table", {"class": "table_indices"}):
        group_title = ""
        pending_name = ""

        for token in table.stripped_strings:
            text = clean_text(token)
            if not text:
                continue

            if text == "Index" or text.startswith("country data"):
                group_title = text
                continue

            if text.endswith(":"):
                pending_name = text
                continue

            if pending_name:
                metric = normalize_metric(pending_name, text)
                if group_title:
                    metric["group"] = group_title
                indices.append(metric)
                pending_name = ""

    return indices


def extract_city_rankings(page: BeautifulSoup) -> list[dict[str, Any]]:
    rankings: list[dict[str, Any]] = []

    for table in page.find_all("table", id="t2"):
        headers = [clean_text(cell.get_text(" ")) for cell in table.find_all("th")]
        rows: list[dict[str, Any]] = []

        for tr in table.find_all("tr"):
            cells = [clean_text(cell.get_text(" ")) for cell in tr.find_all("td")]
            if not cells:
                continue

            row: dict[str, Any] = {}
            for index, value in enumerate(cells):
                header = headers[index] if index < len(headers) else f"column_{index + 1}"
                row[header or f"column_{index + 1}"] = normalized_cell(value)
            rows.append(row)

        if rows:
            rankings.append(
                {
                    "title": nearest_heading(table) or "By City",
                    "columns": headers,
                    "rows": rows,
                }
            )

    return rankings


def extract_metric_tables(page: BeautifulSoup) -> list[dict[str, Any]]:
    metric_tables: list[dict[str, Any]] = []
    excluded_classes = {
        "data_wide_table",
        "languages_ref_table",
        "standard_margin",
        "stripe",
        "table_indices",
    }

    for index, table in enumerate(page.find_all("table"), start=1):
        classes = set(table.get("class") or [])
        if classes & excluded_classes or table.get("id") == "t2":
            continue

        metrics: list[dict[str, Any]] = []
        for tr in table.find_all("tr"):
            cells = [clean_text(cell.get_text(" ")) for cell in tr.find_all(["th", "td"])]
            cells = [cell for cell in cells if cell]

            if len(cells) < 2:
                continue

            metric = normalize_metric(cells[0], cells[1])
            if len(cells) >= 3 and cells[2] in RATING_LABELS:
                metric["rating"] = cells[2]
            elif len(cells) >= 3:
                metric["extra"] = cells[2:]
            metrics.append(metric)

        if metrics:
            metric_tables.append(
                {
                    "title": nearest_heading(table) or f"Metric Table {index}",
                    "metrics": metrics,
                }
            )

    return metric_tables


def merge_metrics(metric_tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []

    for table in metric_tables:
        for metric in table["metrics"]:
            metric_with_group = dict(metric)
            metric_with_group.setdefault("group", table["title"])
            metrics.append(metric_with_group)

    return metrics


class NumbeoScraper:
    def __init__(
        self,
        category: str,
        *,
        city_limit: int = 0,
        retries: int = 4,
        delay: float = 2.0,
        timeout: int = 20,
    ) -> None:
        if category not in CATEGORIES:
            raise ValueError(f"Unsupported category: {category}")

        self.category = category
        self.base_url = CATEGORIES[category]
        self.city_limit = city_limit
        self.retries = retries
        self.delay = delay
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def scrape_country(self, country: str) -> dict[str, Any]:
        country_url = self._country_url(country)
        page = self._get_soup(country_url)
        tables = extract_data_tables(page)
        metric_tables = extract_metric_tables(page)
        result: dict[str, Any] = {
            "source_url": country_url,
            "indices": extract_indices(page),
            "metrics": merge_metrics(metric_tables),
            "metric_tables": metric_tables,
            "data": merge_table_sections(tables),
            "tables": tables,
            "city_rankings": extract_city_rankings(page),
            "cities": {},
        }

        cities = self._extract_cities(page)
        if self.city_limit > 0:
            for city in cities[: self.city_limit]:
                print(f"  > Scraping {country} / {city}")
                result["cities"][city] = self.scrape_city(country, city)

        return result

    def scrape_city(self, country: str, city: str) -> dict[str, Any]:
        city_url = self._city_url(country, city)
        page = self._get_soup(city_url)
        tables = extract_data_tables(page)
        metric_tables = extract_metric_tables(page)
        return {
            "source_url": city_url,
            "indices": extract_indices(page),
            "metrics": merge_metrics(metric_tables),
            "metric_tables": metric_tables,
            "data": merge_table_sections(tables),
            "tables": tables,
            "city_rankings": extract_city_rankings(page),
        }

    def _get_soup(self, url: str) -> BeautifulSoup:
        response = self._get_page(url)
        return BeautifulSoup(response.content, "html.parser")

    def _get_page(self, url: str) -> requests.Response:
        backoff = max(self.delay, 1.0)
        last_error: Exception | None = None

        for attempt in range(1, self.retries + 1):
            try:
                time.sleep(backoff + random.uniform(0.25, 1.0))
                response = self.session.get(url, timeout=self.timeout)

                if response.status_code == 200:
                    return response

                if response.status_code == 429:
                    print(f"Rate limited by Numbeo; retrying attempt {attempt}/{self.retries}")
                    backoff *= 2
                    continue

                response.raise_for_status()
            except requests.RequestException as exc:
                last_error = exc
                print(f"Request failed for {url}: {exc}")
                backoff *= 2

        raise NumbeoScrapeError(f"Could not fetch {url}") from last_error

    def _extract_cities(self, page: BeautifulSoup) -> list[str]:
        for select in page.find_all("select"):
            options = select.find_all("option")
            values = [clean_text(option.get_text(" ")) for option in options]
            if not values:
                continue

            placeholder = values[0].lower()
            if "select city" not in placeholder:
                continue

            cities = [
                option.get("value", "").strip()
                for option in options[1:]
                if option.get("value", "").strip()
            ]
            return unique_non_empty(cities)

        return []

    def _country_url(self, country: str) -> str:
        return f"{self.base_url}country_result.jsp?country={quote_plus(country)}"

    def _city_url(self, country: str, city: str) -> str:
        return (
            f"{self.base_url}city_result.jsp?"
            f"country={quote_plus(country)}&city={quote_plus(city)}"
        )


def get_available_countries(timeout: int = 20) -> list[str]:
    response = requests.get(
        COUNTRY_LIST_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=timeout,
    )
    response.raise_for_status()

    page = BeautifulSoup(response.content, "html.parser")
    country_form = page.find("form", action=re.compile(r"country_result\.jsp"))
    if not country_form:
        raise NumbeoScrapeError("Could not find Numbeo country selector.")

    countries: list[str] = []
    for option in country_form.find_all("option"):
        value = option.get("value")
        if value and value.strip():
            countries.append(value.strip())

    return sorted(dict.fromkeys(countries))


class Extract_table(TableExtractor):
    """Backward-compatible name used by the original project."""


class API:
    """Backward-compatible wrapper around NumbeoScraper."""

    def __init__(self, base_url: str, country: str, city_limit: int = 0) -> None:
        category = self._category_from_url(base_url)
        self.country = country
        self.scraper = NumbeoScraper(category, city_limit=city_limit)
        payload = self.scraper.scrape_country(country)
        self.result = {country: self._legacy_payload(payload)}

    def get_result(self) -> dict[str, Any]:
        return self.result.get(self.country, {})

    @staticmethod
    def _legacy_payload(payload: dict[str, Any]) -> dict[str, Any]:
        data = dict(payload.get("data", {}))
        cities = payload.get("cities", {})
        if cities:
            data["child"] = {
                city: city_payload.get("data", {}) for city, city_payload in cities.items()
            }
        return data

    @staticmethod
    def _category_from_url(base_url: str) -> str:
        for category, url in CATEGORIES.items():
            if category in base_url or url.rstrip("/") in base_url.rstrip("/"):
                return category
        return "cost-of-living"


def write_json(path: str, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as output:
        json.dump(payload, output, indent=2, ensure_ascii=False)
        output.write("\n")
    print(f"\n[Done] Data saved to {path}")


def read_json_if_exists(path: str | Path) -> dict[str, Any] | None:
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return None

    try:
        with file_path.open("r", encoding="utf-8") as input_file:
            payload = json.load(input_file)
        if isinstance(payload, dict):
            return payload
    except (OSError, json.JSONDecodeError):
        return None

    return None


def split_output_root(output: str) -> Path:
    path = Path(output)
    return path.with_suffix("") if path.suffix else path


def country_payload(country: str, category: str, payload: dict[str, Any], city_limit: int) -> dict[str, Any]:
    return {
        "metadata": {
            "schema_version": "2.0",
            "category": category,
            "country": country,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "city_limit": city_limit,
        },
        "country": country,
        "category": category,
        **payload,
    }


def error_payload(exc: Exception) -> dict[str, Any]:
    return {
        "error": str(exc),
        "indices": [],
        "metrics": [],
        "data": {},
        "tables": [],
        "city_rankings": [],
        "cities": {},
    }


def split_manifest(args: argparse.Namespace, countries: list[str]) -> tuple[Path, dict[str, Any]]:
    root = split_output_root(args.output)
    root.mkdir(parents=True, exist_ok=True)
    (root / ".root").touch()
    (root / "countries").mkdir(parents=True, exist_ok=True)

    metadata: dict[str, Any] = {
        "schema_version": "2.0",
        "category": args.category,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "city_limit": max(args.city_limit, 0),
        "all_countries": bool(args.all_countries),
        "country_count": len(countries),
    }
    if args.category == ALL_CATEGORY_CHOICE:
        metadata["categories"] = sorted(CATEGORIES)

    return root, {"metadata": metadata, "files": []}


def write_split_country_category(
    root: Path,
    manifest: dict[str, Any],
    country: str,
    category: str,
    payload: dict[str, Any],
    city_limit: int,
) -> None:
    target = root / "countries" / slugify(country) / f"{slugify(category)}.json"
    write_json(str(target), country_payload(country, category, payload, city_limit))
    entry = {
        "country": country,
        "category": category,
        "path": target.relative_to(root).as_posix(),
    }
    manifest["files"] = [
        file_entry
        for file_entry in manifest["files"]
        if not (file_entry.get("country") == country and file_entry.get("category") == category)
    ]
    manifest["files"].append(entry)
    write_json(str(root / "manifest.json"), manifest)


def write_split_outputs(output: str, results: dict[str, Any]) -> None:
    root = split_output_root(output)
    root.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "metadata": dict(results.get("metadata", {})),
        "files": [],
    }
    city_limit = int(results.get("metadata", {}).get("city_limit", 0))
    root_marker = root / ".root"
    root_marker.touch()

    countries_root = root / "countries"
    countries_root.mkdir(parents=True, exist_ok=True)

    for country, country_data in results.get("countries", {}).items():
        country_dir = countries_root / slugify(country)
        country_dir.mkdir(parents=True, exist_ok=True)

        if "categories" in country_data:
            for category, payload in country_data["categories"].items():
                target = country_dir / f"{slugify(category)}.json"
                write_json(str(target), country_payload(country, category, payload, city_limit))
                manifest["files"].append(
                    {
                        "country": country,
                        "category": category,
                        "path": target.relative_to(root).as_posix(),
                    }
                )
        else:
            category = results.get("metadata", {}).get("category", "unknown")
            target = country_dir / f"{slugify(category)}.json"
            write_json(str(target), country_payload(country, category, country_data, city_limit))
            manifest["files"].append(
                {
                    "country": country,
                    "category": category,
                    "path": target.relative_to(root).as_posix(),
                }
            )

    write_json(str(root / "manifest.json"), manifest)


def has_payload_data(payload: dict[str, Any] | None) -> bool:
    if not payload:
        return False

    return any(
        payload.get(key)
        for key in ("indices", "metrics", "data", "tables", "city_rankings", "cities")
    )


def split_output_target(output: str, country: str, category: str) -> Path:
    root = split_output_root(output)
    return root / "countries" / slugify(country) / f"{slugify(category)}.json"


def existing_payload_from_output(
    args: argparse.Namespace, country: str, category: str
) -> dict[str, Any] | None:
    if args.split_output:
        for candidate in (
            split_output_target(args.output, country, category),
            split_output_root(args.output) / slugify(country) / f"{slugify(category)}.json",
        ):
            split_payload = read_json_if_exists(candidate)
            if split_payload and has_payload_data(split_payload):
                return split_payload
        return None

    combined_payload = read_json_if_exists(args.output)
    if not combined_payload:
        return None

    countries = combined_payload.get("countries", {})
    country_payload = countries.get(country)
    if not isinstance(country_payload, dict):
        return None

    if combined_payload.get("metadata", {}).get("category") == ALL_CATEGORY_CHOICE:
        category_payload = country_payload.get("categories", {}).get(category)
        if isinstance(category_payload, dict) and has_payload_data(category_payload):
            return category_payload
        return None

    if combined_payload.get("metadata", {}).get("category") == category and has_payload_data(country_payload):
        return country_payload

    return None


def resolve_requested_countries(args: argparse.Namespace) -> list[str]:
    countries = unique_non_empty(list(args.countries or []))

    if args.all_countries:
        print("Discovering countries from Numbeo...")
        countries = get_available_countries()
        if args.country_limit > 0:
            countries = countries[: args.country_limit]
        print(f"Discovered {len(countries)} countries.")
        return countries

    if countries:
        return countries

    if args.country_count > 0:
        print("Discovering countries from Numbeo for count-based selection...")
        countries = get_available_countries()
        countries = countries[: args.country_count]
        print(f"Selected first {len(countries)} countries from Numbeo.")
        return countries

    raw_countries = input("Enter countries separated by commas: ")
    return unique_non_empty(raw_countries.split(","))


def country_selection_mode(args: argparse.Namespace, countries: list[str]) -> str:
    if args.all_countries:
        return "all"
    if args.country_count > 0 and not args.countries:
        return "count"
    if countries:
        return "named"
    return "prompt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Numbeo tables into JSON.")
    parser.add_argument(
        "--category",
        choices=[ALL_CATEGORY_CHOICE, *sorted(CATEGORIES)],
        default="cost-of-living",
        help="Numbeo section to scrape.",
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        help="Country names to scrape, for example: Malaysia Singapore Australia",
    )
    parser.add_argument(
        "--all-countries",
        action="store_true",
        help="Scrape every country currently listed by Numbeo.",
    )
    parser.add_argument(
        "--country-limit",
        type=int,
        default=0,
        help="Limit number of countries after discovery. Useful for testing --all-countries.",
    )
    parser.add_argument(
        "--country-count",
        type=int,
        default=0,
        help="When --all-countries is not used and --countries is omitted, discover and scrape the first N countries.",
    )
    parser.add_argument(
        "--city-limit",
        type=int,
        default=0,
        help="Maximum cities to scrape per country. Use 0 for country-level data only.",
    )
    parser.add_argument(
        "--output",
        default="results.json",
        help="JSON file to write, or output folder base when --split-output is used.",
    )
    parser.add_argument(
        "--split-output",
        action="store_true",
        help="Write one JSON file per country/category plus a manifest instead of one combined file.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Base delay between requests in seconds.",
    )
    return parser.parse_args()


def scrape_single_category(args: argparse.Namespace, countries: list[str]) -> dict[str, Any]:
    scraper = NumbeoScraper(
        args.category,
        city_limit=max(args.city_limit, 0),
        delay=max(args.delay, 0),
    )

    results: dict[str, Any] = {
        "metadata": {
            "schema_version": "2.0",
            "category": args.category,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "city_limit": max(args.city_limit, 0),
            "all_countries": bool(args.all_countries),
            "country_count": len(countries),
            "country_selection_mode": country_selection_mode(args, countries),
        },
        "countries": {},
    }

    for country in countries:
        existing_payload = existing_payload_from_output(args, country, args.category)
        if existing_payload:
            print(f"Skipping {country}; found existing {args.category} data.")
            results["countries"][country] = existing_payload
            continue

        print(f"Processing {country}...")
        try:
            results["countries"][country] = scraper.scrape_country(country)
        except NumbeoScrapeError as exc:
            results["countries"][country] = error_payload(exc)

    return results


def scrape_single_category_split(args: argparse.Namespace, countries: list[str]) -> dict[str, Any]:
    root, manifest = split_manifest(args, countries)
    scraper = NumbeoScraper(
        args.category,
        city_limit=max(args.city_limit, 0),
        delay=max(args.delay, 0),
    )

    for country in countries:
        existing_payload = existing_payload_from_output(args, country, args.category)
        if existing_payload:
            print(f"Skipping {country}; found existing {args.category} data.")
            write_split_country_category(
                root,
                manifest,
                country,
                args.category,
                existing_payload,
                max(args.city_limit, 0),
            )
            continue

        print(f"Processing {country}...")
        try:
            payload = scraper.scrape_country(country)
        except NumbeoScrapeError as exc:
            payload = error_payload(exc)

        write_split_country_category(
            root,
            manifest,
            country,
            args.category,
            payload,
            max(args.city_limit, 0),
        )

    return manifest


def scrape_all_categories(args: argparse.Namespace, countries: list[str]) -> dict[str, Any]:
    category_names = sorted(CATEGORIES)
    results: dict[str, Any] = {
        "metadata": {
            "schema_version": "2.0",
            "category": ALL_CATEGORY_CHOICE,
            "categories": category_names,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "city_limit": max(args.city_limit, 0),
            "all_countries": bool(args.all_countries),
            "country_count": len(countries),
            "country_selection_mode": country_selection_mode(args, countries),
        },
        "countries": {},
    }

    for country in countries:
        print(f"Processing {country}...")
        results["countries"][country] = {"categories": {}}

        for category in category_names:
            existing_payload = existing_payload_from_output(args, country, category)
            if existing_payload:
                print(f"  > {category} (skipped; already exists)")
                results["countries"][country]["categories"][category] = existing_payload
                continue

            print(f"  > {category}")
            scraper = NumbeoScraper(
                category,
                city_limit=max(args.city_limit, 0),
                delay=max(args.delay, 0),
            )
            try:
                results["countries"][country]["categories"][category] = scraper.scrape_country(country)
            except NumbeoScrapeError as exc:
                results["countries"][country]["categories"][category] = error_payload(exc)

    return results


def scrape_all_categories_split(args: argparse.Namespace, countries: list[str]) -> dict[str, Any]:
    root, manifest = split_manifest(args, countries)
    category_names = sorted(CATEGORIES)

    for country in countries:
        print(f"Processing {country}...")

        for category in category_names:
            existing_payload = existing_payload_from_output(args, country, category)
            if existing_payload:
                print(f"  > {category} (skipped; already exists)")
                write_split_country_category(
                    root,
                    manifest,
                    country,
                    category,
                    existing_payload,
                    max(args.city_limit, 0),
                )
                continue

            print(f"  > {category}")
            scraper = NumbeoScraper(
                category,
                city_limit=max(args.city_limit, 0),
                delay=max(args.delay, 0),
            )
            try:
                payload = scraper.scrape_country(country)
            except NumbeoScrapeError as exc:
                payload = error_payload(exc)

            write_split_country_category(
                root,
                manifest,
                country,
                category,
                payload,
                max(args.city_limit, 0),
            )

    return manifest


def main() -> None:
    args = parse_args()
    countries = resolve_requested_countries(args)

    if args.split_output and args.category == ALL_CATEGORY_CHOICE:
        scrape_all_categories_split(args, countries)
    elif args.split_output:
        scrape_single_category_split(args, countries)
    elif args.category == ALL_CATEGORY_CHOICE:
        results = scrape_all_categories(args, countries)
        write_json(args.output, results)
    else:
        results = scrape_single_category(args, countries)
        write_json(args.output, results)


if __name__ == "__main__":
    main()
