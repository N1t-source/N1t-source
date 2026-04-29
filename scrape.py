# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import json
import time
import random

URL = "https://numbeo.com"

class Extract_table:
    def __init__(self, page):
        self.Data = {}
        self.Table = page.find("table", {'class': 'data_wide_table'})

    def extract(self):
        if not self.Table: return None
        key = "Summary"
        for row in self.Table("tr"):
            if row("th"):
                key = row("th").text.strip()
                self.Data[key] = []
            elif row("td"):
                cells = [cell.text.strip() for cell in row("td")]
                if key not in self.Data: self.Data[key] = []
                self.Data[key].append(cells)
        return self.Data

class API(object):
    def __init__(self, BASE_URL, Country, city_limit=0):
        self.base = BASE_URL
        self.country = Country
        self.url = f"{BASE_URL}country_result.jsp?country={Country.replace(' ', '+')}"
        self.result = {Country: {}}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://google.com'
        }
        
        response = self.get_page(self.url)
        if response:
            self.page = BeautifulSoup(response.text, "html.parser")
            self.get_city()
            EX = Extract_table(self.page)
            extracted_data = EX.extract()
            if extracted_data:
                self.result[Country] = extracted_data
                if city_limit > 0 and self.city:
                    self.get_all_city(city_limit)
        else:
            print(f"Skipping {Country} due to persistent errors.")

    def get_page(self, url):
        retries = 5
        backoff = 5 
        for i in range(retries):
            try:
                time.sleep(backoff + random.uniform(1, 3)) 
                request = requests.get(url, headers=self.headers, timeout=15)
                if request.status_code == 200:
                    return request
                elif request.status_code == 429:
                    print(f"Rate limited (429). Waiting {backoff}s before retry...")
                    backoff *= 2 
                else:
                    print(f"Error {request.status_code} for {url}")
                    return None
            except Exception as e:
                print(f"Connection error: {e}")
                time.sleep(backoff)
        return None

    def get_city(self):
        form = self.page.find("form", {"class": "standard_margin"})
        self.city = [v["value"] for v in form("option") if v.has_attr("value") and v["value"]] if form else []

    def get_all_city(self, limit):
        if not self.result[self.country]: self.result[self.country] = {}
        self.result[self.country]["child"] = {}
        for city in self.city[:limit]:
            print(f"  > Scraping City: {city}")
            city_url = f"{self.base}city_result.jsp?country={self.country.replace(' ', '+')}&city={city.replace(' ', '+')}"
            resp = self.get_page(city_url)
            if resp:
                table = Extract_table(BeautifulSoup(resp.text, "html.parser"))
                self.result[self.country]["child"][city] = table.extract()

    def get_result(self):
        return self.result.get(self.country, {})

def write_json(FILE, OBJECT):
    with open(FILE, 'w', encoding='utf-8') as w:
        json.dump(OBJECT, w, indent=4)
    print(f"\n[Done] Data saved to {FILE}")

if __name__ == "__main__":
    raw_countries = input("Enter countries separated by commas: ")
    COUNTRY_LIST = [c.strip() for c in raw_countries.split(",") if c.strip()]
    
    try:
        val = input("Max cities per country (Enter for 0): ")
        user_limit = int(val) if val.strip() else 0
    except ValueError:
        user_limit = 0
    
    final_results = {}
    for country in COUNTRY_LIST:
        print(f"Processing {country}...")
        obj = API(URL, country, user_limit)
        final_results[country] = obj.get_result()
    
    write_json("results.json", final_results)
