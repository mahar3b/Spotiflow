import os
import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd

REGIONS = ["global", "us", "gb", "de", "jp", "br", "ca", "au", "fr", "mx"]
OUTPUT_DIR = "data/kworb"


def scrape_region(region):
    url = f"https://kworb.net/spotify/country/{region}_daily.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch {region}: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "spotifydaily"}) or soup.find("table")

    if not table:
        return None

    rows = []
    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all("td")
        if len(cols) < 10:
            continue

        title_td = cols[2]
        track_id = None
        a_tag = title_td.find("a")
        if a_tag and "href" in a_tag.attrs:
            match = re.search(r'track/([a-zA-Z0-9]+)', a_tag["href"])
            if match:
                track_id = match.group(1)

        rows.append(
            {
                "pos": cols[0].text.strip(),
                "trend": cols[1].text.strip(),
                "artist_title": cols[2].text.strip(),
                "days": cols[3].text.strip(),
                "peak": cols[4].text.strip(),
                "streams": cols[6].text.strip().replace(",", ""),
                "streams_change": cols[7].text.strip().replace(",", ""),
                "seven_day": cols[8].text.strip().replace(",", ""),
                "total": cols[10].text.strip().replace(",", ""),
                "track_id": track_id,
                "region": region,
            }
        )

    return pd.DataFrame(rows)


def main():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    os.makedirs(f"{OUTPUT_DIR}/{today}", exist_ok=True)

    for region in REGIONS:
        print(f"Scraping {region}...")
        df = scrape_region(region)
        if df is not None and not df.empty:
            df.to_csv(f"{OUTPUT_DIR}/{today}/{region}.csv", index=False)
        time.sleep(1)


if __name__ == "__main__":
    main()
