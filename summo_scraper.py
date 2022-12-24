import concurrent.futures
import configparser
import datetime
import logging
import os
import re
import time
from logging import config, getLogger
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from robots import Robots

MAX_RETRIES = 5
DELAY = 1
TIMEOUT = 10
MAX_WORKERS = None


class SummoScraper(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = True

    def can_fetch(self, url):
        robots = Robots(url)
        if robots.can_fetch():
            self.delay = robots.crawl_delay()
            if self.delay is None:
                self.delay = DELAY
        else:
            raise Exception("Disallow crawl.")

    def _fetch_soup(self, url):
        retry = 0
        while True:
            try:
                time.sleep(self.delay)
                response = requests.get(url, timeout=TIMEOUT)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if retry < MAX_RETRIES:
                    retry += 1
                    self.logger.warning(f"Retry because request failed. URL: {url}")
                    self.logger.warning(f"Exception: {repr(e)}")
                    continue
                else:
                    self.logger.exception(f"Retry count is maxed. URL: {url}")
                    self.logger.exception(f"Exception: {repr(e)}")
                    raise e

        soup = BeautifulSoup(response.content, "html.parser")
        return soup

    def fetch_max_page_no(self, url):
        soup = self._fetch_soup(url)

        pagination = soup.find("div", {"class": "pagination pagination_set-nav"})
        links = pagination.findAll("a")

        max_page_no = 1
        for link in reversed(links):
            if link.get_text().isdigit():
                max_page_no = int(link.get_text())
                break

        return max_page_no

    def get_base_url(self, url):
        base_url = re.sub("&page=[0-9]*", "", url)
        return base_url

    def fetch_rooms_data(self, url):
        rooms = []
        soup = self._fetch_soup(url)

        items = soup.findAll("div", {"class": "cassetteitem"})
        for item in items:
            building = {}
            building["name"] = item.find("div", {"class": "cassetteitem_content-title"}).get_text().strip()
            building["category"] = item.find("div", {"class": "cassetteitem_content-label"}).get_text().strip()
            building["address"] = item.find("li", {"class": "cassetteitem_detail-col1"}).get_text().strip()
            accesses = item.find_all("div", {"class": "cassetteitem_detail-text"})
            building["accesses1"] = accesses[0].get_text().strip() if accesses[0] is not None else None
            building["accesses2"] = accesses[1].get_text().strip() if accesses[1] is not None else None
            building["accesses3"] = accesses[2].get_text().strip() if accesses[2] is not None else None
            building["age"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[0].get_text().strip()
            building["story"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[1].get_text().strip()

            tbodys = item.find("table", {"class": "cassetteitem_other"}).findAll("tbody")
            for tbody in tbodys:
                room = building.copy()
                room["floor"] = tbody.findAll("td")[2].get_text().strip()
                room["rent"] = tbody.find("span", {"class": "cassetteitem_price--rent"}).get_text().strip()
                room["administration"] = tbody.find("span", {"class": "cassetteitem_price--administration"}).get_text().strip()
                room["deposit"] = tbody.find("span", {"class": "cassetteitem_price--deposit"}).get_text().strip()
                room["gratuity"] = tbody.find("span", {"class": "cassetteitem_price--gratuity"}).get_text().strip()
                room["layout"] = tbody.find("span", {"class": "cassetteitem_madori"}).get_text().strip()
                room["size"] = tbody.find("span", {"class": "cassetteitem_menseki"}).get_text().strip()
                room["url"] = urljoin("https://suumo.jp", tbody.find("a", {"class": "cassetteitem_other-linktext"}).get("href"))
                rooms.append(room)

        rooms_df = pd.DataFrame(rooms)
        return rooms_df

    def cleaning_rooms_data(self, rooms_df):
        regex_access_walk = re.compile(r"(.*線)/(.*駅) 歩(\d+)分")
        rooms_df["line1"] = rooms_df["station1"] = rooms_df["minutes_on_foot1"] = rooms_df["accesses1"]
        rooms_df.loc[~rooms_df["line1"].str.match(regex_access_walk), "line1"] = None
        rooms_df.loc[~rooms_df["station1"].str.match(regex_access_walk), "station1"] = None
        rooms_df.loc[~rooms_df["minutes_on_foot1"].str.match(regex_access_walk), "minutes_on_foot1"] = None
        rooms_df["line1"] = rooms_df["line1"].replace(regex_access_walk, r"\1", regex=True)
        rooms_df["station1"] = rooms_df["station1"].replace(regex_access_walk, r"\2", regex=True)
        rooms_df["minutes_on_foot1"] = rooms_df["minutes_on_foot1"].replace(regex_access_walk, r"\3", regex=True)
        rooms_df.drop(columns="accesses1", inplace=True)

        rooms_df["line2"] = rooms_df["station2"] = rooms_df["minutes_on_foot2"] = rooms_df["accesses2"]
        rooms_df.loc[~rooms_df["line2"].str.match(regex_access_walk), "line2"] = None
        rooms_df.loc[~rooms_df["station2"].str.match(regex_access_walk), "station2"] = None
        rooms_df.loc[~rooms_df["minutes_on_foot2"].str.match(regex_access_walk), "minutes_on_foot2"] = None
        rooms_df["line2"] = rooms_df["line2"].replace(regex_access_walk, r"\1", regex=True)
        rooms_df["station2"] = rooms_df["station2"].replace(regex_access_walk, r"\2", regex=True)
        rooms_df["minutes_on_foot2"] = rooms_df["minutes_on_foot2"].replace(regex_access_walk, r"\3", regex=True)
        rooms_df.drop(columns="accesses2", inplace=True)

        rooms_df["line3"] = rooms_df["station3"] = rooms_df["minutes_on_foot3"] = rooms_df["accesses3"]
        rooms_df.loc[~rooms_df["line3"].str.match(regex_access_walk), "line3"] = None
        rooms_df.loc[~rooms_df["station3"].str.match(regex_access_walk), "station3"] = None
        rooms_df.loc[~rooms_df["minutes_on_foot3"].str.match(regex_access_walk), "minutes_on_foot3"] = None
        rooms_df["line3"] = rooms_df["line3"].replace(regex_access_walk, r"\1", regex=True)
        rooms_df["station3"] = rooms_df["station3"].replace(regex_access_walk, r"\2", regex=True)
        rooms_df["minutes_on_foot3"] = rooms_df["minutes_on_foot3"].replace(regex_access_walk, r"\3", regex=True)
        rooms_df.drop(columns="accesses3", inplace=True)

        regex_age = re.compile(r"築(\d+)年(以上)*")
        rooms_df["age"] = rooms_df["age"].replace("新築", "1")
        rooms_df["age"] = rooms_df["age"].replace(regex_age, r"\1", regex=True)
        rooms_df["age"] = pd.to_numeric(rooms_df["age"], errors="coerce")

        regex_story = re.compile(r"(地下)*(\d*)(地上)*(\d+)階建")
        rooms_df["story"] = rooms_df["story"].replace("平屋", "1")
        rooms_df["story"] = rooms_df["story"].replace(regex_story, r"\4", regex=True)
        rooms_df["story"] = pd.to_numeric(rooms_df["story"], errors="coerce")

        regex_floor = re.compile(r"B*(\d+)*-*(\d+)階")
        rooms_df["floor"] = rooms_df["floor"].replace(regex_floor, r"\2", regex=True)
        rooms_df["floor"] = pd.to_numeric(rooms_df["floor"], errors="coerce")

        regex_price = re.compile(r"(\d+)万*円")
        rooms_df["rent"] = rooms_df["rent"].replace(regex_price, r"\1", regex=True)
        rooms_df["rent"] = pd.to_numeric(rooms_df["rent"], errors="coerce") * 10000

        rooms_df["administration"] = rooms_df["administration"].replace(regex_price, r"\1", regex=True)
        rooms_df["administration"] = rooms_df["administration"].str.replace("-", "0")
        rooms_df["administration"] = pd.to_numeric(rooms_df["administration"], errors="coerce")

        rooms_df["deposit"] = rooms_df["deposit"].replace(regex_price, r"\1", regex=True)
        rooms_df["deposit"] = rooms_df["deposit"].str.replace("-", "0")
        rooms_df["deposit"] = pd.to_numeric(rooms_df["deposit"], errors="coerce") * 10000

        rooms_df["gratuity"] = rooms_df["gratuity"].replace(regex_price, r"\1", regex=True)
        rooms_df["gratuity"] = rooms_df["gratuity"].str.replace("-", "0")
        rooms_df["gratuity"] = pd.to_numeric(rooms_df["gratuity"], errors="coerce") * 10000

        regex_size = re.compile(r"(\d+)m2")
        rooms_df["size"] = rooms_df["size"].replace(regex_size, r"\1", regex=True)
        rooms_df["size"] = pd.to_numeric(rooms_df["size"], errors="coerce")

        return rooms_df

    def parallel_process_func(self, url):
        rooms_df = self.fetch_rooms_data(url)
        rooms_df = self.cleaning_rooms_data(rooms_df)
        return rooms_df

    def scrape(self, url, output_dir):
        self.can_fetch(url)
        max_page_no = self.fetch_max_page_no(url)
        base_url = self.get_base_url(url)
        scraped_urls = [base_url + "&page=" + str(page_no) for page_no in range(1, max_page_no + 1)]

        rooms = pd.DataFrame()
        with tqdm(total=len(scraped_urls)) as pbar:
            future_to_url = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for scraped_url in scraped_urls:
                    future = executor.submit(self.parallel_process_func, scraped_url)
                    future_to_url[future] = scraped_url

                for future in concurrent.futures.as_completed(future_to_url):
                    scraped_url = future_to_url[future]
                    if future.exception() is None:
                        self.logger.info(f"Successed to scrape. URL: {scraped_url}")
                        rooms = pd.concat([rooms, future.result()])
                    else:
                        self.logger.error(f"Failed to scrape. URL: {scraped_url}")
                        self.logger.error(f"Exception: {repr(future.exception())}")
                    pbar.update(1)

        output_dir = Path(output_dir)
        os.makedirs(str(output_dir), exist_ok=True)
        rooms.to_csv(output_dir / (datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".csv"), index=False)


def main():
    config_ini = configparser.ConfigParser()
    config_ini.read("conf.ini", encoding="utf-8")

    logger = config.fileConfig(config_ini.get("logger", "ini_path"))
    logger = getLogger(__name__)

    scraper = SummoScraper()
    scraper.scrape(config_ini.get("scraper", "url"), config_ini.get("scraper", "output_dir"))


if __name__ == "__main__":
    main()
