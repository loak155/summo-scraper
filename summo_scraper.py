import argparse
import datetime
import concurrent.futures
import logging
from urllib.parse import urljoin
import numpy as np
import os
import pandas as pd
import re
import time

from bs4 import BeautifulSoup
import requests
from tqdm import tqdm

from robots import Robots


MAX_RETRIES = 5
DELAY = 1
TIMEOUT = 10
MAX_WORKERS = None


class SummoScraper(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

        self.logger = self.get_logger(self.log_dir)

        self.items = []
        self.timestamp = None

        robots = Robots(self.url)
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

    def get_base_url(self, url):
        base_url = re.sub("&page=[0-9]*", "", url)
        return base_url

    def get_page_no(self, url):
        page = re.findall("&page=[0-9]*", url)
        if len(page) != 0:
            page_no = page[0].replace("&page=", "")
            if page_no == "":
                page_no = None
        else:
            page_no = None
        return page_no

    def get_max_page_no(self, url):
        soup = self._fetch_soup(url)

        pagination = soup.find("div", {"class": "pagination pagination_set-nav"})
        links = pagination.findAll("a")

        max_page_no = 1
        for link in reversed(links):
            if link.get_text().isdigit():
                max_page_no = int(link.get_text())
                break

        return max_page_no

    def get_rooms_data(self, url):
        rooms = []
        soup = self._fetch_soup(url)

        items = soup.findAll("div", {"class": "cassetteitem"})
        for item in items:
            building = {}

            building["名称"] = item.find("div", {"class": "cassetteitem_content-title"}).get_text().strip()
            building["カテゴリー"] = item.find("div", {"class": "cassetteitem_content-label"}).get_text().strip()
            building["アドレス"] = item.find("li", {"class": "cassetteitem_detail-col1"}).get_text().strip()
            accesses = item.find_all("div", {"class": "cassetteitem_detail-text"})
            building["アクセス1"] = accesses[0].get_text().strip() if accesses[0] is not None else None
            building["アクセス2"] = accesses[1].get_text().strip() if accesses[1] is not None else None
            building["アクセス3"] = accesses[2].get_text().strip() if accesses[2] is not None else None
            building["築年数"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[0].get_text().strip()
            building["階建"] = item.find("li", {"class": "cassetteitem_detail-col3"}).findAll("div")[1].get_text().strip()

            tbodys = item.find("table", {"class": "cassetteitem_other"}).findAll("tbody")

            for tbody in tbodys:
                room = building.copy()

                room["階数"] = tbody.findAll("td")[2].get_text().strip()
                room["家賃"] = tbody.find("span", {"class": "cassetteitem_price--rent"}).get_text().strip()
                room["管理費"] = tbody.find("span", {"class": "cassetteitem_price--administration"}).get_text().strip()
                room["敷金"] = tbody.find("span", {"class": "cassetteitem_price--deposit"}).get_text().strip()
                room["礼金"] = tbody.find("span", {"class": "cassetteitem_price--gratuity"}).get_text().strip()
                room["間取り"] = tbody.find("span", {"class": "cassetteitem_madori"}).get_text().strip()
                room["面積"] = tbody.find("span", {"class": "cassetteitem_menseki"}).get_text().strip()
                room["URL"] = urljoin("https://suumo.jp", tbody.find("a", {"class": "cassetteitem_other-linktext"}).get("href"))

                rooms.append(room)

        rooms_df = pd.DataFrame(rooms)
        return rooms_df

    def cleaning_rooms_data(self, rooms):
        access1 = rooms["アクセス1"].str.split(" 歩", n=1, expand=True)
        access1.columns = ["路線1/駅1", "徒歩1"]
        access1["徒歩1"] = access1["徒歩1"].str.replace("分", "")
        access1 = pd.concat([access1["路線1/駅1"].str.split("/", n=1, expand=True), access1["徒歩1"]], axis=1)
        access1.columns = ["路線1", "駅1", "徒歩1"]
        rooms = pd.concat([rooms, access1], axis=1)
        rooms.drop(columns="アクセス1", inplace=True)

        access2 = rooms["アクセス2"].str.split(" 歩", n=1, expand=True)
        access2.columns = ["路線2/駅2", "徒歩2"]
        access2["徒歩2"] = access2["徒歩2"].str.replace("分", "")
        access2 = pd.concat([access2["路線2/駅2"].str.split("/", n=1, expand=True), access2["徒歩2"]], axis=1)
        access2.columns = ["路線2", "駅2", "徒歩2"]
        rooms = pd.concat([rooms, access2], axis=1)
        rooms.drop(columns="アクセス2", inplace=True)

        access3 = rooms["アクセス3"].str.split(" 歩", n=1, expand=True)
        access3.columns = ["路線3/駅3", "徒歩3"]
        access3["徒歩3"] = access3["徒歩3"].str.replace("分", "")
        access3 = pd.concat([access3["路線3/駅3"].str.split("/", n=1, expand=True), access3["徒歩3"]], axis=1)
        access3.columns = ["路線3", "駅3", "徒歩3"]
        rooms = pd.concat([rooms, access3], axis=1)
        rooms.drop(columns="アクセス3", inplace=True)

        rooms["築年数"] = rooms["築年数"].str.replace("新築", "0")
        rooms["築年数"] = rooms["築年数"].str.replace("築", "")
        rooms["築年数"] = rooms["築年数"].str.replace("年", "")
        rooms["築年数"] = rooms["築年数"].str.replace("以上", "")

        rooms["階建"] = rooms["階建"].str.replace("地下[\d+]地上", "", regex=True)
        rooms["階建"] = rooms["階建"].str.replace("平屋", "1")
        rooms["階建"] = rooms["階建"].str.replace("階建", "")

        floor = rooms["階数"].str.split("-", expand=True)
        if len(floor.columns) == 1:
            floor["1"] = np.nan
        floor.columns = ["階1", "階2"]
        floor["階1"] = floor["階1"].str.replace("階", "")
        floor["階1"] = floor["階1"].str.replace("-", "0")
        floor["階1"] = floor["階1"].str.replace("B", "-")
        rooms = pd.concat([rooms, floor], axis=1)
        rooms.drop(columns="階数", inplace=True)

        rooms["家賃"] = rooms["家賃"].str.replace("万円", "")
        rooms["家賃"] = rooms["家賃"].str.replace("-", "0")

        rooms["管理費"] = rooms["管理費"].str.replace("円", "")
        rooms["管理費"] = rooms["管理費"].str.replace("-", "0")

        rooms["敷金"] = rooms["敷金"].str.replace("万円", "")
        rooms["敷金"] = rooms["敷金"].str.replace("-", "0")

        rooms["礼金"] = rooms["礼金"].str.replace("万円", "")
        rooms["礼金"] = rooms["礼金"].str.replace("-", "0")

        rooms["面積"] = rooms["面積"].str.replace("m2", "")

        rooms["築年数"] = pd.to_numeric(rooms["築年数"], errors="coerce")
        rooms["階建"] = pd.to_numeric(rooms["階建"], errors="coerce")
        rooms["階1"] = pd.to_numeric(rooms["階1"], errors="coerce")
        rooms["階2"] = pd.to_numeric(rooms["階2"], errors="coerce")
        rooms["家賃"] = pd.to_numeric(rooms["家賃"], errors="coerce")
        rooms["管理費"] = pd.to_numeric(rooms["管理費"], errors="coerce")
        rooms["敷金"] = pd.to_numeric(rooms["敷金"], errors="coerce")
        rooms["礼金"] = pd.to_numeric(rooms["礼金"], errors="coerce")
        rooms["築年数"] = pd.to_numeric(rooms["築年数"], errors="coerce")
        rooms["面積"] = pd.to_numeric(rooms["面積"], errors="coerce")

        rooms["家賃"] = rooms["家賃"] * 10000
        rooms["敷金"] = rooms["敷金"] * 10000
        rooms["礼金"] = rooms["礼金"] * 10000

        return rooms

    def save_csv(self, df, filepath):
        filepath = str(os.path.splitext(filepath)[0] + ".csv")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df.to_csv(filepath, index=False)

    def template(self, item):
        if self.timestamp is None:
            self.timestamp = datetime.datetime.now()

        template_value = {
            "url": item["url"],
            "page_no": str(item["page_no"]),
            "datetime": self.timestamp.strftime("%Y%m%d%H%M%S"),
            "date": self.timestamp.strftime("%Y%m%d"),
            "year": self.timestamp.strftime("%Y"),
            "month": self.timestamp.strftime("%m"),
            "day": self.timestamp.strftime("%d"),
            "h": self.timestamp.strftime("%H"),
            "m": self.timestamp.strftime("%M"),
            "s": self.timestamp.strftime("%S"),
        }

        return item["filepath"].format(**template_value)

    def scrape_summo(self, item):
        item["room"] = self.get_rooms_data(item["url"])
        item["room"] = self.cleaning_rooms_data(item["room"])

        if self.should_save_temp:
            item["filepath"] = str(os.path.splitext(item["filepath"])[0] + "_{page_no}.csv")
            item["filepath"] = self.template(item)
            self.save_csv(item["room"], item["filepath"])

        return item

    def scrape(self):
        save_filepath = os.path.join(self.save_dir, self.save_filename)

        if self.should_turn_page:
            self.max_page_no = self.get_max_page_no(self.url)
            self.base_url = self.get_base_url(self.url)
            for page_no in range(1, self.max_page_no + 1):
                self.items.append({"url": self.base_url + "&page=" + str(page_no), "page_no": page_no, "filepath": save_filepath})
        else:
            self.items.append({"url": self.url, "page_no": self.get_page_no(self.url), "filepath": save_filepath})

        rooms = pd.DataFrame()
        with tqdm(total=len(self.items)) as pbar:
            future_to_url = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for item in self.items:
                    future = executor.submit(self.scrape_summo, item)
                    future_to_url[future] = item

                for future in concurrent.futures.as_completed(future_to_url):
                    item = future_to_url[future]
                    if future.exception() is None:
                        self.logger.info(f"Successed to scrape. URL: {item['url']}")
                        rooms = pd.concat([rooms, future.result()["room"]])
                    else:
                        self.logger.error(f"Failed to scrape. URL: {item['url']}")
                        self.logger.error(f"Exception: {repr(future.exception())}")
                    pbar.update(1)

        self.save_csv(rooms, self.template({"url": self.url, "page_no": None, "filepath": save_filepath}))

    @staticmethod
    def get_logger(log_dir="./"):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s")

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.ERROR)
        stream_handler.setFormatter(fmt)
        logger.addHandler(stream_handler)

        log_path = os.path.join(log_dir, "summo_scraper.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

        return logger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="SUMMO URL")
    parser.add_argument("--save_dir", "-s", type=str, default="./", help="保存ディレクトリ")
    parser.add_argument("--save_filename", "-f", type=str, default="{datetime}", help="保存ファイル名")
    parser.add_argument("--log_dir", "-l", type=str, default="./", help="ログディレクトリ")
    parser.add_argument("--should_turn_page", "-T", default=False, action="store_true", help="ページをめくるか")
    parser.add_argument("--should_save_temp", "-S", default=False, action="store_true", help="中間ファイルを保存するか")
    args = parser.parse_args()

    scraper = SummoScraper(**vars(args))
    scraper.scrape()


if __name__ == "__main__":
    main()
