import logging
import requests
from multiprocessing import set_start_method
import argparse

from bs4 import BeautifulSoup
from lm_dataformat import Archive


class Config:
    def __init__(self, dataset_url):
        parser = self.parse_program_arguments()

        args = parser.parse_args()

        self.DATASET_CATEGORY: str = args.DATASET_CATEGORY or "Forum"

        self.DATASET_URL: str = dataset_url or args.DATASET_URL or "https://forum.szajbajk.pl"

        name_tmp = self.DATASET_CATEGORY.lower() + "_" + "".join(self.DATASET_URL.split("//")[-1]
                                                                 .replace("https:", "")
                                                                 .replace("http:", "")
                                                                 .replace("www.", "")
                                                                 .replace(".", "_")
                                                                 .replace("forum_", "")) + "_corpus"
        self.DATASET_NAME: str = args.DATASET_NAME or name_tmp

        self.DATASET_DESCRIPTION: str = args.DATASET_DESCRIPTION or f"Collection of forum discussions from {self.DATASET_URL.split('//')[-1]}"

        self.LICENSE: str = args.DATASET_LICENSE or f"(c) {self.DATASET_URL.split('//')[-1]}"

        # we're targeting the full topics to optimize the performance
        self.EXPECTED_URL_PARTS = ['/viewtopic']
        # number of processes, from 1 up to os.cpu_count()
        self.PROCESSES = args.PROCESSES

        # waiting interval between requests
        self.TIME_SLEEP = args.TIME_SLEEP

        # urls interval at which script saves data, prevents from losing data if crashed or stopped
        self.SAVE_STATE = args.SAVE_STATE

        # minimal character count to consider it a text data
        self.MIN_LEN_TXT = args.MIN_LEN_TXT

    @staticmethod
    def parse_program_arguments():
        parser = argparse.ArgumentParser(
            description='Crawler and scraper - using sitemaps (in XML) and scrap text from Invision forums')
        parser.add_argument("-D_C", "--DATASET_CATEGORY", help="Set category e.g. Forum", default="", type=str)
        parser.add_argument("-D_U", "--DATASET_URL", help="Desire URL with http/https e.g. https://forumaddress.pl",
                            default="",
                            type=str)
        parser.add_argument("-D_N", "--DATASET_NAME", help="Dataset name e.g. forum_<url_domain>_pl_corpus", default="",
                            type=str)
        parser.add_argument("-D_D", "--DATASET_DESCRIPTION",
                            help="Description e.g. Collection of forum discussions from DATASET_URL", default="",
                            type=str)
        parser.add_argument("-D_L", "--DATASET_LICENSE", help="Dataset license e.g. (c) DATASET_URL", default="",
                            type=str)

        parser.add_argument("-proc", "--PROCESSES", help="Number of processes - from 1 up to os.cpu_count()", default=4,
                            type=int)
        parser.add_argument("-sleep", "--TIME_SLEEP", help="Waiting interval between requests (in sec)", default=0.2,
                            type=float)
        parser.add_argument("-save", "--SAVE_STATE",
                            help="URLs interval at which script saves data, prevents from losing data if crashed or stopped",
                            default=100, type=int)
        parser.add_argument("-min_len", "--MIN_LEN_TXT", help="Minimum character count to consider it a text data",
                            default=20,
                            type=int)

        return parser


class Utility:
    @staticmethod
    def get_soup_content_from_page_url(page_url):
        response = requests.get(page_url)

        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
        else:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")

    @staticmethod
    def contains_any_substring(input_string, substring_list):
        for substring in substring_list:
            if substring in input_string:
                return True
        return False

    @staticmethod
    def get_hrefs_from_links(html_links: list, whitelist_url_parts: list):
        page_links = []

        for link in html_links:
            href = link.get('href')
            if href and Utility.contains_any_substring(href, whitelist_url_parts):
                page_links.append(href)

        return page_links


class Crawler:
    def __init__(self, dataset_url: str):
        self.total_docs = 0
        self.config = Config(dataset_url)
        self.file_name_manifest = None
        self.file_name_zst = None
        self.crawled_archive = None
        self.topics_urls_list = None

    def create_manifest_and_dataset_file(self):
        # Create .jsonl.zst and .manifest files for the dataset
        self.file_name_zst: str = self.config.DATASET_NAME + '.jsonl.zst'
        self.file_name_manifest: str = self.config.DATASET_NAME + '.manifest'

        # Initialize the Archive
        self.crawled_archive = Archive(f'./data_{self.config.DATASET_NAME}')

    def get_topics_links_from_page(self, page_url):
        soup_main_page = Utility.get_soup_content_from_page_url(page_url)
        soup_main_page_links = soup_main_page.find_all('a')
        data_filtered_topics_urls = Utility.get_hrefs_from_links(soup_main_page_links, self.config.EXPECTED_URL_PARTS)
        self.topics_urls_list = data_filtered_topics_urls

    def get_all_pages_with_topic_links(self):
        pages_with_topics = []
        soup_paginate_page_item = Utility.get_soup_content_from_page_url(self.config.DATASET_URL)

    def get_topics_links(self):
        self.get_all_pages_with_topic_links()

    def get_data_from_topic_link(self):
        pass

    def start(self):
        # self.create_manifest_and_dataset_file()
        self.get_topics_links()
        self.get_data_from_topic_link()


def main():
    dataset_url = 'https://www.gry-planszowe.pl/'

    pbpbb_crawler = Crawler(dataset_url)
    pbpbb_crawler.start()


if __name__ == '__main__':
    try:
        set_start_method("spawn")
        main()
        logging.info("--- EVERYTHING WAS FINE +++")
    except Exception as e:
        logging.warning("--- UPS SOMETHING WENT WRONG +++", e)
