import os
import glob
import json
import urllib.request, urllib.parse, urllib.robotparser
import time, datetime
import logging
import sys
from multiprocessing import set_start_method, Pool, current_process
import argparse

import requests
from requests.adapters import HTTPAdapter  # install requests
from urllib3.util.retry import Retry  # install urllib3
from bs4 import BeautifulSoup  # install beautifulsoup4
from usp.tree import sitemap_tree_for_homepage  # install ultimate-sitemap-parser
from lm_dataformat import Archive, Reader  # install lm-dataformat
from tqdm import tqdm  # install tqdm
import pandas as pd  # install pandas
import psutil  # install psutil


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

    def save_visited_dataframe(self,
                               urls: pd.DataFrame,
                               file_name: str,
                               head=False,
                               mode='a') -> None:
        """
        Saves the urls to txt file.

        Parameters
        ----------
        urls : pandas.DataFrame
            DataFrame containing processed urls (and columns with data).

        file_name : str
            Name of txt file.

        head : bool
            True / False - if use header in df.to_csv function.

        mode : char
            Mode to use while opening file in df.to_csv function.

        """

        urls.to_csv(file_name, sep='\t', header=head, mode=mode, index=False)
        logging.info(f"SAVE // Saved file -> DataFrame: {urls.shape} -> {file_name}")

    @staticmethod
    def tree_sitemap(url: str):
        """
        Uses the Ulitmate Sitemap Parser's sitemap_tree_for_homepage method to get the sitemap and extract all the URLs.

        Parameters
        ----------
        url : str
            Domain URL to get sitemap.

        Returns
        -------
        tree : AbstractSitemap
            Tree of AbstractSitemap subclass objects that represent the sitemap hierarchy found on the website.

        """
        start_time = time.perf_counter()
        tree = sitemap_tree_for_homepage(url)
        end_time = time.perf_counter()
        logging.info(
            f"SITEMAP_TREE // Sitemap DONE | Time = {end_time - start_time} sec = {(end_time - start_time) / 60} min")
        return tree

    def urls_generator(self, tree) -> list[str]:
        """
        Uses the Ulitmate Sitemap Parser's sitemap_tree_for_homepage method to get the sitemap and extract all the URLs.

        Parameters
        ----------
        tree : AbstractSitemap
            Tree with sitemaps (class AbstractSitemap).

        Returns
        -------
        list[str]
            Yielded output with urls to scrap.

        """
        # Extract all the URLs with EXPECTED_URL_PARTS in it
        urls_expected: list[str] = []

        for page in tree.all_pages():
            if any(url_part in page.url for url_part in self.config.EXPECTED_URL_PARTS):
                urls_expected.append(page.url)

        logging.info(f"URL_GEN // URL Generator -> URLs_expected: {len(urls_expected)}")
        return urls_expected

    def files_check(self,
                    urls_filename: str = None,
                    visited_filename: str = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Checking if exists files with:
        1) forum urls - if not create sitemaps tree -> generate urls -> save to file.
        2) visited urls - if not create empty file.

        Parameters
        ----------
        urls_filename : str
            Filename for CSV file with generated urls from sitemaps tree --> 3 columns = ['urls', 'visited', 'skip'] (sep = '\t').

        visited_filename : str
            Filename for CSV file with visited urls --> 3 columns = ['urls', 'visited', 'skip'] (sep = '\t').

        Returns
        -------
        forum_links : pd.DataFrame
            DataFrame (pd) with urls generated from sitemaps tree.

        visited_links : pd.DataFrame
            DataFrame (pd) with visited urls and info if visited (1) and skipped (0 or 1).
        """
        if urls_filename is None:
            urls_filename = f"Forum_URLs_{self.config.DATASET_URL}.csv",

        if visited_filename is None:
            visited_filename = f"Visited_{self.config.DATASET_URL}.csv"

        sitemap_tree = []
        forum_links = pd.DataFrame(columns=['urls', 'visited', 'skip'])
        visited_links = pd.DataFrame(columns=['urls', 'visited', 'skip'])
        flag_fresh_start = False

        # Check if file with URLs from sitemaps exist, if not search for sitemaps and return links
        if os.path.exists(path=urls_filename):
            logging.info(f"*** Resuming from previous progress... ***")
            # Read the saved URLs from the file
            logging.info(f"FILE_CHECK // Importing DataFrame for: {self.config.DATASET_URL} ...")
            forum_links = pd.read_csv(urls_filename, sep='\t', header=None, names=['urls', 'visited', 'skip'])
            logging.info(
                f"FILE_CHECK // Imported DataFrame for: {self.config.DATASET_URL} | Shape: {forum_links.shape} | Size in memory (MB): {forum_links.memory_usage(deep=True).sum() / pow(10, 6)}")
        else:
            logging.info(f"*** Starting from scratch... ***")
            # Create sitemap tree for domain url
            logging.info(f"FILE_CHECK // Creating sitemaps tree...")
            sitemap_tree = self.tree_sitemap(self.config.DATASET_URL)
            logging.info(f"FILE_CHECK // Created sitemaps tree for: {self.config.DATASET_URL}")

            # Create DataFrame with URLs + columns: 'visited','skip'
            forum_links = pd.DataFrame(self.urls_generator(sitemap_tree), columns=['urls'])
            if forum_links.shape[0] == 0:
                logging.error(
                    f"FILE_CHECK + SITEMAP // No valid URL found in SITEMAPs -> Check it manually ({forum_links.shape=})")
                raise ValueError("No valid URL found in SITEMAPs.")
            forum_links.drop_duplicates(inplace=True, ignore_index=True)
            forum_links['visited'] = 0
            forum_links['skip'] = 0
            logging.info(
                f"FILE_CHECK // Created DataFrame for: {self.config.DATASET_URL} | Shape: {forum_links.shape} | Size in memory (MB): {forum_links.memory_usage(deep=True).sum() / pow(10, 6)}")
            self.save_visited_dataframe(urls=forum_links, file_name=urls_filename)
            flag_fresh_start = True

        # Check if file with visited is created, if not then create
        if os.path.exists(visited_filename) and flag_fresh_start == False:
            logging.info(f"FILE_CHECK // Reading file with visited links ...")
            visited_links = pd.read_csv(visited_filename, sep='\t', header=None, names=['urls', 'visited', 'skip'])
            visited_links.drop_duplicates(inplace=True, ignore_index=True)
            logging.info(
                f"FILE_CHECK // File for saving visited links already created: {visited_filename} | Lines: {visited_links.shape} | Visited: {visited_links['visited'].sum()} | Skipped: {visited_links['skip'].sum()}")
        else:
            with open(visited_filename, "w") as f:
                pass
            logging.info(f"FILE_CHECK // File for saving visited links created: {visited_filename}")

        return forum_links, visited_links

    def initialize_worker(url: str, visited_urls: list[str]) -> None:
        """
        Initialize the workers (parser and session) for multithreading performace.

        Parameters
        ----------
        url : str
            Domain URL.

        visited_urls : list[str]
            All visited URLs - can be empty or passed from file.
        """
        if psutil.LINUX == True:
            logging.info(f'INIT_WORKER // Initializing worker... | CPU Core: {psutil.Process().cpu_num()}')
        else:
            logging.info(f'INIT_WORKER // Initializing worker... 1 of many! We Wanna Work!')

        global rp
        global session
        global all_visited_urls
        all_visited_urls = visited_urls

        rp = urllib.robotparser.RobotFileParser()
        with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'Python'})) as response:
            rp.parse(response.read().decode("utf-8").splitlines())

        session = requests.Session()
        retry = Retry(total=3, backoff_factor=3)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        if psutil.LINUX == True:
            logging.info(
                f"INIT_WORKER // Created: RobotFileParser & requests.Session | CPU Core: {psutil.Process().cpu_num()}")
        else:
            logging.info(f"INIT_WORKER // Created: RobotFileParser & requests.Session - 1 of many")

    def get_item_text(self, url: str) -> str:
        """
        Extracts text data from URL (from urls_generator).

        Parameters
        ----------
        url : str
            URL to get text data.

        Returns
        -------
        text : str
            Text data.

        """

        # Variables
        response = None
        text = ''
        headers = {
            'User-Agent': 'Speakleash-v0.1',
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive"
        }

        # Try to connect to a given URL
        try:
            response = session.get(url,
                                   timeout=60,
                                   headers=headers)

        # Handle connection error
        except Exception as e:
            logging.error(f"GET_TEXT // Error downloading -> {url} : {str(e)}")

            # Connection successful
        if response and response.ok:

            # Check if the file exceeds 15 MB
            if len(response.content) > 15000000:
                logging.warning(f"GET_TEXT // File too big")
                return text

            # Beautiful Soup to extract data from HTML
            try:
                soup = BeautifulSoup(response.content, "html.parser")
                comment_blocks = soup.find_all("div", {'data-role': "commentContent"})
            except Exception as e:
                logging.error(f"GET_TEXT // ERROR BeautifulSoup: {str(e)}")
            # Get text data from posts on page and add it to the string
            for comment in comment_blocks:
                text += comment.text.strip() + "\n"

            # Process next pages
            try:
                # Sleep for convinience
                time.sleep(self.config.TIME_SLEEP)

                # Iterate through all of the pages in given topic/thread
                while len(soup.find_all('li', {'class': 'ipsPagination_next'})) > 0 and \
                        len(soup.find_all('li', class_='ipsPagination_next ipsPagination_inactive')) == 0:

                    next_page_btns = soup.find_all('li', {'class': 'ipsPagination_next'})
                    next_page_url = next_page_btns[0].find('a')['href']

                    if url in next_page_url:
                        logging.debug(
                            f"GET_TEXT // Found new page: {next_page_url.replace(url, '')} --> for topic: {url}")

                        next_page_response = session.get(next_page_url, timeout=60, headers=headers)
                        soup = BeautifulSoup(next_page_response.content, "html.parser")

                        comment_blocks = soup.find_all("div", {'data-role': "commentContent"})

                        for comment in comment_blocks:
                            text += comment.text.strip() + "\n"
                        # for i in page_nav_results:
                        #    text += i.text.strip()+"\n"
                        time.sleep(self.config.TIME_SLEEP)
                    else:
                        logging.debug(f"GET_TEXT // Topic URL is NOT in next_page_url: {next_page_url=}")
                        break

            # Handle next page error
            except Exception as e:
                logging.error(f"GET_TEXT // ERROR processing next page: {next_page_url} : {str(e)}")

                # Connection not successful - reponse empty
        elif not response:
            logging.warning(f"GET_TEXT // Empty response -> {url}\nResponse: {response}")

        # Connection not successful - error
        elif not response.ok:
            logging.error(f"GET_TEXT // Error response -> {url}\nResponse: {response.status_code}")

        return text

    def process_item(self, url: str) -> tuple[str, dict]:
        """
        Extract from URL -> cleaning -> simple metadata.

        Parameters
        ----------
        url : str
            URL to get text data.

        Returns
        -------
        text : str
            Text data (stripped).

        meta : dict
            Dict with simple metadata -> {'url' : url, 'length': len(txt_strip)} or {'url' : url, 'skip': 'error' / 'robots.txt' / 'visited'}.
        """

        global rp
        global all_visited_urls
        meta: dict = {'url': url}
        txt: str = ''
        txt_strip: str = ''

        if psutil.LINUX == True:
            logging.debug(
                f"PROCESS_ITEM // Proc ID: {psutil.Process().pid} | CPU Core: {psutil.Process().cpu_num()} | Processing URL: {url}")
        else:
            logging.debug(f"PROCESS_ITEM // Proc ID: {psutil.Process().pid} | Processing URL: {url}")

        if url not in all_visited_urls:
            if rp.can_fetch('*', url):
                try:
                    txt = self.get_item_text(url)
                    meta = {'url': url, 'skip': 'error'}
                    if txt:
                        txt_strip = txt.strip()
                        meta = {'url': url, 'length': len(txt_strip)}
                except Exception as e:
                    logging.error(f"PROCESS_ITEM // Error processing item -> {url} : {str(e)}")
                    meta = {'url': url, 'skip': 'error'}
            else:
                logging.info(f"PROCESS_ITEM // Robots not allowed: {url}")
                meta = {'url': url, 'skip': 'robots.txt'}
        else:
            logging.info(f"PROCESS_ITEM // URL already visited -> skipping: {url}")
            meta = {'url': url, 'skip': 'visited'}

        # Only on LINUX
        # logging.info(f"PROCESS_ITEM // Metadata: {meta} | CPU Core: {psutil.Process().cpu_num()}")
        return txt_strip, meta

    def scrape_data(self, ar: Archive) -> int:
        """
        Extract text data from URL using multiprocessing.
        Init -> MP Pool -> Extract -> Save URLs and update Archive.

        Parameters
        ----------
        ar : Archive
            Archive class from library lm_dataformat.

        fresh_start : bool
            Determine if file with visited urls exist (False) or not (True).

        Returns
        -------
        total_docs : int
            Total number of added documents.
        """

        # Check if previous progress exists
        # Check if exists files with: 1) forum urls, 2) visited urls
        filename_forum_urls: str = f"Forum_URLs_{self.config.DATASET_NAME}.csv"
        filename_visited: str = f"Visited_{self.config.DATASET_NAME}.csv"
        forum_links = pd.DataFrame(columns=['urls', 'visited', 'skip'])
        visited_links = pd.DataFrame(columns=['urls', 'visited', 'skip'])

        try:
            forum_links, visited_links = self.files_check(urls_filename=filename_forum_urls,
                                                          visited_filename=filename_visited)
        except Exception as e:
            logging.error(f"*** ERROR *** Something wrong with CSV files: {str(e)}")
            raise e

        logging.info(f"Forum links duplicated: {forum_links['urls'].duplicated().sum()}")
        forum_links.drop_duplicates(inplace=True, ignore_index=True)

        logging.info(f"Visited links duplicated: {visited_links['urls'].duplicated().sum()}")
        visited_links.drop_duplicates(inplace=True, ignore_index=True)

        filtered_forum_urls = forum_links[~forum_links['urls'].isin(visited_links['urls'])]
        len_urls_left = int(filtered_forum_urls.shape[0])
        logging.info(f"SCRAPE // *** *** ***")
        logging.info(f"SCRAPE // URLs to check: {len_urls_left}")

        total_docs: int = (visited_links['visited'].sum() - visited_links['skip'].sum())
        total_visited: int = visited_links['visited'].sum()

        if len_urls_left and len_urls_left != 0:
            logging.info(f"SCRAPE // Start scraping... in 5 sec")
            time.sleep(5)

            # Temp values, Placeholders will be updated in postprocessing
            added: int = 0
            skipped: int = 0  # Will be checked if visited -> in pool
            total: int = 0
            visited_urls_dataframe = pd.DataFrame(columns=['urls', 'visited', 'skip'])
            time_loop_start = time.time()
            total_checkpoint = 0
            added_checkpoint = 0
            skipped_checkpoint = 0

            # Create and configure the process pool
            logging.info(f"SCRAPE // Starting Multiprocessing Pool...")
            with Pool(initializer=self.initialize_worker,
                      initargs=[self.config.DATASET_URL, visited_links['urls'].values.tolist()],
                      processes=self.config.PROCESSES) as pool:

                time_loop_start = time.time()

                try:
                    # Issue tasks to the process pool for remaining URLs
                    for txt, meta in pool.imap(func=self.process_item,
                                               # iterable = urls_generator(sitemap_tree),
                                               iterable=filtered_forum_urls['urls'],
                                               # iterable = url_pd_lazy_gen(filtered_forum_urls, colname='urls'),
                                               # Got more RAM allocated
                                               chunksize=1):
                        # ,total = len_urls_left, leave = False, ):
                        total += 1
                        flag_visited: int = 0
                        flag_skip: int = 0
                        visit_temp: dict = {}

                        if txt and len(txt) > self.config.MIN_LEN_TXT:
                            total_docs += 1
                            ar.add_data(txt, meta=meta)
                            added += 1
                            flag_visited = 1
                            flag_skip = 0
                            visit_temp = {'urls': [meta.get('url')], 'visited': [flag_visited], 'skip': [flag_skip]}
                            # logging.info(f"SCRAPE // OK --- Processed: {total} | Added counter: {added} | Len(txt): {meta.get('length')} | Added URL: {meta.get('url')}")
                        else:
                            skipped += 1
                            flag_visited = 1
                            flag_skip = 1
                            if meta.get('skip') != 'visited':
                                visit_temp = {'urls': [meta.get('url')], 'visited': [flag_visited], 'skip': [flag_skip]}
                            # logging.info(f"SCRAPE // Short or empty TXT --- Processed: {total} | Skipped counter: {skipped} | Why skipped: {meta.get('skip')} | Skipped URL: {meta.get('url')}")

                        if visit_temp:
                            # print(f"VISIT_TEMP EXIST ---> {visit_temp}")
                            visited_urls_dataframe = pd.concat(
                                [visited_urls_dataframe, pd.DataFrame(visit_temp)], ignore_index=True)

                        if len(pool._pool) != self.config.PROCESSES:
                            logging.error(
                                f"*** ERROR *** Ups, something went wrong --> pool got: {len(pool._pool)} workers, should be {self.config.PROCESSES}")

                        # Save visited URLs to file
                        if total % self.config.SAVE_STATE == 0 and added > 0:
                            logging.info(
                                f"SCRAPE // Scraping info --> Checked URLs: {total_visited + total} | Added docs: {total_docs}")
                            logging.info(
                                f"SCRAPE // This session --> Checked URLs: {total} | Added: {added}  | Skipped: {skipped}")
                            logging.info(
                                f"SCRAPE // Since last checkpoint --> Checked URLs: {total - total_checkpoint} | Added: {added - added_checkpoint}  | Skipped: {skipped - skipped_checkpoint}")
                            total_checkpoint = total
                            added_checkpoint = added
                            skipped_checkpoint = skipped

                            # logging.info(f"SCRAPE // Saving visited URLs to file, visited: {visited_urls_dataframe.shape[0]}")
                            self.save_visited_dataframe(visited_urls_dataframe)
                            visited_urls_dataframe = pd.DataFrame(columns=['urls', 'visited', 'skip'])

                            ar.commit()
                            logging.info(f"SCRAPE + SAVE // Commiting to Archive, total commited = {added}")
                            # logging.info("SCRAPE // Commited to Archive - DONE | Visited URLs saved - DONE")

                            time_loop_end = time.time()
                            time_since_start = time_loop_end - time_loop_start + 1e-9
                            remains_iter = len_urls_left - total
                            time_per_iter = time_since_start / (total + 1)
                            time_eta = remains_iter * time_per_iter
                            logging.info(
                                f"SCRAPE + TIMING // *** Time since start: {(time_since_start / 60):.2f} min | {(time_since_start / 3600):.2f} h | {(time_since_start / 86400):.2f} days")
                            logging.info(
                                f"SCRAPE + TIMING // *** Performance: {(total / time_since_start):.2f} ops/sec  |  Processes = {len(pool._pool)} / {self.config.PROCESSES}")
                            logging.info(
                                f"SCRAPE + TIMING // *** ETA: {(time_eta / 60):.2f} min | {(time_eta / 3600):.2f} hours | {(time_eta / 86400):.2f} days --> {(datetime.datetime.today() + datetime.timedelta(seconds=time_eta)).strftime('%Y-%m-%d %H:%M (%A)')}")
                            logging.info(
                                f"SCRAPE // ------------------------------------------------------------------- ")

                except Exception as e:
                    logging.error(f"*** ERROR *** --> {str(e)}")

                logging.info(
                    f"SCRAPE // Scraping DONE! --> Checked URLs: {total_visited + total} | Added docs: {total_docs} ||| This session --> Checked URLs: {total} | Added: {added}  | Skipped: {skipped}")

                # Saving Archive and visited URLs
                ar.commit()
                self.save_visited_dataframe(visited_urls_dataframe)
                logging.info(f"SCRAPE // Saved URLs and Archive - DONE!")
        else:
            logging.info(f"SCRAPE // Nothing to scrape...")

        self.total_docs = total_docs

    def handle_scrape_data(self):
        try:
            self.scrape_data(self.crawled_archive)
        except Exception as e:
            logging.info(f"*** Error while scraping... | Error: {str(e)} ***")
            sys.exit()

        logging.info(f"*** Scraping is done -> preparing merge archives ***")

    def merge_archive(self) -> tuple[str, int, int]:
        """
        Create new Archive in: Archive_Merged_{DATASET_NAME}.
        Merge all existing archive chunks into one file, checking duplicates along the way.

        Parameters
        ----------
        None

        Returns
        -------
        data_merge[-1] : str
            Filename with prepared merged archive (last in folder).

        len_archive_merged : int
            Number of documents in archive after merging process is complete.

        total_docs_len : int
            Total length of documents in finale merged archive.
        """

        start_time = time.perf_counter()

        logging.info(f"MERGE_ARCH // Merging Archives in: {self.config.DATASET_NAME}")

        # merged_file_path = f"./{DATASET_NAME}_merged.jsonl.zst"
        merged_file_path: str = f"./archive_merged_{self.config.DATASET_NAME}"
        ar_merge = Archive(merged_file_path)

        # Re-write chunks of archives to 1 merged
        data_files = glob.glob(f'./data_{self.config.DATASET_NAME}/*.zst')

        urls_visited = []
        urls_duplicated = 0
        total_docs = 0
        total_docs_len = 0

        for file_path in tqdm(data_files):
            arch_part = Reader(file_path)
            for id, record in enumerate(arch_part.stream_data(get_meta=True)):
                urel = record[1].get('url')
                if urel not in urls_visited:
                    urls_visited.append(urel)
                    ar_merge.add_data(data=record[0], meta=record[1])
                    total_docs += 1
                    total_docs_len += record[1].get('length')
                else:
                    urls_duplicated += 1
        ar_merge.commit()

        logging.info(f"MERGE_ARCH // Archive MERGED --> Total docs: {total_docs} | Duplicated: {urls_duplicated}")

        # Read merged archive
        data_merge = glob.glob(f'{merged_file_path}/*.zst')
        data_merge.sort()
        len_archive_merged = 0
        ar_merge_reader = Reader(merged_file_path)

        # archive_check = pandas.DataFrame(columns=['doc_len', 'meta_len_doc', 'meta_url'])

        for id, doc in enumerate(ar_merge_reader.read_jsonl_zst(data_merge[-1], get_meta=True)):
            # if id in [1,2,3,4,5]:
            #     print(f"ID: {id}")
            #     print(doc[0])
            #     print(doc[1])
            #     print('\n')

            # archive_check = pandas.concat([archive_check, pandas.DataFrame({'doc_len': [len(doc[0])], 'meta_len_doc': [doc[1].get('length')], 'meta_url': [doc[1].get('url')]})], ignore_index=True)
            len_archive_merged = id

        len_archive_merged = len_archive_merged + 1
        logging.info(
            f"MERGE_ARCH // Checked Archive --> joined - DONE! | Docs: {len_archive_merged} | File: {data_merge[-1]}")

        # archive_check_urls_duplicated = archive_check['meta_url'].duplicated().sum()
        #
        # archive_check_miss_length = 0
        # for x in archive_check:
        #     if x['doc_len'] == x['meta_len_doc']:
        #         archive_check_miss_length += 1

        # logging.info(f"Archive info --> Duplicated URLs: {archive_check_urls_duplicated} | Wrong length: {archive_check_miss_length}")
        # print(archive_check.describe())

        end_time = time.perf_counter()
        logging.info(
            f"MERGE_ARCH // Archive merged and checked - DONE! | Time = {(end_time - start_time):.2f} sec = {((end_time - start_time) / 60):.2f} min")
        return data_merge[-1], len_archive_merged, total_docs_len

    def merge_archives(self):
        file_name_temp_zst: str = ''
        total_docs_archive: int = 0
        total_docs_len: int = 0

        try:
            file_name_temp_zst, total_docs_archive, total_docs_len = self.merge_archive()
            # new_name_zst = datetime.datetime.today().strftime('%Y%m%d%H%M%S') + '-' + file_name_zst
            new_name_zst = self.file_name_zst  # Archive name should not be enchanced with timestamp as it'll need additional actions
            os.rename(file_name_temp_zst, new_name_zst)
            file_name_temp_zst = new_name_zst

            if file_name_temp_zst and os.path.exists(file_name_temp_zst):
                logging.info(f"*** Archive SAVED as file: {file_name_temp_zst}")
        except Exception as e:
            logging.error(f"*** Error while mergind archives: {str(e)}")
        logging.info(f"*** Archives merge is done -> preparing dataset manifest... ***")

        if self.total_docs == total_docs_archive:
            logging.info(f"* Total number of documents was checked: {self.total_docs=} == {total_docs_archive=}")
        else:
            logging.warning(
                f"*** Total number of documents was checked: {self.total_docs=} != {total_docs_archive=} --> using {total_docs_archive=}")

    def start(self):
        self.create_manifest_and_dataset_file()
        self.handle_scrape_data()


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
