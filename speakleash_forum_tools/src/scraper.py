"""
Scraper Module

This module provides a comprehensive solution for web scraping activities, particularly focused on forum data extraction. It encapsulates the scraping logic within the Scraper class, which utilizes multiprocessing to efficiently handle large-scale data scraping tasks.

Classes:
- Scraper: A class designed to manage the scraping process. It utilizes multiprocessing techniques to optimize the scraping of text data from web pages, handling the complexities of concurrent data processing.

Dependencies:
- multiprocessing: Utilized for creating a pool of processes to enable concurrent scraping of data.
- Pool from multiprocessing: Used for managing a pool of worker processes.
- Other relevant libraries (e.g., requests, BeautifulSoup) as needed for web scraping.

The Scraper class within this module is responsible for setting up a multiprocessing environment to concurrently process multiple web scraping tasks. It includes methods for initializing the scraping process, handling individual scraping tasks, and managing the results. The class is designed to be adaptable to various scraping requirements, with a focus on efficiency and robust error handling.
"""
import time
import logging
from logging.handlers import QueueHandler
import datetime
import urllib3
from urllib.parse import urljoin
from typing import Tuple
# from multiprocessing import set_start_method, Pool, freeze_support
import multiprocessing

import psutil
import pandas
from tqdm import tqdm
from bs4 import BeautifulSoup
from speakleash_forum_tools.src.config_manager import ConfigManager
from speakleash_forum_tools.src.crawler_manager import CrawlerManager
from speakleash_forum_tools.src.forum_engines import ForumEnginesManager
from speakleash_forum_tools.src.archive_manager import ArchiveManager, Archive
from speakleash_forum_tools.src.utils import create_session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)     # Supress warning about 'InsecureRequest' (session.get(..., verify = False))

logger_tool = logging.getLogger('sl_forum_tools')

class Scraper:
    """
    A class responsible for managing the scraping process of forum data using multiprocessing.

    Attributes:
        config (ConfigManager): An instance of ConfigManager providing configuration settings.
        crawler (CrawlerManager): An instance of CrawlerManager for managing crawling operations.
        arch_manager (ArchiveManager): Manages the archive of scraped data.
        archive (Archive): An instance of Archive to store scraped data.
        text_separator (str): Separator used in text extraction.

    Methods:
        start_scraper: Initiates the scraping process and returns the total number of documents scraped.
        _initialize_worker: Static method to initialize worker processes for multiprocessing.
        _get_item_text: Static method to extract text and metadata from a given URL.
        _process_item: Static method to process individual items (URLs) and return text and metadata.
        _scrap_txt_mp: Orchestrates the scraping process using multiprocessing.
    """
    def __init__(self, config_manager: ConfigManager, crawler_manager: CrawlerManager):
        self.config = config_manager
        self.logger_tool = self.config.logger_tool
        self.logger_print = self.config.logger_print

        self.crawler = crawler_manager

        self.arch_manager = ArchiveManager(self.crawler.dataset_name, self.crawler._get_dataset_folder(), self.crawler.topics_visited_file,
                                           logger_tool = self.logger_tool, logger_print = self.logger_print, print_to_console = self.config.print_to_console)
        self.arch_manager.create_empty_file(self.crawler.visited_topics, self.crawler.topics_visited_file)
        self.archive = self.arch_manager.archive
        
        self.text_separator = '\n'


    ### Functions ###

    def start_scraper(self, urls_to_scrap: pandas.DataFrame) -> int:
        """
        Initiates the scraping process and returns the total number of documents scraped.

        :return: Total number of documents successfully scraped.
        """
        total_docs: int = 0
        try:
            total_docs: int = self._scrap_txt_mp(ar = self.archive,
                                             topics_minus_visited = urls_to_scrap,
                                             visited_topics = self.crawler.visited_topics)
        except Exception as e:
            self.logger_tool.error(f"Error in SCRAPER -> Error: {e}")
            total_docs: int = 0

        self.logger_tool.info(f"*** Scraper found documents: {total_docs}")
        self.logger_print.info(f"* Scraper found documents: {total_docs}")
        return total_docs

    @staticmethod
    def _initialize_worker(visited_urls: list[str], headers_in: dict, content_class_in: list[str],
                           topic_title_class_in: list[str], text_separator_in: str,
                           pagination_in: list[str], time_sleep_in: float, dataset_url_in: str, queue, log_lvl) -> None:
        """
        Initialize the workers (parser and session) for multithreading performace.

        :param visited_urls (list[str]): All visited URLs.
        """
        global loggur
        loggur = logging.getLogger('sl_forum_tools')
        qh = QueueHandler(queue)
        loggur.addHandler(qh)
        loggur.setLevel(log_lvl)

        if psutil.LINUX == True:
            loggur.info(f"INIT_WORKER // Initializing worker... | Proc ID: {psutil.Process().pid} | CPU Core: {psutil.Process().cpu_num()}")
        else:
            loggur.info(f"INIT_WORKER // Initializing worker... | Proc ID: {psutil.Process().pid}")

        global session
        session = create_session()

        global all_visited_urls
        all_visited_urls = visited_urls

        global headers
        headers = headers_in

        global forum_content_class
        forum_content_class = content_class_in

        global forum_topic_title_class
        forum_topic_title_class = topic_title_class_in

        global text_separator
        text_separator = text_separator_in

        global pagination
        pagination = pagination_in

        global time_sleep
        time_sleep = time_sleep_in

        global DATASET_URL
        DATASET_URL = dataset_url_in

        if psutil.LINUX == True:
            loggur.info(f"INIT_WORKER // Created: requests.Session | Proc ID: {psutil.Process().pid} | CPU Core: {psutil.Process().cpu_num()}")
        else:
            loggur.info(f"INIT_WORKER // Created: requests.Session | Proc ID: {psutil.Process().pid}")

    @staticmethod
    def _get_item_text(url: str) -> Tuple[str, str]:
        """
        Extracts text data from URL.

        :param url (str): URL to scrapget text data.

        :return: Tuple with 1) text - text data from given URL, 2) topic_title - topic title searched in specific HTML tags.
        """
        # Variables
        global headers
        global forum_content_class
        global forum_topic_title_class
        global text_separator
        global pagination
        global time_sleep
        global DATASET_URL

        response = None
        text = ''
        topic_title = ''
        topic_url = url
        page_num = 1

        # loggur.debug(f"GET_TEXT // -> Checking URL: {url}")

        # Try to connect to a given URL
        try:
            response = session.get(url, timeout=60, headers = headers)
        except Exception as e:
            loggur.error(f"GET_TEXT // Error downloading -> {url} : {str(e)}") 

        # Connection successful
        if response and response.ok:

            # Check if the file exceeds 15 MB
            if len(response.content)>15000000:
                loggur.warning("GET_TEXT // File too big")
                return text

            # Beautiful Soup to extract data from HTML
            try:
                soup = BeautifulSoup(response.content, "html.parser")
                for content_class in forum_content_class:
                    html_tag, attr_name_value = content_class.split(" >> ")
                    attr_name, attr_value = attr_name_value.split(" :: ")
                    comment_blocks = soup.find_all(html_tag, {attr_name: attr_value})
                    if comment_blocks:
                        break
                
                if not comment_blocks:
                    loggur.warning("GET_TEXT // Comment_Blocks EMPTY !!!!!!!!!")

            except Exception as e:
                loggur.error(f"GET_TEXT // ERROR BeautifulSoup (topic-text): {str(e)}")
            
            # Get text data from posts on page and add it to the string
            for comment in comment_blocks:
                text += comment.text.strip() + text_separator

            # Get Topic-Title as "forum_topic" (only from 1-st page)
            try:
                soup = BeautifulSoup(response.content, "html.parser")
                for content_class in forum_topic_title_class:
                    html_tag, attr_name_value = content_class.split(" >> ")
                    attr_name, attr_value = attr_name_value.split(" :: ")
                    topic_title = soup.find(html_tag, {attr_name: attr_value})
                    if topic_title:
                        break
                
                if topic_title:
                    topic_title = topic_title.text.strip()

                if not topic_title:
                    loggur.warning("GET_TEXT // Topic_Title EMPTY !!!!!!!!!")
                    topic_title = ""

            except Exception as e:
                loggur.error(f"GET_TEXT // ERROR BeautifulSoup (topic-title): {str(e)}")


            # Sleep for - we dont wanna burn servers
            time.sleep(time_sleep)

            #Process next pages
            try:            
                # Iterate through all of the pages in given topic/thread
                # while len(soup.find_all('li', {'class': 'ipsPagination_next'})) > 0:
                while ForumEnginesManager._get_next_page_link(url_now = url, soup = soup, pagination = pagination, logger_tool=loggur):
                    next_page_link = ForumEnginesManager._get_next_page_link(url_now = url, soup = soup, pagination = pagination, logger_tool=loggur, push_log=False)
                    url = urljoin(DATASET_URL, next_page_link) if next_page_link else False

                    if url and DATASET_URL in url:
                        page_num += 1
                        loggur.debug(f"GET_TEXT // Found new page for topic: {page_num} -> {url} | Topic: {topic_url}")

                        next_page_response = session.get(url, timeout=60, headers = headers)
                        soup = BeautifulSoup(next_page_response.content, "html.parser")

                        for content_class in forum_content_class:
                            html_tag, attr_name_value = content_class.split(" >> ")
                            attr_name, attr_value = attr_name_value.split(" :: ")
                            comment_blocks = soup.find_all(html_tag, {attr_name: attr_value})
                            if comment_blocks:
                                break
                
                        if not comment_blocks:
                            loggur.warning("GET_TEXT // Comment_Blocks EMPTY !!!!!!!!!")

                        for comment in comment_blocks:
                            text += comment.text.strip() + text_separator

                        time.sleep(time_sleep)
                    else:
                        loggur.debug(f"GET_TEXT // Topic URL is NOT in next_page_url: {next_page_link=}")
                        break

            # Handle next page error       
            except Exception as e:
                loggur.error(f"GET_TEXT // ERROR processing next page: {url} : {str(e)}")           

        # Connection not successful - reponse empty
        elif not response:    
            loggur.warning(f"GET_TEXT // Empty response -> {url} | Response: {response}")

        # Connection not successful - error
        elif not response.ok:
            loggur.warning(f"GET_TEXT // Error response -> {url} | Response: {response.status_code}")

        return text, topic_title

    @staticmethod
    def _process_item(url: str) -> tuple[str, dict]:
        """
        Extract from URL -> cleaning -> simple metadata.

        :param url (str): URL to get text data.

        :return text (str): Text data (stripped).
        :return meta (dict): Dict with simple metadata -> {'url' : url, 'length': len(txt_strip)} or {'url' : url, 'skip': 'error' / 'visited'}.
        """
        global all_visited_urls

        meta: dict = {'url' : url}
        txt: str = ''
        txt_strip: str = ''
        topic_title = ''

        # For DEBUG only
        # if psutil.LINUX == True:
        #     loggur.debug(f"PROCESS_ITEM // Processing URL: {url} | Proc ID: {psutil.Process().pid} | CPU Core: {psutil.Process().cpu_num()}")
        # else:
        #     loggur.debug(f"PROCESS_ITEM // Processing URL: {url} | Proc ID: {psutil.Process().pid}")

        if url not in all_visited_urls:
            try:
                txt, topic_title = Scraper._get_item_text(url)
                meta = {'url' : url, 'topic_title': topic_title, 'skip': 'error'}
                if txt:
                    txt_strip = txt.strip()
                    meta = {'url' : url, 'topic_title': topic_title, 'characters': len(txt_strip)}
            except Exception as e:
                loggur.error(f"PROCESS_ITEM // Error processing item -> {url} : {str(e)}")
                meta = {'url' : url, 'topic_title': topic_title, 'skip': 'error'}
        else:
            loggur.debug(f"PROCESS_ITEM // URL already visited -> skipping: {url}")
            meta = {'url' : url, 'topic_title': topic_title, 'skip': 'visited'}

        # For DEBUG only
        try:
            if psutil.LINUX == True:
                loggur.info(f"PROCESS_ITEM // Metadata: {meta} | Proc ID: {psutil.Process().pid} | CPU Core: {psutil.Process().cpu_num()}")
            else:
                loggur.info(f"PROCESS_ITEM // Metadata: {meta} | Proc ID: {psutil.Process().pid}")
        except Exception as e:
            loggur.warning("Problem with logging... Not printing METADATA for this topic...")
            loggur.debug(f"PROCESS_ITEM // Metadata: ... | Proc ID: {psutil.Process().pid}")

        return txt_strip, meta

    def _scrap_txt_mp(self, ar: Archive, topics_minus_visited: pandas.DataFrame, visited_topics: pandas.DataFrame) -> int:
        """
        Extract text data from URL using multiprocessing. 
        Init -> MP Pool -> Extract -> Save URLs and update Archive.

        :param ar (Archive): Archive class from library lm_dataformat.
        :param topics_minus_visited (pandas.DataFrame): DataFrame with URLs only for scraping.
        :param visited_topics (pandas.DataFrame): DataFrame with visited URLs.

        :return total_docs (int): Total number of added documents.

        ..note:
        - forum_topics -> columns = ['Topic_URLs', 'Topic_Titles']
        - visited_topics -> columns = ['Topic_URLs', 'Topic_Titles', 'Visited_flag', 'Skip_flag']
        """
        ctx = multiprocessing.get_context("spawn")
        ctx.freeze_support()
        # ctx_manager = ctx.Manager()
        # logger_q = ctx_manager.Queue(self.config.settings["PROCESSES"])

        total_docs: int = (visited_topics['Visited_flag'].sum() - visited_topics['Skip_flag'].sum())
        total_visited: int = visited_topics['Visited_flag'].sum()
        urls_left_number: int = topics_minus_visited.shape[0]

        if urls_left_number and urls_left_number != 0:
            self.logger_tool.info("*** Scraper will start in 5 sec ...")
            self.logger_print.info("* Scraper will start in 5 sec ...")
            time.sleep(5)

            # Temp values, Placeholders will be updated in postprocessing
            added: int = 0
            skipped: int = 0                 # Will be checked if visited -> in pool
            total: int = 0
            visited_urls_dataframe = pandas.DataFrame(columns = ['Topic_URLs', 'Topic_Titles', 'Visited_flag', 'Skip_flag'])
            time_loop_start = time.time()
            total_checkpoint = 0
            added_checkpoint = 0
            skipped_checkpoint = 0
            PROCESSES = self.config.settings["PROCESSES"]

            # Create and configure the process pool
            self.logger_tool.info("* Starting Multiprocessing Pool...")
            with ctx.Pool(initializer = self._initialize_worker,
                      initargs = [visited_topics['Topic_URLs'],
                                  self.config.headers,
                                  self.crawler.forum_engine.content_class,
                                  self.crawler.forum_engine.topic_title_class,
                                  self.text_separator,
                                  self.crawler.forum_engine.pagination,
                                  self.config.settings["TIME_SLEEP"],
                                  self.config.settings["DATASET_URL"],
                                  self.config.q_que,
                                  self.logger_tool.level],
                      processes = PROCESSES) as pool:

                time_loop_start = time.time()

                try:
                    # Issue tasks to the process pool for remaining URLs
                    for txt, meta in tqdm(pool.imap(func = self._process_item, 
                                                    iterable = topics_minus_visited['Topic_URLs'],
                                                    chunksize = 1),
                                                    total = urls_left_number, smoothing = 0.02,
                                                    disable = not self.config.print_to_console):
                        total += 1
                        flag_visited: int = 0
                        flag_skip: int = 0
                        visit_temp: dict = {}

                        if txt and len(txt) > self.config.settings["MIN_LEN_TXT"]:
                            total_docs += 1
                            if not meta["topic_title"]:
                                meta.update({"topic_title": "x"})
                            ar.add_data(txt, meta = meta)
                            added += 1
                            flag_visited = 1
                            flag_skip = 0
                            visit_temp = {'Topic_URLs': [meta.get('url')], 'Topic_Titles': meta.get('topic_title'), 'Visited_flag': [flag_visited], 'Skip_flag': [flag_skip]}
                            # self.logger_tool.info(f"SCRAPE // OK --- Processed: {total} | Added counter: {added} | Len(txt): {meta.get('length')} | Added URL: {meta.get('url')}")
                        else:
                            skipped += 1
                            flag_visited = 1
                            flag_skip = 1
                            if meta.get('skip') != 'visited':
                                visit_temp = {'Topic_URLs': [meta.get('url')], 'Topic_Titles': meta.get('topic_title'), 'Visited_flag': [flag_visited], 'Skip_flag': [flag_skip]}
                            # self.logger_tool.info(f"SCRAPE // Short or empty TXT --- Processed: {total} | Skipped counter: {skipped} | Why skipped: {meta.get('skip')} | Skipped URL: {meta.get('url')}")

                        if visit_temp:
                            # self.logger_print.info(f"VISIT_TEMP EXIST ---> {visit_temp}")
                            visited_urls_dataframe = pandas.concat([visited_urls_dataframe, pandas.DataFrame(visit_temp)], ignore_index=True)

                        if len(pool._pool) != PROCESSES:
                            self.logger_tool.error(f"*** ERROR *** Ups, something went wrong --> pool got: {len(pool._pool)} workers, should be {PROCESSES}")

                        # Save visited URLs to file
                        if total % self.config.settings["SAVE_STATE"] == 0 and added > 0:
                            self.logger_tool.info(f"SCRAPE // Scraping info --> Checked URLs: {total_visited + total} | Added docs: {total_docs}")
                            self.logger_tool.info(f"SCRAPE // This session --> Checked URLs: {total} | Added: {added}  | Skipped: {skipped}")
                            self.logger_tool.info(f"SCRAPE // Since last checkpoint --> Checked URLs: {total-total_checkpoint} | Added: {added-added_checkpoint}  | Skipped: {skipped-skipped_checkpoint}")
                            total_checkpoint = total
                            added_checkpoint = added
                            skipped_checkpoint = skipped

                            # self.logger_tool.info(f"SCRAPE // Saving visited URLs to file, visited: {visited_urls_dataframe.shape[0]}")
                            self.arch_manager.add_to_visited_file(visited_urls_dataframe)
                            visited_urls_dataframe = pandas.DataFrame(columns = ['Topic_URLs', 'Topic_Titles', 'Visited_flag', 'Skip_flag'])

                            ar.commit()
                            self.logger_tool.info(f"SCRAPE + SAVE // Commiting to Archive, total commited = {added}")
                            # self.logger_tool.info("SCRAPE // Commited to Archive - DONE | Visited URLs saved - DONE")

                            time_loop_end = time.time()
                            time_since_start = time_loop_end - time_loop_start + 1e-9
                            remains_iter = urls_left_number - total
                            time_per_iter = time_since_start / (total+1)
                            time_eta = remains_iter * time_per_iter
                            self.logger_tool.info(f"SCRAPE + TIMING // *** Time since start: {(time_since_start / 60):.2f} min | {(time_since_start / 3600):.2f} h | {(time_since_start / 86400):.2f} days")
                            self.logger_tool.info(f"SCRAPE + TIMING // *** Performance: {(total / time_since_start):.2f} ops/sec  |  Processes = {len(pool._pool)} / {PROCESSES}")
                            self.logger_tool.info(f"SCRAPE + TIMING // *** ETA: {(time_eta / 60):.2f} min | {(time_eta / 3600):.2f} hours | {(time_eta / 86400):.2f} days --> {(datetime.datetime.today() + datetime.timedelta(seconds=time_eta)).strftime('%Y-%m-%d %H:%M (%A)')}")
                            self.logger_tool.info("SCRAPE // ------------------------------------------------------------------- ")

                except Exception as e:
                    self.logger_tool.error(f"*** ERROR *** --> {str(e)}")

                self.logger_tool.info(f"SCRAPE // Scraping DONE! --> Checked URLs: {total_visited + total} | Added docs: {total_docs} ||| This session --> Checked URLs: {total} | Added: {added}  | Skipped: {skipped}")
                self.logger_print.info(f"* Scraping DONE! --> Checked URLs: {total_visited + total} | Added docs: {total_docs} ||| This session --> Checked URLs: {total} | Added: {added}  | Skipped: {skipped}")

                # Saving Archive and visited URLs
                ar.commit()
                self.arch_manager.add_to_visited_file(visited_urls_dataframe)
                self.logger_tool.info("SCRAPE // Saved URLs and Archive - DONE!")
                self.logger_print.info("* Saved URLs and Archive - DONE!")
        else:
            self.logger_tool.info("SCRAPE // Nothing to scrape...")
            self.logger_print.info("*** Nothing to scrape...")

        return total_docs
