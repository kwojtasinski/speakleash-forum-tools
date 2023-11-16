"""
Forum Engines Manager Module

This module offers a comprehensive solution for crawling various types of forum software. 
It includes the ForumEnginesManager class, which orchestrates the process of extracting threads and topics from forum pages. 
The manager utilizes specific crawler classes tailored to different forum engines, ensuring efficient and targeted data extraction.

Classes:
- ForumEnginesManager: Manages the crawling process across different forum engine types, 
handling the extraction of threads and topics from forum pages. It respects site-specific scraping policies 
as defined in the 'robots.txt' file and supports customizable filtering for thread and topic URLs.

Dependencies:
- logging: Utilized for logging various stages and activities during the crawling process.
- requests: Used for making HTTP requests to fetch forum pages.
- bs4 (BeautifulSoup): Aids in parsing HTML content of forum pages for data extraction.
- urllib.parse (urljoin): Assists in constructing absolute URLs from relative paths.
"""
import time
import logging
import requests
import dataclasses
import urllib3
from urllib.parse import urljoin
from typing import List

from bs4 import BeautifulSoup
from speakleash_forum_tools.config import ConfigManager
from speakleash_forum_tools.utils import create_session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)     # Supress warning about 'InsecureRequest' (session.get(..., verify = False))

class ForumEnginesManager:
    """
    Manages the crawling process for various forum engine types. 
    It provides methods to extract threads and topics from forum pages and handle pagination.
    The class orchestrates the use of specific crawler classes tailored to each identified forum engine type, 
    ensuring that the crawling process is optimized for the nuances of each forum software.

    :param config_manager (ConfigManager): Configuration class, containing settings with keys like 'FORUM_ENGINE', 'DATASET_URL', etc.

    .. note:: The `urllib.robotparser` is used to ensure compliance with the forum's scraping policies as declared in its 'robots.txt'.
    """
    def __init__(self, config_manager: ConfigManager):
        self.engine_type = config_manager.settings['FORUM_ENGINE']
        self.forum_url = config_manager.settings['DATASET_URL']
        self.dataset_name = config_manager.settings['DATASET_NAME']
        self.time_sleep = config_manager.settings['TIME_SLEEP']
        logging.info(f"Forum Engines Manager -> Forum URL = {self.forum_url} | Engine Type = {self.engine_type} | Sleep Time = {self.time_sleep}")

        self.robot_parser = config_manager.robot_parser

        try:
            if self.engine_type == 'invision':
                engine_type = InvisionCrawler()
            elif self.engine_type == 'phpbb':
                engine_type = PhpBBCrawler()
            elif self.engine_type == 'ipboard':
                engine_type = IPBoardCrawler()
            elif self.engine_type == 'xenforo':
                engine_type = XenForoCrawler()
            else:
                raise ValueError("Unsupported forum engine type")
        except Exception as e:
            logging.error(f"{e}")

        self.threads_class = engine_type.threads_class
        self.threads_whitelist = engine_type.threads_whitelist
        self.threads_blacklist = engine_type.threads_blacklist
        self.topics_class = engine_type.topics_class
        self.topics_whitelist = engine_type.topics_whitelist
        self.topics_blacklist = engine_type.topics_blacklist
        self.pagination = engine_type.pagination
        self.content_class = engine_type.content_class

        if config_manager.settings['THREADS_CLASS']:
            self.threads_class.extend(config_manager.settings['THREADS_CLASS'])
        if config_manager.settings['THREADS_WHITELIST']:
            self.threads_whitelist.extend(config_manager.settings['THREADS_WHITELIST'])
        if config_manager.settings['THREADS_BLACKLIST']:
            self.threads_blacklist.extend(config_manager.settings['THREADS_BLACKLIST'])
        if config_manager.settings['TOPICS_CLASS']:
            self.topics_class.extend(config_manager.settings['TOPICS_CLASS'])
        if config_manager.settings['TOPICS_WHITELIST']:
            self.topics_whitelist.extend(config_manager.settings['TOPICS_WHITELIST'])
        if config_manager.settings['TOPICS_BLACKLIST']:
            self.topics_blacklist.extend(config_manager.settings['TOPICS_BLACKLIST'])
        if config_manager.settings['PAGINATION']:
            self.pagination.extend(config_manager.settings['PAGINATION'])
        if config_manager.settings['CONTENT_CLASS']:
            self.content_class.extend(config_manager.settings['CONTENT_CLASS'])

        self.forum_threads = []
        self.threads_topics = {}
        self.urls_all = []

        self.headers = config_manager.headers

    def crawl_forum(self):
        """
        Initiates the crawling process over the configured forum URL. It leverages the specified forum engine crawler
        to navigate through threads and topics, adhering to the rules specified in `robots.txt`.

        :return (bool): A boolean indicating the success or failure of the crawling process.
        """
        logging.info(f"Starting crawl on {self.forum_url}")

        try:
            # Fetch the main page of the forum and extract thread links
            session = create_session()
            self.forum_threads.append(self.get_forum_threads(self.forum_url, session = session))
            
            # Iterate over each thread and extract topics
            for x in self.forum_threads:
                for thread_url, thread_name in x.items():
                    logging.info(f"Crawling thread: || {thread_name} || at {thread_url}")
                    topics = self.get_thread_topics(thread_url, session = session)
                    self.threads_topics.update(topics)
                    logging.info(f"-> All Topics found: {len(self.threads_topics)}")
                    time.sleep(self.time_sleep)

            self.forum_threads = {key: value for d in self.forum_threads for key, value in d.items()}

            logging.info(f"Found: Threads = {len(self.forum_threads)}")
            logging.info(f"Found: Topics = {len(self.threads_topics)}")

            if len(self.forum_threads) and len(self.threads_topics):
                return False

            return True
        except Exception as e:
            logging.error("ERROR --- ERROR --- ERROR --- ERROR --- ERROR")
            logging.error(f"Can't crawl topics -> {e}")
            logging.error("ERROR --- ERROR --- ERROR --- ERROR --- ERROR")
            return False

    def get_forum_threads(self, url_now: str, session: requests.Session):
        """
        Retrieves all the threads listed on a given forum page by utilizing the CSS selectors specified for the forum engine.

        :param url_now (str): The URL of the forum page from which to extract the threads.
        :param session (requests.Session): Session with http/https adapters.
        :return (dict): A dictionary mapping thread URLs to their respective thread titles.
        """
        response = session.get(url_now, timeout=60, headers=self.headers, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')

        forum_threads = self._get_forum_threads(soup)
        return forum_threads

    def _get_forum_threads(self, soup: BeautifulSoup):
        """
        Extracts valid threads from the forum page.

        :param soup (BeautifulSoup): BeautifulSoup object with currently searched URL.
        :return (dict): Dict with threads (forums) found on website.
        """
        forum_threads = {}

        for thread_class in self.threads_class:
            html_tag, th_class = thread_class.split(" >> ")
            threads = soup.find_all(html_tag, {'class': th_class})

            threads_found = self._crawler_filter(to_find = "THREAD", to_search = threads, whitelist = self.threads_whitelist, blacklist = self.threads_blacklist, robotparser = self.robot_parser, forum_url = self.forum_url)
            forum_threads.update(threads_found)

            time.sleep(self.time_sleep)
        
            #TODO: Pagination for THREADS
            # while len(soup.find_all())

        logging.info(f"-> Found threads: {len(forum_threads)}")
        return forum_threads


    def get_thread_topics(self, url_now: str, session: requests.Session):
        """
        Retrieves all the topics listed on a given thread (forum) page by utilizing the CSS selectors specified for the forum engine.

        :param url_now (str): The URL of the thread (forum) page from which to extract the topics.
        :param session (requests.Session): Session with http/https adapters.
        :return (dict): A dictionary mapping topic URLs to their respective topic titles.
        """
        thread_topics = {}
        
        response = session.get(url_now, timeout=60, headers=self.headers, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        thread_topics = self._get_thread_topics(soup = soup)
        logging.info(f"--> Topics found in thread: {len(thread_topics)}")

        # Find the link to the next page
        while self.get_next_page_link(url_now, soup):
            next_page_link = self.get_next_page_link(url_now, soup)
            url_now = urljoin(self.forum_url, next_page_link) if next_page_link else False
            
            if url_now and self.forum_url in url_now:
                logging.info(f"*** Found new page with topics... URL: {url_now}")
                response = session.get(url_now, timeout=60, headers=self.headers, verify=False)
                soup = BeautifulSoup(response.content, 'html.parser')

                thread_topics.update(self._get_thread_topics(soup = soup))
                logging.info(f"--> Topics found in thread: {len(thread_topics)}")
            else:
                break
        
        return thread_topics


    def _get_thread_topics(self, soup: BeautifulSoup):
        """
        Extracts valid topics from the forum page.

        :param soup (BeautifulSoup): BeautifulSoup object with currently searched URL.
        :return (dict): Dict with topics found in thread (forum)
        """
        thread_topics = {}

        for topic_class in self.topics_class:
            # topics = soup.select(topic_class)
            html_tag, tp_class = topic_class.split(" >> ")
            topics = soup.find_all(html_tag, {'class': tp_class})
            logging.debug(f"Found URLs = {len(topics)}")

            if len(topics) == 0:
                forum_threads = self._get_forum_threads(soup=soup)
                self.forum_threads.append(forum_threads)
                logging.info(f"Added new threads (while searching for topics) = {len(forum_threads)}")
                continue
            
            topics_found = self._crawler_filter(to_find = "TOPIC", to_search = topics, whitelist = self.topics_whitelist, blacklist = self.topics_blacklist, robotparser = self.robot_parser, forum_url = self.forum_url)
            thread_topics.update(topics_found)
        
        time.sleep(self.time_sleep)
        logging.info(f"Found topics: {len(thread_topics)}")
        return thread_topics


    def get_next_page_link(self, url_now: str, soup: BeautifulSoup):
        """
        Finds the link to the next page using pagination.

        :param url_now (str): URL of website which crawler is now checking.
        :param soup (BeautifulSoup): BeautifulSoup object with currently searched URL.
        """
        for pagination_class in self.pagination:
            next_button = []
            html_tag = ['li', 'a', 'div']
            next_button = soup.find(html_tag, {'class': {pagination_class}})

            if not next_button:
                try:
                    pag_type, pag_class = pagination_class.split("::")
                    next_button = soup.find_all(html_tag, {pag_type:pag_class})[0]
                    if (next_button):
                        logging.debug("Button to next page - FOUND -> wierd spot")
                    else:
                        logging.debug("Button to next page - NOT FOUND")
                        continue
                except Exception as e:
                    if e:
                        logging.debug(f"ERROR: Error while searching for pagination -> {e}")
                    logging.debug("Button to next page - NOT FOUND")
                    continue
            
            if next_button:
                logging.debug(f"Found button! ({len(next_button)}) | Button: {True if next_button else False}") 
                try:
                    next_page = next_button['href']
                except:
                    next_page = next_button.find('a')['href']
                
                if next_page == url_now:
                    continue

                logging.debug(f"Found next page with topics -> {next_page}")
                return next_page
        logging.debug("Can't find more pages in this thread ---")
        return False
    
    @staticmethod
    def _crawler_filter(to_find: str, to_search, whitelist: List[str], blacklist: List[str], robotparser, forum_url: str) -> dict:
        """
        Filtering found URLs and check them with robots.txt parser.

        :param to_find (str): Simple string for debug only (e.g. "THREADS" or "TOPICS").
        :param to_search (BeautifulSoup.find_all()): ResultSet from BeautifulSoup.find_all() function.
        :param whitelist (list[str]): Strings which have to be inside URL if we wanna make sure it is valid URL.
        :param blacklist (list[str]): Strings for blocking some URLs.
        :param robotparser (urllib.robotparser): Parser for 'robots.txt' - check if robots.txt doesn't block topics / threads URLs.
        :param forum_url (str): Forum main website URL - for checking if crawler will take only forum URLs.
        """
        to_return_dict = {}

        for tag_solo in to_search:
            try:
                try:
                    if tag_solo['href']:
                        a_tags = [tag_solo]
                except:
                    a_tags = tag_solo.find_all('a')

                for a_tag in a_tags:
                    # logging.debug(f"{to_find} -> {a_tag['href']}")
                    if whitelist:
                        if any(y in a_tag['href'] for y in whitelist):
                            if blacklist:
                                if any(y in a_tag['href'] for y in blacklist):
                                    logging.debug(f"{to_find} OUT <- {a_tag['href']}")
                                    continue
                            if robotparser.can_fetch("*", a_tag['href']):
                                logging.debug(f"{to_find} GOOD -> {a_tag['href']}")
                                to_return_dict.update({urljoin(forum_url, a_tag['href']) : a_tag.get_text(strip=True)})
                            else:
                                logging.debug(f"{to_find} OUT <- {a_tag['href']}")
                            continue
                        else:
                            logging.debug(f"{to_find} OUT <- {a_tag['href']}")
                            continue
                    if blacklist:
                        if any(y in a_tag['href'] for y in blacklist):
                            logging.debug(f"{to_find} OUT <- {a_tag['href']}")
                            continue
                    
                    if not whitelist or not blacklist:
                        if robotparser.can_fetch("*", a_tag['href']):
                            logging.debug(f"{to_find} GOOD (+) -> {a_tag['href']}")
                            to_return_dict.update({urljoin(forum_url, a_tag['href']) : a_tag.get_text(strip=True)})
                        else:
                            logging.debug(f"{to_find} OUT (+) <- {a_tag['href']}")
            except Exception as e:
                logging.error(f"Error while crawl for {to_find}s -> {e}")

        return to_return_dict



class InvisionCrawler():
    """
    Specific functionalities for Invision forums
    """
    def __init__(self):
        self.threads_class: List[str] = ["div >> ipsDataItem_main"]    # Used for threads and subforums
        self.topics_class: List[str] = ["div >> ipsDataItem_main"]          # Used for topics
        self.threads_whitelist: List[str] = ["forum"]
        self.threads_blacklist: List[str] = ["topic"]
        self.topics_whitelist: List[str] = ["topic"]
        self.topics_blacklist: List[str] = ["page", "#comments"]
        self.pagination: List[str] = ["ipsPagination_next"]             # Used for subforums and topics pagination
        self.content_class: List[str] = ["ipsType_normal ipsType_richText ipsPadding_bottom ipsContained"]  # Used for content_class

class PhpBBCrawler():
    """
    Specific functionalities for phpBB forums
    """
    def __init__(self):
        self.threads_class: List[str] = ["a >> forumtitle", "a >> forumlink"]                       # Used for threads
        self.topics_class: List[str] = ["a >> topictitle"]                        # Used for topics
        self.threads_whitelist: List[str] = []
        self.threads_blacklist: List[str] = []
        self.topics_whitelist: List[str] = []
        self.topics_blacklist: List[str] = []
        self.pagination: List[str] = ["arrow next", "right-box right", "title::Dalej", "pag-img"]  # Different phpBB forums
        self.content_class: List[str] = ["content_class"]                      # Used for content_class / messages

class IPBoardCrawler():
    """
    Specific functionalities for IPBoard forums
    """
    def __init__(self):
        self.threads_class: List[str] = ["td >> col_c_forum"]                     # Used for threads
        self.topics_class: List[str] = ["a >> topic_title"]                       # Used for topics
        self.threads_whitelist: List[str] = []
        self.threads_blacklist: List[str] = []
        self.topics_whitelist: List[str] = []
        self.topics_blacklist: List[str] = []
        self.pagination: List[str] = ["next"]                           # Used for subforums and topics pagination
        self.content_class: List[str] = ["post entry-content_class"]           # Used for content_class / messages

class XenForoCrawler():
    """
    Specific functionalities for XenForo forums
    """
    def __init__(self):
        self.threads_class: List[str] = ["h3 >> node-title"]                        # Used for threads
        self.topics_class: List[str] = ["div >> structItem-title"]                  # Used for topics
        self.threads_whitelist: List[str] = []
        self.threads_blacklist: List[str] = ["prefix_id"]
        self.topics_whitelist: List[str] = ["threads"]
        self.topics_blacklist: List[str] = ["preview"]
        self.pagination: List[str] = ["pageNav-jump pageNav-jump--next"]  # Used for subforums and topics pagination
        self.content_class: List[str] = ["message-body js-selectToQuote"]        # Used for content_class / messages
