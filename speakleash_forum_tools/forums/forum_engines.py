"""

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

        self.forum_threads = []
        self.threads_topics = {}
        self.urls_all = []

        self.headers = config_manager.headers

    def crawl_forum(self):
        """Crawls the forum, extracting threads and topics."""
        logging.info(f"Starting crawl on {self.forum_url}")

        try:
            # Fetch the main page of the forum and extract thread links
            self.forum_threads.append(self.get_forum_threads(self.forum_url, create_session()))
            
            # Iterate over each thread and extract topics
            for x in self.forum_threads:
                for thread_url, thread_name in x.items():
                    logging.info(f"Crawling thread: || {thread_name} || at {thread_url}")
                    topics = self.get_thread_topics(thread_url, create_session())
                    self.threads_topics.update(topics)
                    logging.info(f"-> All Topics found: {len(self.threads_topics)}")
                    time.sleep(self.time_sleep)

            # Here you could also handle pagination within each thread if necessary
            # After crawling, you might want to do something with the topics, like saving to a database
            # For demonstration, we're just intinting the topics
            # for topic_name, topic_url in all_topics.items():
            #     logging.info(f"Topic found: {topic_name} |->| {topic_url}")

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
        Extracts threads from the forum page - wrapper.
        """
        response = session.get(url_now, timeout=60, headers=self.headers, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')

        forum_threads = self._get_forum_threads(soup)
        return forum_threads

    def _get_forum_threads(self, soup: BeautifulSoup):
        """
        Extracts threads from the forum page.
        """
        forum_threads = {}

        for thread_class in self.threads_class:
            html_tag, th_class = thread_class.split(" >> ")
            threads = soup.find_all(html_tag, {'class': th_class})

            threads_found = self._crawler_filter(to_find = "THREAD", to_search = threads, whitelist = self.threads_whitelist, blacklist = self.threads_blacklist, robotparser = self.robot_parser, forum_url = self.forum_url)
            forum_threads.update(threads_found)

#             for thread_solo in threads:
#                 try:
#                     try:
#                         if thread_solo['href']:
#                             a_tags = [thread_solo]
#                     except:
#                             a_tags = thread_solo.find_all('a')
# 
#                     for a_tag in a_tags:
#                         # logging.debug(f"THREAD -> {a_tag['href']}")
#                         if self.threads_whitelist:
#                             if any(y in a_tag['href'] for y in self.threads_whitelist):
#                                 if self.threads_blacklist:
#                                     if any(y in a_tag['href'] for y in self.threads_blacklist):
#                                         logging.debug(f"THREAD OUT <- {a_tag['href']}")
#                                         continue
#                                 if self.robot_parser.can_fetch("*", a_tag['href']):
#                                     logging.debug(f"THREAD GOOD -> {a_tag['href']}")
#                                     forum_threads.update({urljoin(self.forum_url, a_tag['href']) : a_tag.get_text(strip=True)})
#                                 else:
#                                     logging.debug(f"THREAD OUT <- {a_tag['href']}")
#                                 continue
#                             else:
#                                 logging.debug(f"THREAD OUT <- {a_tag['href']}")
# 
#                         if self.threads_blacklist:
#                             if any(y in a_tag['href'] for y in self.threads_blacklist):
#                                 logging.debug(f"THREAD OUT <- {a_tag['href']}")
#                                 continue
#                         
#                         if not self.threads_whitelist or not self.threads_blacklist:
#                             if self.robot_parser.can_fetch("*", a_tag['href']):
#                                 logging.debug(f"THREAD GOOD (+) -> {a_tag['href']}")
#                                 forum_threads.update({urljoin(self.forum_url, a_tag['href']) : a_tag.get_text(strip=True)})
#                             else:
#                                 logging.debug(f"THREAD OUT (+) <- {a_tag['href']}")
#                 except Exception as e:
#                     logging.error(f"Error while crawl for THREADS -> {e}")

            time.sleep(self.time_sleep)

            #TODO: Pagination for THREADS
            # while len(soup.find_all())

        logging.info(f"-> Found threads: {len(forum_threads)}")
        return forum_threads


    def get_thread_topics(self, url_now: str, session: requests.Session):
        """Extracts topics from a thread page."""
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
            
            #_crawler_filter(to_find, to_search, whitelist, blacklist, robotparser, forum_url)
            topics_found = self._crawler_filter(to_find = "TOPIC", to_search = topics, whitelist = self.topics_whitelist, blacklist = self.topics_blacklist, robotparser = self.robot_parser, forum_url = self.forum_url)
            thread_topics.update(topics_found)

#             for topic_solo in topics:
#                 try:
#                     try:
#                         if topic_solo['href']:
#                             a_tags = [topic_solo]
#                     except:
#                             a_tags = topic_solo.find_all('a')
# 
#                     for a_tag in a_tags:
#                         # logging.debug(f"TOPIC -> {a_tag['href']}")
#                         if self.topics_whitelist:
#                             if any(y in a_tag['href'] for y in self.topics_whitelist):
#                                 if self.topics_blacklist:
#                                     if any(y in a_tag['href'] for y in self.topics_blacklist):
#                                         logging.debug(f"TOPIC OUT <- {a_tag['href']}")
#                                         continue
#                                 if self.robot_parser.can_fetch("*", a_tag['href']):
#                                     logging.debug(f"TOPIC GOOD -> {a_tag['href']}")
#                                     thread_topics.update({urljoin(self.forum_url, a_tag['href']) : a_tag.get_text(strip=True)})
#                                 else:
#                                     logging.debug(f"TOPIC OUT <- {a_tag['href']}")
#                                 continue
#                             else:
#                                 logging.debug(f"TOPIC OUT <- {a_tag['href']}")
# 
#                         if self.topics_blacklist:
#                             if any(y in a_tag['href'] for y in self.topics_blacklist):
#                                 logging.debug(f"TOPIC OUT <- {a_tag['href']}")
#                                 continue
# 
#                         if not self.topics_whitelist and not self.topics_blacklist:
#                             if self.robot_parser.can_fetch("*", a_tag['href']):
#                                 logging.debug(f"TOPIC GOOD (+) -> {a_tag['href']}")
#                                 thread_topics.update({urljoin(self.forum_url, a_tag['href']) : a_tag.get_text(strip=True)})
#                             else:
#                                 logging.debug(f"TOPIC OUT (+) <- {a_tag['href']}")
#                                 continue
#                 except Exception as e:
#                     logging.error(f"Error while crawl for TOPICS -> {e}")
        
        time.sleep(self.time_sleep)
        logging.info(f"Found topics: {len(thread_topics)}")
        return thread_topics


    def get_next_page_link(self, url_now: str, soup: BeautifulSoup):
        """Finds the link to the next page from pagination."""
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
    def _crawler_filter(to_find, to_search, whitelist, blacklist, robotparser, forum_url) -> dict:
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
    def __init__(self):
    # Specific functionalities for Invision forums
        self.threads_class: List[str] = ["div >> ipsDataItem_main"]    # Used for threads and subforums
        self.topics_class: List[str] = ["div >> ipsDataItem_main"]          # Used for topics
        self.threads_whitelist: List[str] = ["forum"]
        self.threads_blacklist: List[str] = ["topic"]
        self.topics_whitelist: List[str] = ["topic"]
        self.topics_blacklist: List[str] = ["page", "#comments"]
        self.pagination: List[str] = ["ipsPagination_next"]             # Used for subforums and topics pagination
        self.content_class: List[str] = ["ipsType_normal ipsType_richText ipsPadding_bottom ipsContained"]  # Used for content_class

class PhpBBCrawler():
    # Specific functionalities for phpBB forums
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
    def __init__(self):
    # Specific functionalities for IPBoard forums
        self.threads_class: List[str] = ["td >> col_c_forum"]                     # Used for threads
        self.topics_class: List[str] = ["a >> topic_title"]                       # Used for topics
        self.threads_whitelist: List[str] = []
        self.threads_blacklist: List[str] = []
        self.topics_whitelist: List[str] = []
        self.topics_blacklist: List[str] = []
        self.pagination: List[str] = ["next"]                           # Used for subforums and topics pagination
        self.content_class: List[str] = ["post entry-content_class"]           # Used for content_class / messages

class XenForoCrawler():
    def __init__(self):
    # Specific functionalities for XenForo forums
        self.threads_class: List[str] = ["h3 >> node-title"]                        # Used for threads
        self.topics_class: List[str] = ["div >> structItem-title"]                  # Used for topics
        self.threads_whitelist: List[str] = []
        self.threads_blacklist: List[str] = ["prefix_id"]
        self.topics_whitelist: List[str] = ["threads"]
        self.topics_blacklist: List[str] = ["preview"]
        self.pagination: List[str] = ["pageNav-jump pageNav-jump--next"]  # Used for subforums and topics pagination
        self.content_class: List[str] = ["message-body js-selectToQuote"]        # Used for content_class / messages
