"""

"""
import time
import logging
import requests
from urllib.parse import urljoin

from bs4 import BeautifulSoup

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s', level=logging.DEBUG)

class ForumEnginesManager:
    """
    
    """
    def __init__(self, settings: dict):
        self.engine_type = settings['FORUM_ENGINE']
        self.forum_url = settings['DATASET_URL']
        self.time_sleep = settings['TIME_SLEEP']
        logging.info(f"Forum Engines Manager -> Forum URL = {self.forum_url} | Engine Type = {self.engine_type} | Sleep Time = {self.time_sleep}")

        self.threads_class = ""
        self.topics_class = ""
        self.topics_pagination = ""
        self.content_class = ""

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
        self.topics_class = engine_type.topics_class
        self.topics_pagination = engine_type.topics_pagination
        self.content_class = engine_type.content_class

        self.forum_threads = []
        self.threads_topics = []
        self.urls_all = []

    def crawl_forum(self):
        """Crawls the forum, extracting threads and topics."""
        logging.info(f"Starting crawl on {self.forum_url}")

        try:
            # Fetch the main page of the forum and extract thread links
            forum_threads = self.get_forum_threads(self.forum_url)
            logging.info(f"Found threads: {len(forum_threads)}")

            # Iterate over each thread and extract topics
            all_topics = {}
            for thread_name, thread_url in forum_threads.items():
                logging.info(f"Crawling thread: {thread_name} at {thread_url}")
                topics = self.get_thread_topics(thread_url)
                all_topics.update(topics)
                time.sleep(self.time_sleep)

            # Here you could also handle pagination within each thread if necessary
            # After crawling, you might want to do something with the topics, like saving to a database
            # For demonstration, we're just printing the topics
            # for topic_name, topic_url in all_topics.items():
            #     logging.info(f"Topic found: {topic_name} |->| {topic_url}")

            self.urls_all = all_topics
            return True
        except Exception as e:
            logging.error(f"Can't crawl topics -> {e}")
            return False

    def get_forum_threads(self, url):
        """Extracts threads from the forum page."""
        response = requests.get(url, timeout=60)
        soup = BeautifulSoup(response.content, 'html.parser')
        forum_threads = {}
        for thread_class in self.threads_class:
            threads = soup.select(f'a.{thread_class}')
            for thread_solo in threads:
                forum_threads.update({thread_solo.get_text(strip=True): urljoin(self.forum_url, thread_solo['href'])})
            time.sleep(self.time_sleep)

            #TODO: Pagination for THREADS
            # while len(soup.find_all())

        self.forum_threads = forum_threads
        return forum_threads

    def get_thread_topics(self, thread_url):
        """Extracts topics from a thread page."""
        response = requests.get(thread_url, timeout=60)
        soup = BeautifulSoup(response.content, 'html.parser')
        thread_topics = {}
        for topic_class in self.topics_class:
            topics = soup.select(f'a.{topic_class}')
            if len(topics) == 0:
                continue
            for topic_solo in topics:
                thread_topics.update({topic_solo.get_text(strip=True).replace('\n',''): urljoin(self.forum_url, topic_solo['href'])})
            time.sleep(self.time_sleep)

            #TODO: Pagination for TOPICS
        
        self.threads_topics = thread_topics
        return thread_topics


class InvisionCrawler():
    # Specific functionalities for Invision forums
    def __init__(self):
        self.threads_class = ["ipsDataItem_title ipsType_break"]    # Used for threads and subforums
        self.topics_class = ["ipsType_break ipsContained"]          # Used for topics
        self.topics_pagination = ["ipsPagination_next"]             # Used for subforums and topics pagination
        self.content_class = ["ipsType_normal ipsType_richText ipsPadding_bottom ipsContained"]  # Used for content_class

class PhpBBCrawler():
    # Specific functionalities for phpBB forums
    def __init__(self):
        self.threads_class = ["forumtitle"]                         # Used for threads
        self.topics_class = ["topictitle"]                          # Used for topics
        self.topics_pagination = ["arrow next", "right-box right"]  # Different phpBB forums
        self.content_class = ["content_class"]                                  # Used for content_class / messages

class IPBoardCrawler():
    # Specific functionalities for IPBoard forums
    def __init__(self):
        self.threads_class = ["col_c_forum"]                        # Used for threads
        self.topics_class = ["topic_title"]                         # Used for topics
        self.topics_pagination = ["next"]                           # Used for subforums and topics pagination
        self.content_class = ["post entry-content_class"]                       # Used for content_class / messages

class XenForoCrawler():
    # Specific functionalities for XenForo forums
    def __init__(self):
        self.threads_class = ["node-title"]                         # Used for threads
        self.topics_class = ["structItem-title"]                    # Used for topics
        self.topics_pagination = ["pageNav-jump pageNav-jump--next"]  # Used for subforums and topics pagination
        self.content_class = ["message-body js-selectToQuote"]            # Used for content_class / messages
