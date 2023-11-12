"""

"""
import time
import logging
import requests
from requests.adapters import HTTPAdapter           # install requests
from urllib3.util.retry import Retry                # install urllib3
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

        self.forum_threads = {}
        self.threads_topics = {}
        self.urls_all = []

        self.headers = {
	        'User-Agent': 'Speakleash',
	        "Accept-Encoding": "gzip, deflate",
	        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	        "Connection": "keep-alive"
	    }

    def crawl_forum(self):
        """Crawls the forum, extracting threads and topics."""
        logging.info(f"Starting crawl on {self.forum_url}")

        try:
            session = requests.Session()
            retry = Retry(total=3, backoff_factor=3)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            # Fetch the main page of the forum and extract thread links
            self.forum_threads = self.get_forum_threads(self.forum_url, session)
            logging.info(f"Found threads: {len(self.forum_threads)}")

            # Iterate over each thread and extract topics
            for thread_url, thread_name in self.forum_threads.items():
                logging.info(f"Crawling thread: {thread_name} at {thread_url}")
                topics = self.get_thread_topics(thread_url, session)
                self.threads_topics.update(topics)
                time.sleep(self.time_sleep)

            # Here you could also handle pagination within each thread if necessary
            # After crawling, you might want to do something with the topics, like saving to a database
            # For demonstration, we're just printing the topics
            # for topic_name, topic_url in all_topics.items():
            #     logging.info(f"Topic found: {topic_name} |->| {topic_url}")

            return True
        except Exception as e:
            logging.error("ERROR --- ERROR --- ERROR --- ERROR --- ERROR")
            logging.error(f"Can't crawl topics -> {e}")
            logging.error("ERROR --- ERROR --- ERROR --- ERROR --- ERROR")
            return False

    def get_forum_threads(self, url: str, session: requests.Session):
        """Extracts threads from the forum page."""
        response = session.get(url, timeout=60, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        forum_threads = {}

        for thread_class in self.threads_class:
            threads = soup.select(thread_class)
            for thread_solo in threads:
                try:
                    forum_threads.update({urljoin(self.forum_url, thread_solo['href']) : thread_solo.get_text(strip=True)})
                except:
                    a_tag = thread_solo.find('a')
                    forum_threads.update({urljoin(self.forum_url, a_tag['href']) : a_tag.get_text(strip=True)})

            time.sleep(self.time_sleep)

            #TODO: Pagination for THREADS
            # while len(soup.find_all())

        return forum_threads

    def get_thread_topics(self, thread_url: str, session: requests.Session):
        """Extracts topics from a thread page."""
        response = session.get(thread_url, timeout=60, headers=self.headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        thread_topics = {}
        for topic_class in self.topics_class:
            topics = soup.select(topic_class)
            logging.debug(f"Found URLs = {len(topics)}")
            if len(topics) == 0:
                continue
            for topic_solo in topics:
                thread_topics.update({urljoin(self.forum_url, topic_solo['href']) : topic_solo.get_text(strip=True).replace('\n','')})
                self.urls_all.append(urljoin(self.forum_url, topic_solo['href']))
            print(len(self.urls_all))

            # Find the link to the next page
            while self.get_next_page_link(soup):
                next_page_link = self.get_next_page_link(soup)
                next_page_url = urljoin(self.forum_url, next_page_link) if next_page_link else False

                if next_page_url and self.forum_url in next_page_url:
                    logging.debug(f"*** Found new page with topics... URL: {next_page_url}")
                    response = requests.get(next_page_url, timeout=60, headers=self.headers)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    topics = soup.select(topic_class)
                    logging.debug(f"Found URLs = {len(topics)}")
                    if len(topics) == 0:
                        continue
                    for topic_solo in topics:
                        thread_topics.update({urljoin(self.forum_url, topic_solo['href']) : topic_solo.get_text(strip=True).replace('\n','')})
                        self.urls_all.append(urljoin(self.forum_url, topic_solo['href']))
                    print(len(self.urls_all))
                    time.sleep(self.time_sleep)
                else:
                    break
            time.sleep(self.time_sleep)
        return thread_topics

    def get_next_page_link(self, soup: BeautifulSoup):
        """Finds the link to the next page from pagination."""
        for pagination_class in self.topics_pagination:
            next_button = []
            html_tag = ['li', 'a']
            next_button = soup.find(html_tag, {'class': {pagination_class}})
            
            if next_button:
                logging.debug(f"Found button! ({len(next_button)}) | Button: {True if next_button else False}") 
                try:
                    next_page = next_button['href']
                except:
                    next_page = next_button.find('a')['href']
                logging.debug(f"Found next page -> {next_page}")
                return next_page
        logging.debug("Can't find more pages in this thread ---")
        return False


class InvisionCrawler():
    # Specific functionalities for Invision forums
    def __init__(self):
        self.threads_class = ["h4.ipsDataItem_title ipsType_break"]    # Used for threads and subforums
        self.topics_class = ["span.ipsType_break ipsContained"]          # Used for topics
        self.topics_pagination = ["ipsPagination_next"]             # Used for subforums and topics pagination
        self.content_class = ["ipsType_normal ipsType_richText ipsPadding_bottom ipsContained"]  # Used for content_class

class PhpBBCrawler():
    # Specific functionalities for phpBB forums
    def __init__(self):
        self.threads_class = ["a.forumtitle"]                       # Used for threads
        self.topics_class = ["a.topictitle"]                        # Used for topics
        self.topics_pagination = ["arrow next", "right-box right"]  # Different phpBB forums
        self.content_class = ["content_class"]                      # Used for content_class / messages

class IPBoardCrawler():
    # Specific functionalities for IPBoard forums
    def __init__(self):
        self.threads_class = ["td.col_c_forum"]                     # Used for threads
        self.topics_class = ["a.topic_title"]                       # Used for topics
        self.topics_pagination = ["next"]                           # Used for subforums and topics pagination
        self.content_class = ["post entry-content_class"]           # Used for content_class / messages

class XenForoCrawler():
    # Specific functionalities for XenForo forums
    def __init__(self):
        self.threads_class = ["h3.node-title"]                        # Used for threads
        self.topics_class = ["div.structItem-title"]                  # Used for topics
        self.topics_pagination = ["pageNav-jump pageNav-jump--next"]  # Used for subforums and topics pagination
        self.content_class = ["message-body js-selectToQuote"]        # Used for content_class / messages
