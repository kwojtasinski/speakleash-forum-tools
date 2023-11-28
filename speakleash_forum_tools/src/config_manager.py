"""
Config Manager Module

This module defines the `ConfigManager` class, which serves as a centralized manager for configuring and managing settings for a forum crawler and scraper. 
The `ConfigManager` class offers a comprehensive setup for various parameters required for crawling and scraping forums. 
It supports various forum engines, adheres to `robots.txt` policies, and handles command-line arguments for flexibility and customization.

Classes:
- ConfigManager: Manages the configuration for forum crawling and scraping. 
It initializes with defaults or user-provided settings and handles `robots.txt` parsing.

Functions:
- _initialize_settings: Initializes the configuration settings for the crawler.
- _parse_arguments: Parses command-line arguments if enabled.
- _check_robots_txt: Checks and parses the forum's `robots.txt` file.
- _check_instance: Validates the instance types of provided arguments.
- _print_settings: Prints the current configuration settings.
- init_robotstxt: Initializes a dummy `robots.txt` parser.

Usage:
The `ConfigManager` class is instantiated with various settings like forum URL, engine type, crawling and scraping settings. 
It can be used standalone or in combination with other modules in the `speakleash-forum-tools` package for efficient forum data scraping.

Dependencies:
- logging: Used for logging information, warnings, and errors.
- argparse: Parses command-line arguments if enabled.
- urllib: Provides functionality for URL parsing and handling `robots.txt`.
- speakleash_forum_tools.src.utils: Optional utility functions, e.g., for checking library updates.
"""
import os
import time
import logging
import argparse
import datetime
import urllib.request
import urllib.robotparser
from urllib.parse import urlparse, urljoin
from typing import Optional, Tuple, List

from speakleash_forum_tools.src.utils import check_for_library_updates


class ConfigManager:
    """
    A configuration manager for setting up and managing settings for a forum crawler.
    """

    def __init__(self, dataset_url: str = "https://forum.szajbajk.pl", dataset_category: str = 'Forum', forum_engine: str = 'invision',
                 dataset_name: str = "", arg_parser: bool = False, check_robots: bool = True, force_crawl: bool = False,
                 processes: int = 2, time_sleep: float = 0.5, save_state: int = 100, min_len_txt: int = 20, sitemaps: str = "", log_lvl = logging.INFO,
                 threads_class: List[str] = [], threads_whitelist: List[str] = [], threads_blacklist: List[str] = [], topic_class: List[str] = [],
                 topic_whitelist: List[str] = [], topic_blacklist: List[str] = [], pagination: List[str] = [], topic_title_class: List[str] = [], content_class: List[str] = []):
        """
        Initializes the ConfigManager with defaults or overridden settings based on provided arguments.

        Params:
            - dataset_url (str): The base URL of the dataset/forum to be crawled.
            - dataset_category (str): The category of the dataset/forum.
            - forum_engine (str): The forum engine used on website: ['invision', 'phpbb', 'ipboard', 'xenforo', 'other']
            - arg_parser (bool): Flag to determine if command-line arguments should be parsed.
            - check_robots (bool): Flag to determine if robots.txt should be checked.
            - force_crawl (bool): Flag to force crawling even if disallowed by robots.txt.
            - processes (int): Number of processes to use for multiprocessing.
            - time_sleep (float): Time in seconds to sleep between requests.
            - save_state (int): Interval at which to save crawling state.
            - min_len_txt (int): Minimum length of text to consider as valid data.
            - threads_class (List[str]): HTML selectors used for identifying thread links in the forum. 
                "<anchor_tag> >> <attribute_name> :: <attribute_value>", e.g. ["a >> class :: forumtitle"] (for phpBB engine).
            - topics_class (List[str]): HTML selectors used for identifying topic links within a thread. 
                "<anchor_tag> >> <attribute_name> :: <attribute_value>", e.g. ["a >> class :: topictitle"] (for phpBB engine)
            - threads_whitelist (List[str]): List of substrings; only threads whose URLs contain any of these substrings will be processed. Example for Invision forum: ["forum"].
            - threads_blacklist (List[str]): List of substrings; threads whose URLs contain any of these substrings will be ignored. Example for Invision forum: ["topic"].
            - topics_whitelist (List[str]): List of substrings; only topics whose URLs contain any of these substrings will be processed. Example for Invision forum: ["topic"].
            - topics_blacklist (List[str]): List of substrings; topics whose URLs contain any of these substrings will be ignored. Example for Invision forum: ["page", "#comments"].
            - pagination (List[str]): HTML selectors used for identifying pagination elements within threads or topics.
                "<attribute_value>" (when attribute_name is 'class'), "<attribute_name> :: <attribute_value>" (if anchor_tag is ['li', 'a', 'div']) 
                or "<anchor_tag> >> <attribute_name> :: <attribute_value>", e.g. ["arrow next", "right-box right", "title :: Dalej"] (for phpBB engine)
            - topic_title_class (List[str]): HTML selector for topic title on topic website. Searche for first instance -> "<anchor_tag> >> <attribute_name> :: <attribute_value>"
                e.g. ["h2 >>  :: ", "h2 >> class :: topic-title"] (for phpBB engine)
            - content_class (List[str]): HTML selectors used for identifying the main content within a topic. "<anchor_tag> >> <attribute_name> :: <attribute_value>", 
                e.g. ["content_class"] (for phpBB engine)

        Attributes:
        - settings (dict): A dictionary of all the settings for the crawler.
        - robot_parser (RobotFileParser): Parser for robots.txt (if check_robots is True)
        - headers (dict): Headers e.g. 'User-Agent' of crawler. 
        - force_crawl (bool): Indicates whether robots.txt is taken into account (e.g. robots.txt is parsed wrongly)
        """
        # logging.basicConfig(format = '%(asctime)s: %(levelname)s: %(message)s', level = log_lvl, encoding='utf-8')

        #TODO: check_for_library_updates()

        self._check_instance(threads_class = threads_class, threads_whitelist = threads_whitelist, threads_blacklist = threads_blacklist, topic_class = topic_class,
                            topic_whitelist = topic_whitelist, topic_blacklist = topic_blacklist, pagination = pagination, topic_title_class = topic_title_class, content_class = content_class)
        
        self.settings = self._initialize_settings(dataset_url = dataset_url, dataset_category = dataset_category, dataset_name = dataset_name, forum_engine = forum_engine, 
                            processes = processes, time_sleep = time_sleep, save_state = save_state, min_len_txt = min_len_txt, sitemaps = sitemaps, force_crawl = force_crawl,
                            threads_class = threads_class, threads_whitelist = threads_whitelist, threads_blacklist = threads_blacklist, topic_class = topic_class,
                            topic_whitelist = topic_whitelist, topic_blacklist = topic_blacklist, pagination = pagination, topic_title_class = topic_title_class, content_class = content_class)
        
        if arg_parser == True:
            self._parse_arguments()

        print("*******************************************")

        self.files_folder = "scraper_workspace"
        self.dataset_folder = os.path.join(self.files_folder, self.settings['DATASET_NAME'])
        if not os.path.exists(self.dataset_folder):
            os.makedirs(self.dataset_folder)

        print(f"* Set some settings... Working dir: {self.files_folder} | Folder: {self.settings['DATASET_NAME']}")

        logs_path = os.path.join(self.dataset_folder, f"logs_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.log")
        print(f"Logs will be in: {logs_path}")

        #TODO: Use 1-st - logging.basicConfig - for logging to file in dataset directory
        logging.basicConfig(format = '%(asctime)s: %(levelname)s: %(message)s', level = log_lvl, filename = logs_path, encoding='utf-8')
        # logging.basicConfig(format = '%(asctime)s: %(levelname)s: %(message)s', level = log_lvl, encoding='utf-8')
        
        logging.info("*******************************************")

        self.headers = {
	        'User-Agent': 'Speakleash',
	        "Accept-Encoding": "gzip, deflate",
	        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	        "Connection": "keep-alive"
	    }

        logging.info(f"*** Start setting crawler for -> {self.settings['DATASET_URL']} ***")
        print(f"* Start setting crawler for -> {self.settings['DATASET_URL']} ***")

        if check_robots == True:
            logging.info(f"Force crawl set to: {self.settings['FORCE_CRAWL']}")
            self.robot_parser, self.force_crawl = self._check_robots_txt(force_crawl = self.settings['FORCE_CRAWL'])
        else:
            self.robot_parser = self.init_robotstxt()
            self.force_crawl = True

        self._print_settings()


    ### Functions ###

    def _initialize_settings(self, dataset_url: str, dataset_category: str, dataset_name: str = "", forum_engine: str = 'invision', processes: int = 2,
                time_sleep: float = 0.5, save_state: int = 100, min_len_txt: int = 20, sitemaps: str = "", force_crawl: bool = False,
                threads_class: List[str] = [], threads_whitelist: List[str] = [], threads_blacklist: List[str] = [], topic_class: List[str] = [],
                topic_whitelist: List[str] = [], topic_blacklist: List[str] = [], pagination: List[str] = [], topic_title_class: List[str] = [], content_class: List[str] = []) -> dict:
        """
        Initialize dict with info for manifest and settings for crawler/scraper.

        :param: a lot

        :return: Dict with settings for manifest and crawler/scraper.
        """
        parsed_url = urlparse(dataset_url)

        self.main_site = dataset_url
        if parsed_url.path:
            self.main_site = dataset_url.replace(parsed_url.path, '')

        dataset_domain = parsed_url.netloc.replace('www.', '')
        if not dataset_name:
            dataset_name = f"{dataset_category.lower()}_{dataset_domain.replace('.', '_')}_corpus"

        return {
            'DATASET_CATEGORY': dataset_category,
            'DATASET_URL': dataset_url,
            'DATASET_NAME': dataset_name,
            'DATASET_DESCRIPTION': f"Collection of forum discussions from {dataset_domain}",
            'DATASET_LICENSE': f"(c) {dataset_domain}",
            'FORUM_ENGINE': forum_engine,
            'PROCESSES': processes,
            'TIME_SLEEP': time_sleep,
            'SAVE_STATE': save_state,
            'MIN_LEN_TXT': min_len_txt,
            'SITEMAPS': sitemaps,
            'FORCE_CRAWL': force_crawl,
            'THREADS_CLASS': threads_class,
            'THREADS_WHITELIST': threads_whitelist,
            'THREADS_BLACKLIST': threads_blacklist,
            'TOPICS_CLASS': topic_class,
            'TOPICS_WHITELIST': topic_whitelist,
            'TOPICS_BLACKLIST': topic_blacklist,
            'PAGINATION': pagination,
            'TOPIC_TITLE_CLASS': topic_title_class,
            'CONTENT_CLASS': content_class
        }

    def _parse_arguments(self) -> None:
        """
        Parsing arguments for the starter scipt like 'main.py', e.g. DATASET_URL, FORUM_ENGINE etc.
        """
        parser = argparse.ArgumentParser(description='Crawler and scraper for forums')
        parser.add_argument("-D_C", "--DATASET_CATEGORY", help="Set category e.g. Forum", default="Forum", type=str)
        parser.add_argument("-D_U" , "--DATASET_URL", help="Desire URL with http/https e.g. https://forumaddress.pl", default="", type=str)
        parser.add_argument("-D_N" , "--DATASET_NAME", help="Dataset name e.g. forum_<url_domain>_pl_corpus", default="", type=str)
        parser.add_argument("-D_D" , "--DATASET_DESCRIPTION", help="Description e.g. Collection of forum discussions from DATASET_URL", default="", type=str)
        parser.add_argument("-D_L" , "--DATASET_LICENSE", help="Dataset license e.g. (c) DATASET_URL", default="", type=str)
        parser.add_argument("-D_E" , "--FORUM_ENGINE", help="Engine used to build forum website: ['invision', 'phpbb', 'ipboard', 'xenforo', 'other']", default="", type=str)
        parser.add_argument("-proc", "--PROCESSES", help="Number of processes - from 1 up to os.cpu_count()", type=int)
        parser.add_argument("-sleep", "--TIME_SLEEP", help="Waiting interval between requests (in sec)", type=float)
        parser.add_argument("-save", "--SAVE_STATE", help="URLs interval at which script saves data, prevents from losing data if crashed or stopped", type=int)
        parser.add_argument("-min_len", "--MIN_LEN_TXT", help="Minimum character count to consider it a text data", type=int)
        parser.add_argument("-sitemaps" , "--SITEMAPS", help="Desire URL with sitemaps", default="", type=str)
        parser.add_argument("-force", "--FORCE_CRAWL", help="Force to crawl website - overpass robots.txt", action='store_true')
        parser.add_argument("-threads_class", "--THREADS_CLASS", help="Threads/Forums HTML tags: <anchor_tag> >> <attribute_name> :: <attribute_value> -> e.g. ['a >> class :: forumtitle'] (for phpBB engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-threads_whitelist", "--THREADS_WHITELIST", help="Threads/Forums whitelist for URLs, e.g. ['forum'] (for Invision engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-threads_blacklist", "--THREADS_BLACKLIST", help="Threads/Forums blacklist for URLs, e.g. ['topic'] (no, it is not a typo) (for Invision engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-topics_class", "--TOPICS_CLASS", help="Topics HTML tags: <anchor_tag> >> <attribute_name> :: <attribute_value> -> e.g. ['a >> class :: topictitle'] (for phpBB engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-topics_whitelist", "--TOPICS_WHITELIST", help="Topics whitelist for URLs, e.g. ['topic'] (for Invision engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-topics_blacklist", "--TOPICS_BLACKLIST", help="Topics blacklist for URLs, e.g. ['page', '#comments'] (no, it is not a typo) (for Invision engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-pagination", "--PAGINATION", help="<attribute_value> (when attribute_name is 'class'), <attribute_name> :: <attribute_value> (if anchor_tag is ['li', 'a', 'div']) or <anchor_tag> >> <attribute_name> :: <attribute_value> -> e.g. ['arrow next', 'right-box right', 'title :: Dalej'] (for phpBB engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-topic_title_class", "--TOPIC_TITLE_CLASS", help="<attribute_value> (when attribute_name is 'class'), <attribute_name> :: <attribute_value> (if anchor_tag is ['li', 'a', 'div']) or <anchor_tag> >> <attribute_name> :: <attribute_value> -> e.g. ['h2 >> :: ', 'h2 >> class :: topic-title'] (for phpBB engine) | (can pass multiple)", nargs='*')
        parser.add_argument("-content_class", "--CONTENT_CLASS", help="Topics HTML tags: <anchor_tag> >> <attribute_name> :: <attribute_value> -> e.g. ['div >> class :: content'] (for phpBB engine) | (can pass multiple)", nargs='*')
        args = parser.parse_args()

        parsed_url = urlparse(args.DATASET_URL)
        dataset_domain = parsed_url.netloc.replace('www.', '')
        dataset_name = f"{args.DATASET_CATEGORY.lower()}_{dataset_domain.replace('.', '_')}_corpus"

        if not args.DATASET_NAME:
            args.DATASET_NAME = dataset_name
        if not args.DATASET_DESCRIPTION:
            args.DATASET_DESCRIPTION = f"Collection of forum discussions from {dataset_domain}"
        if not args.DATASET_LICENSE:
            args.DATASET_LICENSE = f"(c) {dataset_domain}"

        # Update settings with any arguments provided
        for arg in vars(args):
            if getattr(args, arg) is not None:
                self.settings[arg] = getattr(args, arg)

    def _check_robots_txt(self, force_crawl: bool = False) -> Optional[Tuple[urllib.robotparser.RobotFileParser, bool]]:
        """
        Parsing 'robots.txt' and set some settings if 'robots.txt' overdrive it.

        :param force_crawl (bool): If False (default) we respect website robots.txt (but robots.txt can be wrongly parsed)

        :return: Returns Tuple with robotparser and force_crawl parameter.
        """
        robots_url = urljoin(self.main_site, "robots.txt")
        logging.info(f"* robots.txt expected url: {robots_url}")
        
        rp = urllib.robotparser.RobotFileParser()
        try:
            logging.info("* Parsing 'robots.txt' lines...")
            print("* Parsing 'robots.txt' lines...")
            with urllib.request.urlopen(urllib.request.Request(robots_url, headers=self.headers)) as response:
                try:
                    rp.parse(response.read().decode("utf-8").splitlines())
                except Exception as e:
                    logging.error(f"Error while parsing lines -> using 'latin-1' || Error: {e}")
                    rp.parse(response.read().decode("latin-1").splitlines())
        except Exception as err:
            rp.set_url(robots_url)
            rp.read()
            logging.info("Read 'robots.txt' -> CHECK robots.txt -> Sleep for 1 min")
            logging.warning(f"Error while parsing lines: {err}")
            print("* Read 'robots.txt' -> CHECK robots.txt -> Sleep for 1 min")
            print(f"Error while parsing lines: {err}")
            time.sleep(60)


        if not rp.can_fetch("*", self.settings['DATASET_URL']) and force_crawl == False:
            logging.error(f"ERROR! * robots.txt disallow to scrap this website: {self.settings['DATASET_URL']}")
            print(f"ERROR! * robots.txt disallow to scrap this website: {self.settings['DATASET_URL']}")
            exit()
        else:
            logging.info(f"* robots.txt allow to scrap this website: {self.settings['DATASET_URL']}")

        rrate = rp.request_rate("*")
        if rrate:
            logging.info(f"* robots.txt -> requests: {rrate.requests}")
            logging.info(f"* robots.txt -> seconds: {rrate.seconds}")
            logging.info(f"* setting scraper time_sleep to: {(rrate.seconds / rrate.requests):.2f}")
            self.settings['TIME_SLEEP'] = round(rrate.seconds / rrate.requests, 2)
            self.settings['PROCESSES'] = 2
            logging.info(f"* also setting scraper processes to: {self.settings['PROCESSES']}")
        
        if rp.crawl_delay('*'):
            logging.info(f"* robots.txt -> crawl delay: {rp.crawl_delay('*')}")
            self.settings['TIME_SLEEP'] = rp.crawl_delay('*')
            self.settings['PROCESSES'] = 2
            logging.info(f"* also setting scraper processes to: {self.settings['PROCESSES']}")
        
        if rp.site_maps():
            logging.info(f"* robots.txt -> sitemaps links: {rp.site_maps()}")
            self.settings['SITEMAPS'] = rp.site_maps()

        return (rp, force_crawl)


    def _check_instance(self, threads_class: List[str] = [], threads_whitelist: List[str] = [], threads_blacklist: List[str] = [], topic_class: List[str] = [],
                topic_whitelist: List[str] = [], topic_blacklist: List[str] = [], pagination: List[str] = [], topic_title_class: List[str] = [], content_class: List[str] = []) -> None:
        """
        Check instance of lists for threads/topic/pagination/content classes and whitelist/blacklist.
        """
        try:
            not_instance_flag = False

            if not isinstance(threads_class, list):
                logging.warning("Please check param: threads_class")
                not_instance_flag = True
            if not isinstance(threads_whitelist, list):
                logging.warning("Please check param: threads_whitelist")
                not_instance_flag = True
            if not isinstance(threads_blacklist, list):
                logging.warning("Please check param: threads_blacklist")
                not_instance_flag = True
            if not isinstance(topic_class, list):
                logging.warning("Please check param: topic_class")
                not_instance_flag = True
            if not isinstance(topic_whitelist, list):
                logging.warning("Please check param: topic_whitelist")
                not_instance_flag = True
            if not isinstance(topic_blacklist, list):
                logging.warning("Please check param: topic_blacklist")
                not_instance_flag = True
            if not isinstance(pagination, list):
                logging.warning("Please check param: pagination")
                not_instance_flag = True
            if not isinstance(topic_title_class, list):
                logging.warning("Please check param: topic_title_class")
                not_instance_flag = True
            if not isinstance(content_class, list):
                logging.warning("Please check param: content_class")
                not_instance_flag = True
            if not_instance_flag == True:
                logging.warning("Exiting... Check parameters...")
                exit()
        except Exception as e:
            logging.error(f"Config: Error while checking lists of threads/topics/whitelist/blacklist to search! Error: {e}")


    def _print_settings(self) -> None:

        to_print = "--- Crawler settings ---"
        for key, value in self.settings.items():
            to_print = to_print + f"\n{key}: {value}"

        logging.info(to_print)
        print(to_print)
        logging.info("--- --- --- --- --- --- ---")
        print("--- --- --- --- --- --- ---")
        time.sleep(2)


    def init_robotstxt(self) -> urllib.robotparser.RobotFileParser:
        file = "User-agent: *\nAllow: /"
        
        rp = urllib.robotparser.RobotFileParser()
        
        logging.info("Parsing illusion of 'robots.txt'")
        rp.parse(file)

        if not rp.can_fetch("*", self.settings['DATASET_URL']):
            logging.error(f"ERROR! * robots.txt disallow to scrap this website: {self.settings['DATASET_URL']}")
            exit()
        else:
            logging.info(f"* robots.txt allow to scrap this website: {self.settings['DATASET_URL']}")

        return rp

