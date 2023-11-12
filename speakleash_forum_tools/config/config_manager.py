"""
Config Manager Module

This module provides a configuration management class for a forum crawling and scraping tool.

Classes:
- ConfigManager: Initializes with default values for crawler, scraper and manifest informations, 
can parse command-line arguments, and checks the site's robots.txt file to ensure compliance with scraping policies.

Dependencies:
- logging: Provides logging worflow.
- argparse: Provides parser for command-line arguments.
- urllib: Provides RobotFileParser and functions to parse and join urls.
- datetime: Provides time informations.
"""
import logging
import argparse
import urllib.robotparser
from urllib.parse import urlparse, urljoin
from datetime import datetime
from typing import Optional

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s', level=logging.DEBUG)


class ConfigManager:
    """
    A configuration manager for setting up and managing settings for a forum crawler.

    Attributes:
        settings (dict): A dictionary of all the settings for the crawler.
        robot_parser (RobotFileParser): Parser for robots.txt (if check_robots is True)
        headers (dict): Headers e.g. 'User-Agent' of crawler. 
    """

    def __init__(self, dataset_url: str = "https://forum.szajbajk.pl", dataset_category: str = 'Forum', forum_engine: str = 'invision',
                 arg_parser: bool = False, check_robots: bool = True, force_crawl: bool = False,
                 processes: int = 2, time_sleep: float = 0.5, save_state: int = 100, min_len_txt: int = 20):
        """
        Initializes the ConfigManager with defaults or overridden settings based on provided arguments.

        Args:
            dataset_url (str): The base URL of the dataset/forum to be crawled.
            dataset_category (str): The category of the dataset/forum.
            forum_engine (str): The forum engine used on website.
            arg_parser (bool): Flag to determine if command-line arguments should be parsed.
            check_robots (bool): Flag to determine if robots.txt should be checked.
            force_crawl (bool): Flag to force crawling even if disallowed by robots.txt.
            processes (int): Number of processes to use for multiprocessing.
            time_sleep (float): Time in seconds to sleep between requests.
            save_state (int): Interval at which to save crawling state.
            min_len_txt (int): Minimum length of text to consider as valid data.
        """
        self.settings = self._initialize_settings(dataset_url, dataset_category, forum_engine = forum_engine, processes = processes,
                                time_sleep = time_sleep, save_state = save_state, min_len_txt = min_len_txt, force_crawl = force_crawl)
        if arg_parser == True:
            self._parse_arguments()

        # logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO, filename = f'{self.settings['DATASET_NAME']}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.log', encoding='utf-8')
        logging.info("+++++++++++++++++++++++++++++++++++++++++++")
        logging.info(f"*** Start setting crawler for -> {self.settings['DATASET_URL']} ***")

        if check_robots == True:
            logging.info(f"Force crawl set to: {self.settings['FORCE_CRAWL']}")
            self.robot_parser = self._check_robots_txt(force_crawl = self.settings['FORCE_CRAWL'])
        else:
            self.robot_parser = None

        logging.info(f"Settings for manifest:  \n \
                        {self.settings['DATASET_CATEGORY']=}\n \
                        {self.settings['DATASET_URL']=}\n \
                        {self.settings['DATASET_NAME']=}\n \
                        {self.settings['DATASET_DESCRIPTION']=}\n \
                        {self.settings['DATASET_LICENSE']=}")
        logging.info(f"Settings for scraper: \n \
                        {self.settings['FORUM_ENGINE']=}\n \
                        {self.settings['PROCESSES']=}\n \
                        {self.settings['TIME_SLEEP']=}\n \
                        {self.settings['SAVE_STATE']=}\n \
                        {self.settings['MIN_LEN_TXT']=}\n \
                        {self.settings['FORCE_CRAWL']=}")
        
        self.headers = {
	        'User-Agent': 'Speakleash',
	        "Accept-Encoding": "gzip, deflate",
	        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	        "Connection": "keep-alive"
	    }

    def _initialize_settings(self, dataset_url: str, dataset_category: str, forum_engine: str = 'invision', processes: int = 2,
                time_sleep: float = 0.5, save_state: int = 100, min_len_txt: int = 20, force_crawl: bool = False) -> dict:

        parsed_url = urlparse(dataset_url)
        dataset_domain = parsed_url.netloc.replace('www.', '')
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
            'FORCE_CRAWL': force_crawl
        }

    def _parse_arguments(self) -> None:
        parser = argparse.ArgumentParser(description='Crawler and scraper for forums')
        parser.add_argument("-D_C", "--DATASET_CATEGORY", help="Set category e.g. Forum", default="Forum", type=str)
        parser.add_argument("-D_U" , "--DATASET_URL", help="Desire URL with http/https e.g. https://forumaddress.pl", default="", type=str)
        parser.add_argument("-D_E" , "--FORUM_ENGINE", help="Engine used to build forum website", default="", type=str)
        parser.add_argument("-D_N" , "--DATASET_NAME", help="Dataset name e.g. forum_<url_domain>_pl_corpus", default="", type=str)
        parser.add_argument("-D_D" , "--DATASET_DESCRIPTION", help="Description e.g. Collection of forum discussions from DATASET_URL", default="", type=str)
        parser.add_argument("-D_L" , "--DATASET_LICENSE", help="Dataset license e.g. (c) DATASET_URL", default="", type=str)
        parser.add_argument("-proc", "--PROCESSES", help="Number of processes - from 1 up to os.cpu_count()", type=int)
        parser.add_argument("-sleep", "--TIME_SLEEP", help="Waiting interval between requests (in sec)", type=float)
        parser.add_argument("-save", "--SAVE_STATE", help="URLs interval at which script saves data, prevents from losing data if crashed or stopped", type=int)
        parser.add_argument("-min_len", "--MIN_LEN_TXT", help="Minimum character count to consider it a text data", type=int)
        parser.add_argument("-force", "--FORCE_CRAWL", help="Force to crawl website - overpass robots.txt", action='store_true')
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

    def _check_robots_txt(self, force_crawl: bool = False) -> Optional[urllib.robotparser.RobotFileParser]:
        robots_url = self.settings['DATASET_URL']
        parsed_url = urlparse(self.settings['DATASET_URL'])
        if parsed_url.path:
            robots_url = self.settings['DATASET_URL'].replace(parsed_url.path, '')
            logging.warning(f"* robots.txt expected url: {robots_url}/robots.txt")
        
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(urljoin(robots_url, "robots.txt"))
        rp.read()

        if not rp.can_fetch("*", self.settings['DATASET_URL']) and force_crawl==False:
            logging.error(f"ERROR! * robots.txt disallow to scrap this website: {self.settings['DATASET_URL']}")
            exit()
        else:
            logging.info(f"* robots.txt allow to scrap this website: {self.settings['DATASET_URL']}")

        rrate = rp.request_rate("*")
        if rrate:
            logging.info(f"* robots.txt -> requests: {rrate.requests}")
            logging.info(f"* robots.txt -> seconds: {rrate.seconds}")
            logging.info(f"* setting scraper time_sleep to: {(rrate.seconds / rrate.requests):.2f}")
            self.settings['TIME_SLEEP'] = round(rrate.seconds / rrate.requests, 2)
            logging.info(f"* also setting scraper processes to: {2}")
            self.settings['PROCESSES'] = 2
        logging.info(f"* robots.txt -> crawl delay: {rp.crawl_delay('*')}")
        logging.info(f"* robots.txt -> sitemaps links: {rp.site_maps()}")

        return rp


# Example usage:
if __name__ == "__main__":

    config_manager = ConfigManager()
    # config_manager = ConfigManager(arg_parser=True)
    # config_manager = ConfigManager(dataset_url='https://max3d.pl/forums/', force_crawl=True)
    
    # Use the settings from config_manager.settings as needed
    print(config_manager.settings)
