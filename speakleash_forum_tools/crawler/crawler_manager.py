"""
Crawler Manager Module

This module offers a comprehensive solution for crawling various types of forum software. 
It includes the CrawlerManager class, which orchestrates the process of extracting threads and topics from forum pages. 
The manager utilizes specific crawler classes tailored to different forum engines, ensuring efficient and targeted data extraction.

Classes:
- CrawlerManager: 

Dependencies:

"""
import os
import time
import logging
from typing import List

import pandas
from usp.tree import sitemap_tree_for_homepage      # install ultimate-sitemap-parser (use this fork: pip install git+https://github.com/Samox1/ultimate-sitemap-parser@develop#egg=ultimate-sitemap-parser )

from speakleash_forum_tools.config import ConfigManager
from speakleash_forum_tools.forums_engines import ForumEnginesManager

class CrawlerManager:
    """
    Crawler Manager class handle crawling on given forum website - using sitemaps (if found) or manual crawling using ForumEnginesManager class.
    Import all settings from ConfigManager class, process website and return Topic URLs list.

    How it works:
    1) Import all settings, robotparser and other from ConfigManager
    2) Checks if sitemaps are in robots.txt
    3) Checks if sitemaps are in other places or if they exist at all
    4) If sitemaps exist it parses them using the appropriate engine / library
    5) If sitemaps do not exist then it uses the forum crawl engine from the ForumEnginesManager class.
    6) If there are pages / topics to crawl then it displays the number of them and returns a list of URLs (optionally with topic names if collected manually)
    7) Updates all settings for further process -> scraping

    :param config_manager (ConfigManager): Configuration class, containing settings with keys like 'FORUM_ENGINE', 'DATASET_URL', etc.
    .. note:: The `urllib.robotparser` is used to ensure compliance with the forum's scraping policies as declared in its 'robots.txt'.

    Important:
    - threads_class (List[str]): "<anchor_tag> >> <attribute_name> :: <attribute_value>", 
        e.g. ["a >> class :: forumtitle"] (for phpBB engine)
    - topics_class (List[str]): "<anchor_tag> >> <attribute_name> :: <attribute_value>", 
        e.g. ["a >> class :: topictitle"] (for phpBB engine)
    - threads_whitelist (List[str]): to validate URL with specific words, 
        e.g. ["forum"] (for Invision engine)
    - threads_blacklist (List[str]): but sometimes whitelist is not enough, 
        e.g. ["topic"] (no, it is not a typo) (for Invision engine)
    - topics_whitelist (List[str]): to validate URL with specific words, 
        e.g. ["topic"] (for Invision engine)
    - topics_blacklist (List[str]): but sometimes whitelist is not enough, 
        e.g. ["page", "#comments"] (for Invision engine)
    - pagination (List[str]): "<attribute_value>" (when attribute_name is 'class'), "<attribute_name> :: <attribute_value>" (if anchor_tag is ['li', 'a', 'div']) 
        or "<anchor_tag> >> <attribute_name> :: <attribute_value>", e.g. ["arrow next", "right-box right", "title :: Dalej"] (for phpBB engine)
    - topic_title_class (List[str]): Searched for first instance -> "<anchor_tag> >> <attribute_name> :: <attribute_value>"
        e.g. ["h2 >>  :: ", "h2 >> class :: topic-title"] (for phpBB engine)
    - content_class (List[str]): "<anchor_tag> >> <attribute_name> :: <attribute_value>", 
        e.g. ["content_class"] (for phpBB engine)

    """

    def __init__(self, config_manager: ConfigManager):
        self.files_folder = "scraper_workspace"
        self.dataset_name = config_manager.settings['DATASET_NAME']
        self.topics_dataset_file = f"Topics_URLs_{self.dataset_name}.csv"
        self.topics_visited_file = f"Visited_URLs_{self.dataset_name}.csv"

        self.check_dataset_files(self.dataset_name, self.topics_dataset_file, self.topics_visited_file)

        forum_engine = ForumEnginesManager(config_manager = config_manager)
        
        if config_manager.settings['SITEMAPS']:
            self.sitemaps_url = config_manager.settings['SITEMAPS']
        else:
            self.sitemaps_url = config_manager.main_site

        try:
            logging.info("---------------------------------------------------------------------------------------------------")
            logging.info(f"* Crawler will try to find and parse Sitemaps (using 'usp' library)...")
            forum_tree = self._tree_sitemap(self.sitemaps_url)
            self.forum_topics_list = self._urls_generator(forum_tree = forum_tree, 
                                                         whitelist = forum_engine.topics_whitelist, blacklist = forum_engine.topics_blacklist, 
                                                         robotparser = config_manager.robot_parser, force_crawl = config_manager.force_crawl)
            logging.info("---------------------------------------------------------------------------------------------------")
        except Exception as e:
            logging.error(f"CRAWLER: Error while searching and parsing Sitemaps: {e}")

        if not self.forum_topics_list:
            logging.warning("---------------------------------------------------------------------------------------------------")
            logging.warning(f"* Crawler did not find any Topics URLs... -> checking manually using engine for: {forum_engine.engine_type}")
            
            if forum_engine.crawl_forum():
                self.forum_topics_list = forum_engine.get_topics_urls_only()
                self.forum_topics_list_titles = forum_engine.get_topics_titles_only()

        logging.info(f"* Crawler (Manager) found: Topics = {len(self.forum_topics_list)}")


    ### Functions ###

    def _tree_sitemap(self, url: str):
        """
        Uses the Ulitmate Sitemap Parser's (Samox1 fork with extended search for XML and PHP files) sitemap_tree_for_homepage method to get the sitemap and extract all the URLs.

        :param url (str): Website URL to get sitemap.

        :return: Tree of AbstractSitemap subclass objects that represent the sitemap hierarchy found on the website.
        """
        start_time = time.perf_counter()
        forum_tree = sitemap_tree_for_homepage(url)
        end_time = time.perf_counter()
        logging.info(f"Crawler // Sitemap parsing = DONE || Time = {(end_time - start_time):.2f} sec = {((end_time - start_time) / 60):.2f} min")
        return forum_tree

    def _urls_generator(self, forum_tree, whitelist: List[str], blacklist: List[str], robotparser, force_crawl: bool = False) -> list[str]:
        """
        Uses the Ulitmate Sitemap Parser's sitemap_tree_for_homepage method to get the sitemap and extract all the URLs.

        :param forum_tree (AbstractSitemap): Tree of AbstractSitemap subclass objects that represent the sitemap hierarchy found on the website.

        :return: Extract all urls to scrap (list[str]).
        """
        # Extract all the URLs with EXPECTED_URL_PARTS (whitelist) in it 
        urls_expected: list[str] = []

        for page in forum_tree.all_pages():
            if whitelist:
                if any(url_part in page.url for url_part in whitelist):
                    if blacklist:
                        if any(url_part in page.url for url_part in blacklist):
                            logging.debug(f"URL OUT <- {page.url}")
                            continue
                    if robotparser.can_fetch("*", page.url) or force_crawl == True:
                        logging.debug(f"URL GOOD -> {page.url}")
                        urls_expected.append(page.url)
                    else:
                        logging.debug(f"URL OUT <- {page.url}")
                    continue
                else:
                    logging.debug(f"URL OUT <- {page.url}")
                    continue
            if blacklist:
                if any(url_part in page.url for url_part in blacklist):
                    logging.debug(f"URL OUT <- {page.url}")
                    continue
                        
            if not whitelist or not blacklist:
                if robotparser.can_fetch("*", page.url) or force_crawl == True:
                    logging.debug(f"URL GOOD (+) -> {page.url}")
                    urls_expected.append(page.url)
                else:
                    logging.debug(f"URL OUT (+) <- {page.url}")

        urls_expected = list(set(urls_expected))
        logging.debug(f"CRAWLER // URL Generator -> URLs_expected: {len(urls_expected)}")
        return urls_expected
    

    def check_dataset_files(self, dataset_name: str, topics_urls_filename: str = "Topics_URLs.csv", topics_visited_filename: str = "Visited_Topics_URLs.csv") -> tuple[pandas.DataFrame, pandas.DataFrame]:
        """
        Checking if exists files with:
        1) forum urls - if not create sitemaps tree -> generate urls -> save to file.
        2) visited urls - if not create empty file.

        :param crawl_urls_filename (str): Filename for CSV file with topics urls --> 3 columns = ['Topic_URLs', 'Topic_Titles'] (sep = '\t').
        :param visited_filename (str): Filename for CSV file with visited urls --> 3 columns = ['Topic_URLs', 'Topic_Titles', 'Visited_flag', 'Skip_flag'] (sep = '\t').

        Returns
        -------
        :return topics_links (pandas.DataFrame): DataFrame (pandas) with urls generated from sitemaps tree or crawler engine, ['Topic_URLs', 'Topic_Titles'].
        :return visited_links (pandas.DataFrame): DataFrame (pandas) with ['Topic_URLs', 'Topic_Titles', 'Visited_flag', 'Skip_flag'].
        """
        dataset_folder = os.path.join(self.files_folder, self.dataset_name)
        
        sitemap_tree = []
        topics_links = pandas.DataFrame(columns=['Topic_URLs', 'Topic_Titles'])
        visited_links = pandas.DataFrame(columns=['Topic_URLs', 'Topic_Titles', 'Visited_flag', 'Skip_flag'])
        flag_fresh_start = False

        if os.path.exists(dataset_folder):
            logging.info(f"* Folder for [{dataset_name}] exists -> Checking files...")
            # Check if file with Topics URLs exists
            if os.path.exists(os.path.join(dataset_folder, topics_urls_filename)):
                # Read parsed Topics URLs
                topics_links = pandas.read_csv(os.path.join(dataset_folder, topics_urls_filename), sep = '\t', header = None, names = ['Topic_URLs', 'Topic_Titles'])
                logging.info(f"CHECK_TOPICS_FILES // Imported DataFrame for: {dataset_name} | Shape: {topics_links.shape} | Size in memory (MB): {topics_links.memory_usage(deep=True).sum() / pow(10,6)}")
            else:
                logging.info(f"File with Topics URLs not found... ({topics_urls_filename})")

            if os.path.exists(os.path.join(dataset_folder, topics_visited_filename)):
                # Read scraped Visited Topics URLs
                visited_links = pandas.read_csv(os.path.join(dataset_folder, topics_visited_filename), sep = '\t', header = None, names = ['Topic_URLs', 'Topic_Titles', 'Visited_flag', 'Skip_flag'])
                logging.info(f"CHECK_TOPICS_FILES // Imported DataFrame for: {dataset_name} | Shape: {visited_links.shape} | Size in memory (MB): {visited_links.memory_usage(deep=True).sum() / pow(10,6)}")
            else:
                logging.info(f"File with Visited Topics URLs not found... ({topics_visited_filename})")
        else:
            logging.info(f"* Can't find folder for [{dataset_name}]... -> Create new folder...")
            os.makedirs(dataset_folder)

        if topics_links:
            topics_links = topics_links.drop_duplicates(ignore_index=True)
            topics = True
        else:
            topics = False

        if visited_links:
            visited_links = visited_links.drop_duplicates(ignore_index=True)
            visited = True
        else:
            visited = False

        # Check if file with URLs from sitemaps exist, if not search for sitemaps and return links
        if os.path.exists(path = topics_urls_filename):
            logging.info(f"*** Resuming from previous progress... ***")
            # Read the saved URLs from the file
            logging.info(f"CHECK_TOPICS_FILES // Importing DataFrame for: {dataset_name}")
            topics_links = pandas.read_csv(topics_urls_filename, sep = '\t', header = None, names = ['urls','visited','skip'])
            logging.info(f"CHECK_TOPICS_FILES // Imported DataFrame for: {DATASET_URL} | Shape: {forum_links.shape} | Size in memory (MB): {forum_links.memory_usage(deep=True).sum() / pow(10,6)}")
        else:
            logging.info(f"*** Starting from scratch... ***")
            # Create sitemap tree for domain url
            logging.info(f"CHECK_TOPICS_FILES // Creating sitemaps tree...")
            sitemap_tree = tree_sitemap(DATASET_URL)
            logging.info(f"CHECK_TOPICS_FILES // Created sitemaps tree for: {DATASET_URL}")

            # Create DataFrame with URLs + columns: 'visited','skip'
            forum_links = pandas.DataFrame(urls_generator(sitemap_tree), columns=['urls'])
            if forum_links.shape[0] == 0:
                logging.error(f"CHECK_TOPICS_FILES + SITEMAP // No valid URL found in SITEMAPs -> Check it manually ({forum_links.shape=})")
                raise ValueError("No valid URL found in SITEMAPs.")
            forum_links.drop_duplicates(inplace=True, ignore_index=True)
            forum_links['visited'] = 0
            forum_links['skip'] = 0
            logging.info(f"CHECK_TOPICS_FILES // Created DataFrame for: {DATASET_URL} | Shape: {forum_links.shape} | Size in memory (MB): {forum_links.memory_usage(deep=True).sum() / pow(10,6)}")
            save_visited_dataframe(urls = forum_links, file_name = urls_filename)
            flag_fresh_start = True


        # Check if file with visited is created, if not then create
        if os.path.exists(visited_filename) and flag_fresh_start == False:
            logging.info(f"FILE_CHECK // Reading file with visited links ...")
            visited_links = pandas.read_csv(visited_filename, sep = '\t', header = None, names = ['urls','visited','skip'])
            visited_links.drop_duplicates(inplace=True, ignore_index=True)
            logging.info(f"FILE_CHECK // File for saving visited links already created: {visited_filename} | Lines: {visited_links.shape} | Visited: {visited_links['visited'].sum()} | Skipped: {visited_links['skip'].sum()}")
        else:
            with open(visited_filename, "w") as f:
                pass
            logging.info(f"FILE_CHECK // File for saving visited links created: {visited_filename}")

        return forum_links, visited_links
