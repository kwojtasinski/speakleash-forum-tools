"""
Crawler Manager Module

This module offers a comprehensive solution for crawling various types of forum software. 
It includes the CrawlerManager class, which orchestrates the process of extracting threads and topics from forum pages. 
The manager utilizes specific crawler classes tailored to different forum engines, ensuring efficient and targeted data extraction.

Classes:
- CrawlerManager: 

Dependencies:

"""
import time
import logging
from typing import List

from usp.tree import sitemap_tree_for_homepage      # install ultimate-sitemap-parser

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
            logging.warning("---------------------------------------------------------------------------------------------------")
            
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

        logging.debug(f"CRAWLER // URL Generator -> URLs_expected: {len(urls_expected)}")
        return urls_expected
