import logging
from pydantic import BaseModel, Field
from typing import Literal


class ConfigManager(BaseModel):
    """
    A configuration manager for setting up and managing settings for a forum crawler.
    """

    dataset_url: str = Field(
        "https://forum.szajbajk.pl", description="URL of the forum to crawl"
    )
    dataset_category: str = Field(
        "Forum", description="Category of the dataset to crawl"
    )
    forum_engine: Literal["invision", "phpbb", "ipboard", "xenforo", "other"] = Field(
        "invision", description="Engine of the forum"
    )
    dataset_name: str = Field("", description="Optional custom name for the dataset")
    arg_parser: bool = Field(
        False, description="Flag to enable command-line argument parsing"
    )
    check_robots: bool = Field(
        True, description="Flag to check and adhere to the forum's robots.txt"
    )
    force_crawl: bool = Field(
        False, description="Flag to force crawling even if disallowed by robots.txt"
    )
    processes: int = Field(
        2, description="The number of processes to use for multiprocessing"
    )
    time_sleep: float = Field(
        0.5, description="Time delay between requests during crawling"
    )
    save_state: int = Field(
        100, description="Checkpoint interval for saving the crawling progress"
    )
    min_len_txt: int = Field(
        20, description="Minimum length of text to consider valid for scraping"
    )
    sitemaps: str = Field("", description="Custom sitemaps to be used for crawling")
    log_lvl: str = Field(logging.INFO, description="Logging level for the crawler")
    print_to_console: bool = Field(
        True, description="Flag to enable or disable console logging"
    )
    threads_class: list[str] = Field(
        [], description="HTML selectors used for identifying thread links in the forum"
    )
    threads_whitelist: list[str] = Field(
        [],
        description="List of substrings; only threads whose URLs contain any of these substrings will be processed",
    )
    threads_blacklist: list[str] = Field(
        [],
        description="List of substrings; threads whose URLs contain any of these substrings will be ignored",
    )
    topic_class: list[str] = Field(
        [],
        description="HTML selectors used for identifying topic links within a thread",
    )
    topic_whitelist: list[str] = Field(
        [],
        description="List of substrings; only topics whose URLs contain any of these substrings will be processed",
    )
    topic_blacklist: list[str] = Field(
        [],
        description="List of substrings; topics whose URLs contain any of these substrings will be ignored",
    )
    pagination: list[str] = Field(
        [],
        description="HTML selectors used for identifying pagination elements within threads or topics",
    )
    topic_title_class: list[str] = Field(
        [], description="HTML selector for topic title on topic website"
    )
    content_class: list[str] = Field(
        [],
        description="HTML selectors used for identifying the main content within a topic",
    )
    web_encoding: str = Field("", description="Web encoding")
