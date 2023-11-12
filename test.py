from speakleash_forum_tools import ConfigManager
from speakleash_forum_tools import ForumEnginesManager



# Example usage:
if __name__ == "__main__":
    print("*****************")
    config_manager = ConfigManager(dataset_url="https://forum.prawojazdy.com.pl", forum_engine='phpbb')
    # config_manager = ConfigManager(arg_parser=True)
    # config_manager = ConfigManager(dataset_url='https://max3d.pl/forums/', force_crawl=True)
    
    # Use the settings from config_manager.settings as needed
    print(config_manager.settings)

    crawler = ForumEnginesManager(config_manager.settings)
    print(crawler.crawl_forum())
    print(f"{len(crawler.forum_threads)}")
    print(f"{len(crawler.threads_topics)}")
    print(f"{len(crawler.urls_all)}")