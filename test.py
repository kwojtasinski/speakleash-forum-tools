from speakleash_forum_tools import ConfigManager
from speakleash_forum_tools import ForumEnginesManager

import pandas as pd



# Example usage:
if __name__ == "__main__":
    print("*****************")

    ### Engine: invision
    # config_manager = ConfigManager()      # Default: forum.szajbajk.pl -> invision                        # Topics to find = 18k (from dashboard)
    # config_manager = ConfigManager(dataset_url='https://max3d.pl/forums/', forum_engine='invision', force_crawl=True)   # Topics to find = 85.5k (from dashboard)

    ### Engine: phpBB 
    # config_manager = ConfigManager(dataset_url="https://forum.prawojazdy.com.pl", forum_engine='phpbb')   # Topics to find = 21 669 (calc from website)
    # config_manager = ConfigManager(dataset_url="https://www.gry-planszowe.pl", forum_engine='phpbb')      # Topics to find = 24 596 (calc from website)

    ### Engine: ipboard
    # config_manager = ConfigManager(dataset_url="https://forum.omegaklub.eu", forum_engine='ipboard')      # Disconnect for no reason
    config_manager = ConfigManager(dataset_url="http://forum-kulturystyka.pl", forum_engine='ipboard')    # Topics to find = 110k ~ 119k (calc ~ dashboard)

    ### Engine: xenforo
    # config_manager = ConfigManager(dataset_url="https://forum.modelarstwo.info/", forum_engine='xenforo')  # Topics to find = 16 500


    # Use the settings from config_manager.settings as needed
    print(config_manager.settings)

    crawler = ForumEnginesManager(config_manager.settings)
    print(crawler.crawl_forum())
    print(f"Forum Threads = {len(crawler.forum_threads)}")
    print(f"Threads Topics = {len(crawler.threads_topics)}")
    print(f"Crawler found URLs = {len(crawler.urls_all)}")

    forum_threads = pd.DataFrame(crawler.forum_threads)
    print(forum_threads.shape)
    forum_threads.to_csv('Forum_Threads.csv', sep='\t', encoding='utf-8')

    forum_topics = pd.DataFrame(crawler.threads_topics)
    print(forum_topics.shape)
    forum_topics.to_csv('Forum_Topics.csv', sep='\t', encoding='utf-8')
    