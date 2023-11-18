from speakleash_forum_tools import ConfigManager
from speakleash_forum_tools import ForumEnginesManager

import pandas as pd



# Example usage:
if __name__ == "__main__":
    print("*****************")

    ### Engine: invision
    #+ config_manager = ConfigManager()      # Default: forum.szajbajk.pl -> invision                        # Topics to find = 18k (from dashboard) == almost all
    #+ config_manager = ConfigManager(dataset_url='https://max3d.pl/forums/', forum_engine='invision')   # Topics to find = 85.5k (from dashboard) == to big for testing quality

    ### Engine: phpBB 
    config_manager = ConfigManager(dataset_url="https://forum.prawojazdy.com.pl", forum_engine='phpbb')   # Topics to find = 21 669 (calc from website) == almost all
    #+ config_manager = ConfigManager(dataset_url="https://www.gry-planszowe.pl", forum_engine='phpbb')      # Topics to find = 24 596 (calc from website) == almost all
    #+ config_manager = ConfigManager(dataset_url="https://www.excelforum.pl", forum_engine='phpbb')        # Topics to find = 58 414 (calc from website) == 48k found

    ### Engine: ipboard
    #+ config_manager = ConfigManager(dataset_url="https://forum.omegaklub.eu", forum_engine='ipboard')      # Disconnect for no reason
    #+ config_manager = ConfigManager(dataset_url="http://forum-kulturystyka.pl", forum_engine='ipboard')    # Topics to find = 110k ~ 119k (calc ~ dashboard)

    ### Engine: xenforo
    # Crawl-delay = 15sec (in robots.txt)
    #+ config_manager = ConfigManager(dataset_url="https://forum.modelarstwo.info", forum_engine='xenforo')  # Topics to find = 16 500 (calc from website) == almost all


    # Use the settings from config_manager.settings as needed
    crawler = ForumEnginesManager(config_manager)
    print(crawler.crawl_forum())
    print(f"MAIN: Forum Threads = {len(crawler.forum_threads)}")
    print(f"MAIN: Threads Topics = {len(crawler.threads_topics)}")
    print(f"MAIN: Crawler found URLs = {len(crawler.urls_all)}")

    forum_threads = pd.DataFrame({'URL': thread_url, 'Thread': thread_name} for thread_url, thread_name in crawler.forum_threads.items())
    print(forum_threads.shape)
    forum_threads.to_csv(f'Forum_Threads--{crawler.dataset_name}.csv', sep='\t', encoding='utf-8')

    forum_topics = pd.DataFrame({'URL': topic_url, 'Thread': topic_name} for topic_url, topic_name in crawler.threads_topics.items())
    print(forum_topics.shape)
    forum_topics.to_csv(f'Forum_Topics--{crawler.dataset_name}.csv', sep='\t', encoding='utf-8')
    