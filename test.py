import pandas as pd

from speakleash_forum_tools import ConfigManager, CrawlerManager


# Example usage:
if __name__ == "__main__":
    print("*****************")

    ### Engine: invision
    #+ config_manager = ConfigManager()      # Default: forum.szajbajk.pl -> invision                           # Topics to find = 18k (from dashboard) == almost all
    config_manager = ConfigManager(dataset_url='https://max3d.pl/forums/', forum_engine='invision')          # Topics to find = 85.5k (from dashboard) == to big for testing manual crawler

    ### Engine: phpBB 
    #+ config_manager = ConfigManager(dataset_url="https://forum.prawojazdy.com.pl", forum_engine='phpbb')      # Topics to find = 21 669 (calc from website) == almost all
    #+ config_manager = ConfigManager(dataset_url="https://www.gry-planszowe.pl", forum_engine='phpbb')         # Topics to find = 24 596 (calc from website) == almost all
    #+ config_manager = ConfigManager(dataset_url="https://www.excelforum.pl", forum_engine='phpbb')            # Topics to find = 58 414 (calc from website) == 48k found

    ### Engine: ipboard
    #+ config_manager = ConfigManager(dataset_url="https://forum.omegaklub.eu", forum_engine='ipboard')         # Disconnect for no reason
    #+ config_manager = ConfigManager(dataset_url="http://forum-kulturystyka.pl", forum_engine='ipboard')       # Topics to find = 110k ~ 119k (calc ~ dashboard)

    ### Engine: xenforo
    # https://forum.modelarstwo.info --> Crawl-delay = 15sec (in robots.txt)
    #+ config_manager = ConfigManager(dataset_url="https://forum.modelarstwo.info", forum_engine='xenforo')     # Topics to find = 16 500 (calc from website) == almost all


    # Use the settings from config_manager.settings as needed
    crawler = CrawlerManager(config_manager)
