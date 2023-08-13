""" Python Script for Scraping Invision Forums """
""" This script runs as follow:
    0) Initialization of Archive folder: ./data_{DATASET_NAME}
    1) Checking for files with URLs to scrape and visited URLs (CSV files: 'urls','visited','skip')
        - if not found create DataFrame of URLs from sitemaps using ultimate-sitemap-parser library
        - always drop duplicates [func: files_check()]
    2) Removing rows that have already been processed [func: scrap_txt_mp()]
    3) Sleep for 5 sec and Starting Multiprocessing Pool (with PROCESSES number) [func: scrap_txt_mp()]
    4) Each worker starts with 'initialize_worker' function and logs what he is doing [func: scrap_txt_mp() -> initialize_worker()]
    5) Pool.imap manages workers - each worker process URL with 'process_item' [func: scrap_txt_mp() -> process_item()]
    6) In 'process_item' each URL is pass to 'get_item_text' [func: process_item() -> get_item_text()]
    7) In 'get_item_text' each URL is checked if the response is OK, scrape "commentContent" 
        (and iterate over subpages for the topic - check if subpage got the topic URL) - return text [func: get_item_text() -> process_item()]
    8) Back in 'process_item', after scraping all text from the topic URL 
        function create metadata for document: {'url' : url, 'length': len(txt_strip)}, and return text and metadata [func: process_item() -> scrap_txt_mp()]
    9) Now the text is checked if is worth to be added as a document (len(txt) > MIN_LEN_TXT) - if yes then adding it to the archive (and temp flags are set)
        if not - only temp flags are set [func: scrap_txt_mp()]
    10) Between every checkpoint temp dataframe with visited URLs is cleared and new data is added 
        (visited_urls_dataframe contains: urls, if url was visited, if url was skipped) [func: scrap_txt_mp()]
    11) Script got checkpoint (aka SAVE_STATE - number of processed URLs) to save scraping progress [func: scrap_txt_mp()]
        - show info about progress, 
        - save visited URLs as dataframe to CSV file
        - show elapsed time, avg performance from starting point and ETA time (time to finish)
    12) After scraping everything is saved (archive commit + visited URLs) and returns number of total documents [func: scrap_txt_mp() -> main()]
    13) If something went wrong while scraping -> every error / warning is in log -> but for convinience sys.exit() is used [func: main()]
    14) In folder ./data_{DATASET_NAME} we got archive chunks (to save progress if something goes wrong) so it should be merged -> merge_archive() [func: main()]
    15) Merge archive is as follows: [func: merge_archive()]
        - new archive folder is created: ./archive_merged_{DATASET_NAME}
        - search for .zst files in ./data_{DATASET_NAME}
        - process every chunk (.zst file) and stream data to one merged archive - check for duplicates, so it can be slow (you should see a nice progress bar (tqdm library))
        - check created (last), merged archive for a total number of documents (info is in logs)
        - return filename of the merged archive, total number of documents, total number of characters in all documents
    16) New merged archive is renamed and pushed to the main folder, new filename: <time-of-creation> + DATASET_NAME + '.jsonl.zst' [func: main()]
    17) Last step is creating basic dataset manifest: DATASET_NAME + '.manifest' [func: main() -> create_manifest()]
    18) The dataset file (*.jsonl.zst) and manifest should be in main folder

    + If you want to check everything or debug the code: set level=logging.DEBUG and search for some commented logging 
    + If something went wrong and some processes were not killed properly or for other reasons:
        - kill it with fire - if CTRL+C not working -> CTRL+BREAK (https://stackoverflow.com/questions/42039231/ctrl-c-for-quitting-python-in-powershell-now-not-working)
        - kill terminal (where python was running)

"""
""" Tested on Python 3.10.7 / 3.10.12 on Windows & Linux(WSL2) (08.2023)"""

# IMPORTS
import os
import glob
import json
import urllib.request, urllib.parse, urllib.robotparser
import time, datetime
import logging
import sys
from multiprocessing import set_start_method, Pool, current_process

import requests
from requests.adapters import HTTPAdapter           # install requests
from urllib3.util.retry import Retry                # install urllib3
from bs4 import BeautifulSoup                       # install beautifulsoup4
from usp.tree import sitemap_tree_for_homepage      # install ultimate-sitemap-parser
from lm_dataformat import Archive, Reader           # install lm-dataformat
from tqdm import tqdm                               # install tqdm
import pandas                                       # install pandas
import psutil                                       # install psutil



# TODO!: Merged *.jsonl.zst - tweak what should be in final archive

# TODO 1: Create generator to iterate over file (csv) with links --> to minimize memory footprint
# TODO 1: - for now: pandas.DataFrame with urls (in memory) is duplicated and split for each worker, 
# TODO 1: --> so we got x2 mem of this dataframe ('cos each subprocess got a chunk) + memory for workers

# TODO 2: Each worker could export dict with: url, flag_visited, flag_skipped - for now its tricky process



# CONFIG
DATASET_CATEGORY = "Forum" # obviously :)
DATASET_NAME = "dataset_name" # for example: forum_website_pl_corpus
DATASET_DESCRIPTION = f"Collection of forum discussions from WEBSITE-NAME-HERE" # also change this according to the forum
LICENSE = "(c) www.forumphoto.pl" # forum's address
DATASET_URL = 'https://forumphoto.pl' # forum's address with http/https
EXPECTED_URL_PARTS = ['/topic','/temat', '/thread'] # we're targeting the full topics to optimize the performance
PROCESSES = 4 # number of processes, from 1 up to os.cpu_count()
TIME_SLEEP = 0.2 # waiting interval between requests
SAVE_STATE = 100 # interval at which script creates a save to start from if crashed or stopped
MIN_LEN_TXT = 20 # minimal character count to consider it a text data
###

# Logging LEVELS: CRITICAL = 50 | ERROR = 40 | WARNING = 30 | INFO = 20 | DEBUG = 10 | NOTSET = 0
# Level set to INFO contains everything from level INFO and higher

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s', level=logging.INFO)
# logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO, filename = f'{DATASET_NAME}.log', encoding='utf-8')


# FUNCTIONS 
def timewrap(func):
    def innerfunc(*args, **kwargs):
        start = time.perf_counter()
        output = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"* Timing * -> Func: {func.__name__} | Time: {end-start} sec = {(end-start)/60} min")
        return output
    return innerfunc


def save_visited_urls(urls: list[str], file_name: str = f"visited_{DATASET_NAME}.txt") -> None:
    """
    Saves the visited urls to txt file to prevent visiting it again.
    
    Parameters
    ----------
    urls : list[str]
        List containing processed urls.
    
    file_name : str
        Name of txt file.
    
    """
    with open (file=file_name, mode="a") as file:
        for url in urls:
            file.write(url + "\n")
    logging.info(f"SAVE // Visited URLs saved: {len(urls)} -> {file_name}")


def save_visited_dataframe(urls: pandas.DataFrame, file_name: str = f"Visited_{DATASET_NAME}.csv", head = False, mode = 'a') -> None:
    """
    Saves the urls to txt file.
    
    Parameters
    ----------
    urls : pandas.DataFrame
        DataFrame containing processed urls (and columns with data).
    
    file_name : str
        Name of txt file.

    head : bool
        True / False - if use header in df.to_csv function.

    mode : char
        Mode to use while opening file in df.to_csv function.
    
    """
    urls.to_csv(file_name, sep='\t', header=head, mode=mode, index=False)
    logging.info(f"SAVE // Saved file -> DataFrame: {urls.shape} -> {file_name}")


def tree_sitemap(url: str):
    """
    Uses the Ulitmate Sitemap Parser's sitemap_tree_for_homepage method to get the sitemap and extract all the URLs.

    Parameters
    ----------
    url : str
        Domain URL to get sitemap.

    Returns
    -------
    tree : AbstractSitemap
        Tree of AbstractSitemap subclass objects that represent the sitemap hierarchy found on the website.
    
    """
    start_time = time.perf_counter()
    tree = sitemap_tree_for_homepage(url)
    end_time = time.perf_counter()
    logging.info(f"SITEMAP_TREE // Sitemap DONE | Time = {end_time - start_time} sec = {(end_time - start_time) / 60} min")
    return tree


def urls_generator(tree) -> list[str]:
    """
    Uses the Ulitmate Sitemap Parser's sitemap_tree_for_homepage method to get the sitemap and extract all the URLs.

    Parameters
    ----------
    tree : AbstractSitemap
        Tree with sitemaps (class AbstractSitemap).

    Returns
    -------
    list[str]
        Yielded output with urls to scrap.
    
    """
    # Extract all the URLs with EXPECTED_URL_PARTS in it 
    urls_expected: list[str] = []

    for page in tree.all_pages():
        if any(url_part in page.url for url_part in EXPECTED_URL_PARTS):
            urls_expected.append(page.url)

    logging.info(f"URL_GEN // URL Generator -> URLs_expected: {len(urls_expected)}")
    return urls_expected


def get_item_text(url: str) -> str:
    """
    Extracts text data from URL (from urls_generator).

    Parameters
    ----------
    url : str
        URL to get text data.

    Returns
    -------
    text : str
        Text data.
    
    """

    # Variables
    response = None
    text = ''
    headers = {
		'User-Agent': 'Speakleash-v0.1',
		"Accept-Encoding": "gzip, deflate",
		"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Connection": "keep-alive"
	}

    # Try to connect to a given URL
    try:
        response = session.get(url, 
                               timeout=60, 
                               headers=headers)

    # Handle connection error
    except Exception as e:
        logging.error(f"GET_TEXT // Error downloading -> {url} : {str(e)}") 

    # Connection successful
    if response and response.ok:

        # Check if the file exceeds 15 MB
        if len(response.content)>15000000:
            logging.warning(f"GET_TEXT // File too big")
            return text
        
        # Beautiful Soup to extract data from HTML
        try:
            soup = BeautifulSoup(response.content, "html.parser")
            comment_blocks = soup.find_all("div", {'data-role': "commentContent"})
        except Exception as e:
            logging.error(f"GET_TEXT // ERROR BeautifulSoup: {str(e)}")
        # Get text data from posts on page and add it to the string
        for comment in comment_blocks:
            text += comment.text.strip()+"\n"
        
        #Process next pages
        try:            
            # Sleep for convinience
            time.sleep(TIME_SLEEP)
            
            # Iterate through all of the pages in given topic/thread
            while len(soup.find_all('li', {'class': 'ipsPagination_next'})) > 0 and \
                  len(soup.find_all('li', class_='ipsPagination_next ipsPagination_inactive')) == 0:
                
                next_page_btns = soup.find_all('li', {'class': 'ipsPagination_next'})
                next_page_url = next_page_btns[0].find('a')['href']

                if url in next_page_url:
                    logging.debug(f"GET_TEXT // Found new page: {next_page_url.replace(url,'')} --> for topic: {url}")
                
                    next_page_response = session.get(next_page_url, timeout=60, headers=headers)
                    soup = BeautifulSoup(next_page_response.content, "html.parser")

                    comment_blocks = soup.find_all("div", {'data-role': "commentContent"})

                    for comment in comment_blocks:
                        text += comment.text.strip() + "\n"
                    #for i in page_nav_results:
                    #    text += i.text.strip()+"\n"
                else:
                    logging.debug(f"GET_TEXT // Topic URL is NOT in next_page_url: {next_page_url=}")
                    break

        # Handle next page error       
        except Exception as e:
            logging.error(f"GET_TEXT // ERROR processing next page: {next_page_url} : {str(e)}")           
    
    # Connection not successful - reponse empty
    elif not response:    
        logging.warning(f"GET_TEXT // Empty response -> {url}\nResponse: {response}")

    # Connection not successful - error
    elif not response.ok:
        logging.error(f"GET_TEXT // Error response -> {url}\nResponse: {response.status_code}")
    
    return text 


def process_item(url: str) -> tuple[str, dict]:
    """
    Extract from URL -> cleaning -> simple metadata.

    Parameters
    ----------
    url : str
        URL to get text data.

    Returns
    -------
    text : str
        Text data (stripped).
    
    meta : dict
        Dict with simple metadata -> {'url' : url, 'length': len(txt_strip)} or {'url' : url, 'skip': 'error' / 'robots.txt' / 'visited'}.
    """

    global rp
    global all_visited_urls
    meta: dict = {'url' : url}
    txt: str = ''
    txt_strip: str = ''

    if psutil.LINUX == True:
        logging.debug(f"PROCESS_ITEM // Proc ID: {psutil.Process().pid} | CPU Core: {psutil.Process().cpu_num()} | Processing URL: {url}")
    else:
        logging.debug(f"PROCESS_ITEM // Proc ID: {psutil.Process().pid} | Processing URL: {url}")

    if url not in all_visited_urls:
        if rp.can_fetch('*', url):
            try:
                txt = get_item_text(url)
                meta = {'url' : url, 'skip': 'error'}
                if txt:
                    txt_strip = txt.strip()
                    meta = {'url' : url, 'length': len(txt_strip)}
            except Exception as e:
                logging.error(f"PROCESS_ITEM // Error processing item -> {url} : {str(e)}")
                meta = {'url' : url, 'skip': 'error'}
        else:
            logging.info(f"PROCESS_ITEM // Robots not allowed: {url}")
            meta = {'url' : url, 'skip': 'robots.txt'}
    else:
        logging.info(f"PROCESS_ITEM // URL already visited -> skipping: {url}")
        meta = {'url' : url, 'skip': 'visited'}

    # Only on LINUX
    # logging.info(f"PROCESS_ITEM // Metadata: {meta} | CPU Core: {psutil.Process().cpu_num()}")    
    return txt_strip, meta


def initialize_worker(url: str, visited_urls: list[str]) -> None:
    """
    Initialize the workers (parser and session) for multithreading performace.

    Parameters
    ----------
    url : str
        Domain URL.

    visited_urls : list[str]
        All visited URLs - can be empty or passed from file.
    """
    if psutil.LINUX == True:
        logging.info(f'INIT_WORKER // Initializing worker... | CPU Core: {psutil.Process().cpu_num()}')
    else:
        logging.info(f'INIT_WORKER // Initializing worker... 1 of many! We Wanna Work!')
    
    global rp
    global session
    global all_visited_urls
    all_visited_urls = visited_urls
    
    rp = urllib.robotparser.RobotFileParser()    
    with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'Python'})) as response:
        rp.parse(response.read().decode("utf-8").splitlines())
    
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=3)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    if psutil.LINUX == True:
        logging.info(f"INIT_WORKER // Created: RobotFileParser & requests.Session | CPU Core: {psutil.Process().cpu_num()}")
    else:
        logging.info(f"INIT_WORKER // Created: RobotFileParser & requests.Session - 1 of many")


def files_check(urls_filename: str = f"Forum_URLs_{DATASET_NAME}.csv", visited_filename: str = f"Visited_{DATASET_NAME}.csv") -> tuple[pandas.DataFrame, pandas.DataFrame]:
    """
    Checking if exists files with:
    1) forum urls - if not create sitemaps tree -> generate urls -> save to file.
    2) visited urls - if not create empty file.

    Parameters
    ----------
    urls_filename : str
        Filename for CSV file with generated urls from sitemaps tree --> 3 columns = ['urls', 'visited', 'skip'] (sep = '\t').
    
    visited_filename : str
        Filename for CSV file with visited urls --> 3 columns = ['urls', 'visited', 'skip'] (sep = '\t').

    Returns
    -------
    forum_links : pandas.DataFrame
        DataFrame (pandas) with urls generated from sitemaps tree.

    visited_links : pandas.DataFrame
        DataFrame (pandas) with visited urls and info if visited (1) and skipped (0 or 1).
    """
    sitemap_tree = []
    forum_links = pandas.DataFrame(columns=['urls','visited','skip'])
    visited_links = pandas.DataFrame(columns=['urls','visited','skip'])
    flag_fresh_start = False
    
    # Check if file with URLs from sitemaps exist, if not search for sitemaps and return links
    if os.path.exists(path = urls_filename):
        logging.info(f"*** Resuming from previous progress... ***")
        # Read the saved URLs from the file
        logging.info(f"FILE_CHECK // Importing DataFrame for: {DATASET_URL} ...")
        forum_links = pandas.read_csv(urls_filename, sep = '\t', header = None, names = ['urls','visited','skip'])
        logging.info(f"FILE_CHECK // Imported DataFrame for: {DATASET_URL} | Shape: {forum_links.shape} | Size in memory (MB): {forum_links.memory_usage(deep=True).sum() / pow(10,6)}")
    else:
        logging.info(f"*** Starting from scratch... ***")
        # Create sitemap tree for domain url
        logging.info(f"FILE_CHECK // Creating sitemaps tree...")
        sitemap_tree = tree_sitemap(DATASET_URL)
        logging.info(f"FILE_CHECK // Created sitemaps tree for: {DATASET_URL}")

        # Create DataFrame with URLs + columns: 'visited','skip'
        forum_links = pandas.DataFrame(urls_generator(sitemap_tree), columns=['urls'])
        if forum_links.shape[0] == 0:
            logging.error(f"FILE_CHECK + SITEMAP // No valid URL found in SITEMAPs -> Check it manually ({forum_links.shape=})")
            raise ValueError("No valid URL found in SITEMAPs.")
        forum_links.drop_duplicates(inplace=True, ignore_index=True)
        forum_links['visited'] = 0
        forum_links['skip'] = 0
        logging.info(f"FILE_CHECK // Created DataFrame for: {DATASET_URL} | Shape: {forum_links.shape} | Size in memory (MB): {forum_links.memory_usage(deep=True).sum() / pow(10,6)}")
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


def url_pandas_lazy_gen(urls_dataframe: pandas.DataFrame, colname: str = 'urls'):
    for idx, row in urls_dataframe.iterrows():
        yield row[colname]


def scrap_txt_mp(ar: Archive) -> int:
    """
    Extract text data from URL using multiprocessing. 
    Init -> MP Pool -> Extract -> Save URLs and update Archive.

    Parameters
    ----------
    ar : Archive
        Archive class from library lm_dataformat.
    
    fresh_start : bool
        Determine if file with visited urls exist (False) or not (True).

    Returns
    -------
    total_docs : int
        Total number of added documents.
    """
    
    # Check if previous progress exists
    # Check if exists files with: 1) forum urls, 2) visited urls
    filename_forum_urls: str = f"Forum_URLs_{DATASET_NAME}.csv"
    filename_visited: str = f"Visited_{DATASET_NAME}.csv"
    forum_links = pandas.DataFrame(columns=['urls','visited','skip'])
    visited_links = pandas.DataFrame(columns=['urls','visited','skip'])

    try:
        forum_links, visited_links = files_check(urls_filename = filename_forum_urls, visited_filename = filename_visited)
    except Exception as e:
        logging.error(f"*** ERROR *** Something wrong with CSV files: {str(e)}")
        raise e
    
    logging.info(f"Forum links duplicated: {forum_links['urls'].duplicated().sum()}")
    forum_links.drop_duplicates(inplace=True, ignore_index=True)

    logging.info(f"Visited links duplicated: {visited_links['urls'].duplicated().sum()}")
    visited_links.drop_duplicates(inplace=True, ignore_index=True)

    filtered_forum_urls = forum_links[~forum_links['urls'].isin(visited_links['urls'])]
    len_urls_left = int(filtered_forum_urls.shape[0])
    logging.info(f"SCRAPE // *** *** ***")
    logging.info(f"SCRAPE // URLs to check: {len_urls_left}")

    total_docs: int = (visited_links['visited'].sum() - visited_links['skip'].sum())
    total_visited: int = visited_links['visited'].sum()


    if len_urls_left and len_urls_left != 0:
        logging.info(f"SCRAPE // Start scraping... in 5 sec")
        time.sleep(5)

        # Temp values, Placeholders will be updated in postprocessing
        added: int = 0
        skipped: int = 0                 # Will be checked if visited -> in pool
        total: int = 0
        visited_urls_dataframe = pandas.DataFrame(columns=['urls','visited','skip'])
        time_loop_start = time.time()
        total_checkpoint = 0
        added_checkpoint = 0
        skipped_checkpoint = 0

        # Create and configure the process pool
        logging.info(f"SCRAPE // Starting Multiprocessing Pool...")
        with Pool(initializer=initialize_worker,
                  initargs=[DATASET_URL, visited_links['urls'].values.tolist()],
                  processes=PROCESSES) as pool:

            time_loop_start = time.time()

            try:
                # Issue tasks to the process pool for remaining URLs
                for txt, meta in pool.imap(func = process_item, 
                                                # iterable = urls_generator(sitemap_tree),
                                                iterable = filtered_forum_urls['urls'],
                                                # iterable = url_pandas_lazy_gen(filtered_forum_urls, colname='urls'),      # Got more RAM allocated
                                                chunksize = 1)  :
                                                # ,total = len_urls_left, leave = False, ):
                    total += 1
                    flag_visited: int = 0
                    flag_skip: int = 0
                    visit_temp: dict = {}

                    if txt and len(txt) > MIN_LEN_TXT:
                        total_docs += 1
                        ar.add_data(txt, meta = meta)
                        added += 1
                        flag_visited = 1
                        flag_skip = 0
                        visit_temp = {'urls': [meta.get('url')], 'visited': [flag_visited], 'skip': [flag_skip]}
                        # logging.info(f"SCRAPE // OK --- Processed: {total} | Added counter: {added} | Len(txt): {meta.get('length')} | Added URL: {meta.get('url')}")
                    else:
                        skipped += 1
                        flag_visited = 1
                        flag_skip = 1
                        if meta.get('skip') != 'visited':
                            visit_temp = {'urls': [meta.get('url')], 'visited': [flag_visited], 'skip': [flag_skip]}
                        # logging.info(f"SCRAPE // Short or empty TXT --- Processed: {total} | Skipped counter: {skipped} | Why skipped: {meta.get('skip')} | Skipped URL: {meta.get('url')}")

                    if visit_temp:
                        # print(f"VISIT_TEMP EXIST ---> {visit_temp}")
                        visited_urls_dataframe = pandas.concat([visited_urls_dataframe, pandas.DataFrame(visit_temp)], ignore_index=True)

                    if len(pool._pool) != PROCESSES:
                        logging.error(f"*** ERROR *** Ups, something went wrong --> pool got: {len(pool._pool)} workers, should be {PROCESSES}")

                    # Save visited URLs to file
                    if total % SAVE_STATE == 0 and added > 0:
                        logging.info(f"SCRAPE // Scraping info --> Checked URLs: {total_visited + total} | Added docs: {total_docs}")
                        logging.info(f"SCRAPE // This session --> Checked URLs: {total} | Added: {added}  | Skipped: {skipped}")
                        logging.info(f"SCRAPE // Since last checkpoint --> Checked URLs: {total-total_checkpoint} | Added: {added-added_checkpoint}  | Skipped: {skipped-skipped_checkpoint}")
                        total_checkpoint = total
                        added_checkpoint = added
                        skipped_checkpoint = skipped

                        # logging.info(f"SCRAPE // Saving visited URLs to file, visited: {visited_urls_dataframe.shape[0]}")
                        save_visited_dataframe(visited_urls_dataframe)
                        visited_urls_dataframe = pandas.DataFrame(columns=['urls','visited','skip'])

                        ar.commit()
                        logging.info(f"SCRAPE + SAVE // Commiting to Archive, total commited = {added}")
                        # logging.info("SCRAPE // Commited to Archive - DONE | Visited URLs saved - DONE")

                        time_loop_end = time.time()
                        time_since_start = time_loop_end - time_loop_start + 1e-9
                        remains_iter = len_urls_left - total
                        time_per_iter = time_since_start / (total+1)
                        time_eta = remains_iter * time_per_iter
                        logging.info(f"SCRAPE + TIMING // *** Time since start: {(time_since_start / 60):.2f} min | {(time_since_start / 3600):.2f} h | {(time_since_start / 86400):.2f} days")
                        logging.info(f"SCRAPE + TIMING // *** Performance: {(total / time_since_start):.2f} ops/sec  |  Processes = {len(pool._pool)} / {PROCESSES}")
                        logging.info(f"SCRAPE + TIMING // *** ETA: {(time_eta / 60):.2f} min | {(time_eta / 3600):.2f} hours | {(time_eta / 86400):.2f} days --> {(datetime.datetime.today() + datetime.timedelta(seconds=time_eta)).strftime('%Y-%m-%d %H:%M (%A)')}")
                        logging.info(f"SCRAPE // ------------------------------------------------------------------- ")

            except Exception as e:
                logging.error(f"*** ERROR *** --> {str(e)}")

            logging.info(f"SCRAPE // Scraping DONE! --> Checked URLs: {total_visited + total} | Added docs: {total_docs} ||| This session --> Checked URLs: {total} | Added: {added}  | Skipped: {skipped}")

            # Saving Archive and visited URLs
            ar.commit()
            save_visited_dataframe(visited_urls_dataframe)
            logging.info(f"SCRAPE // Saved URLs and Archive - DONE!")
    else:
        logging.info(f"SCRAPE // Nothing to scrape...")
    
    return total_docs


def merge_archive() -> tuple[str, int, int]:
    """
    Create new Archive in: Archive_Merged_{DATASET_NAME}.
    Merge all existing archive chunks into one file, checking duplicates along the way.

    Parameters
    ----------
    None

    Returns
    -------
    data_merge[-1] : str
        Filename with prepared merged archive (last in folder).

    len_archive_merged : int
        Number of documents in archive after merging process is complete.

    total_docs_len : int
        Total length of documents in finale merged archive.
    """

    start_time = time.perf_counter()

    logging.info(f"MERGE_ARCH // Merging Archives in: {DATASET_NAME}")

    # merged_file_path = f"./{DATASET_NAME}_merged.jsonl.zst"
    merged_file_path: str = f"./archive_merged_{DATASET_NAME}"
    ar_merge = Archive(merged_file_path)

    # Re-write chunks of archives to 1 merged
    data_files = glob.glob(f'./data_{DATASET_NAME}/*.zst')

    urls_visited = []
    urls_duplicated = 0
    total_docs = 0
    total_docs_len = 0

    for file_path in tqdm(data_files):
        arch_part = Reader(file_path)
        for id, record in enumerate(arch_part.stream_data(get_meta = True)):
            urel = record[1].get('url')
            if urel not in urls_visited:
                urls_visited.append(urel)
                ar_merge.add_data(data = record[0], meta = record[1])
                total_docs += 1
                total_docs_len += record[1].get('length')
            else:
                urls_duplicated += 1
    ar_merge.commit()

    logging.info(f"MERGE_ARCH // Archive MERGED --> Total docs: {total_docs} | Duplicated: {urls_duplicated}")

    # Read merged archive
    data_merge = glob.glob(f'{merged_file_path}/*.zst')
    data_merge.sort()
    len_archive_merged = 0
    ar_merge_reader = Reader(merged_file_path)

    # archive_check = pandas.DataFrame(columns=['doc_len', 'meta_len_doc', 'meta_url'])
    
    for id, doc in enumerate(ar_merge_reader.read_jsonl_zst(data_merge[-1], get_meta=True)):
        # if id in [1,2,3,4,5]:
        #     print(f"ID: {id}")
        #     print(doc[0])
        #     print(doc[1])
        #     print('\n')

        # archive_check = pandas.concat([archive_check, pandas.DataFrame({'doc_len': [len(doc[0])], 'meta_len_doc': [doc[1].get('length')], 'meta_url': [doc[1].get('url')]})], ignore_index=True)
        len_archive_merged = id

    len_archive_merged = len_archive_merged + 1
    logging.info(f"MERGE_ARCH // Checked Archive --> joined - DONE! | Docs: {len_archive_merged} | File: {data_merge[-1]}")

    # archive_check_urls_duplicated = archive_check['meta_url'].duplicated().sum()
    # 
    # archive_check_miss_length = 0
    # for x in archive_check:
    #     if x['doc_len'] == x['meta_len_doc']:
    #         archive_check_miss_length += 1

    # logging.info(f"Archive info --> Duplicated URLs: {archive_check_urls_duplicated} | Wrong length: {archive_check_miss_length}")
    # print(archive_check.describe())

    end_time = time.perf_counter()
    logging.info(f"MERGE_ARCH // Archive merged and checked - DONE! | Time = {(end_time-start_time):.2f} sec = {((end_time-start_time) / 60):.2f} min")
    return data_merge[-1], len_archive_merged, total_docs_len


def create_manifest(file_name_manifest: str, total_docs: int, file_size: int) -> bool:
    """
    Prepare manifest for dataset.

    Parameters
    ----------
    file_name_manifest : str
        Manifest desire filename.
    
    total_docs : int
        Total number of documents in .
    
    file_size : int
        Size (in bytes or total length of all documents in archive).

    Returns
    -------
    bool
        True or False if everything was OK.
    """

    start_time = time.perf_counter()

    # Placeholder values, will be updated in postprocessing
    total_len = 0
    total_docs = 0
    total_sentences = 0
    total_words = 0
    total_verbs = 0
    total_nouns = 0
    total_punctuations = 0
    total_symbols = 0
    total_stopwords = 0
    total_oovs = 0

    try:
        manifest = {"project" : "SpeakLeash", 
                        "name": DATASET_NAME, 
                        "description": DATASET_DESCRIPTION, 
                        "license": LICENSE, 
                        "category": DATASET_CATEGORY, 
                        "language": "pl", 
                        "file_size" : str(file_size),
                        "sources": [{"name": DATASET_NAME, 
                                    "url": DATASET_URL, 
                                    "license": LICENSE}], 
                                    "stats": {"documents": total_docs, 
                                        "sentences": total_sentences, 
                                        "words" : total_words, 
                                        "nouns" : total_nouns, 
                                        "verbs" : total_verbs, 
                                        "characters": total_len, 
                                        "punctuations" : total_punctuations, 
                                        "symbols" : total_symbols, 
                                        "stopwords": total_stopwords, 
                                        "oovs": total_oovs}}

        try:
            json_manifest = json.dumps(manifest, indent = 4)
        except Exception as e:
            logging.error(f"MANIFEST // Error while json.dumps: {str(e)}")
            return e

        try:
            with open(file_name_manifest, 'w') as mf:
                mf.write(json_manifest)
        except Exception as e:
            logging.error(f"MANIFEST // Error while writing json file: {str(e)}")
            return e

        end_time = time.perf_counter()
        logging.info(f"MANIFEST // Manifest created - DONE! | Time = {(end_time-start_time):.2f} sec")
        return True 

    except Exception as e:
        logging.error(f"MANIFEST // Error while creating manifest!!! | Error: {str(e)}")
        return False


def main():

    start_time = time.perf_counter()
    logging.info(f"*** STARTING -> Main function ***")

    # Create .jsonl.zst and .manifest files for the dataset
    file_name_zst: str = DATASET_NAME + '.jsonl.zst'
    file_name_manifest: str = DATASET_NAME + '.manifest'

    # Initialize the Archive
    ar = Archive(f'./data_{DATASET_NAME}')
    total_docs: int = 0

    # Check for progress --> scrape data 
    try:
        total_docs = scrap_txt_mp(ar = ar)
    except Exception as e:
        logging.info(f"*** Error while scraping... | Error: {str(e)} ***")
        sys.exit()
    logging.info(f"*** Scraping is done -> preparing merge archives ***")

    # Merge Archives
    file_name_temp_zst: str = ''
    total_docs_archive: int = 0
    total_docs_len: int = 0

    try:
        file_name_temp_zst, total_docs_archive, total_docs_len = merge_archive()
        new_name_zst = datetime.datetime.today().strftime('%Y%m%d%H%M%S') + '-' + file_name_zst
        os.rename(file_name_temp_zst, new_name_zst)
        file_name_temp_zst = new_name_zst

        if file_name_temp_zst and os.path.exists(file_name_temp_zst):
            logging.info(f"*** Archive SAVED as file: {file_name_temp_zst}")
    except Exception as e:
        logging.error(f"*** Error while mergind archives: {str(e)}")
    logging.info(f"*** Archives merge is done -> preparing dataset manifest... ***")

    if total_docs == total_docs_archive:
        logging.info(f"* Total number of documents was checked: {total_docs=} == {total_docs_archive=}")
    else:
        logging.warning(f"*** Total number of documents was checked: {total_docs=} != {total_docs_archive=} --> using {total_docs_archive=}")

    # Prepare Manifest
    file_size: int = total_docs_len

    if create_manifest(file_name_manifest, total_docs_archive, file_size) == False:
        logging.error(f"*** Error while creating manifest ***")

    # End work
    end_time = time.perf_counter()
    time_work = end_time-start_time
    logging.info(f"*** Work is finished! | Time = {time_work:.2f} sec = {(time_work / 60):.2f} min = {(time_work / 3600):.2f} h ***")

    return True


if __name__ == '__main__':

    set_start_method("spawn")
    if main() == True:
        logging.info(f"--- EVERYTHING WAS FINE +++")
    else:
        logging.warning(f"--- UPS SOMETHING WENT WRONG +++")
