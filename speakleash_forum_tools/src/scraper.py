"""
Scraper Module

<>

Classes:

Functions:

Dependencies:

"""
import time
import logging
import requests
import urllib3
from typing import Optional, Union, List
from multiprocessing import set_start_method, Pool, current_process

from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)     # Supress warning about 'InsecureRequest' (session.get(..., verify = False))


class Scraper:
    """
    Scraper Class
    """

    def __init__(self) -> None:
        pass


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
                        time.sleep(TIME_SLEEP)
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
            with Pool(initializer = initialize_worker,
                      initargs = [DATASET_URL, visited_links['urls'].values.tolist()],
                      processes = PROCESSES) as pool:

                time_loop_start = time.time()

                try:
                    # Issue tasks to the process pool for remaining URLs
                    for txt, meta in pool.imap(func = process_item, 
                                                    iterable = filtered_forum_urls['urls'],
                                                    chunksize = 1) :
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
