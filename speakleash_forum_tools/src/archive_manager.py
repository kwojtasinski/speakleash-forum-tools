"""
Archive Manager Module

This module offers the ArchiveManager class, which is essential for managing data archiving in 
the context of a forum crawling and scraping tool. The class is specifically designed to handle 
various file management tasks related to the archival process of scraped data, including the 
creation of folders, merging of data chunks, and maintaining records of visited URLs.

The ArchiveManager class aims to streamline the archiving aspect of the web scraping process, 
providing a robust and scalable solution for handling large volumes of data in an organized manner.

Key Features:
- Efficient management of archive directories, ensuring organized storage of scraped data.
- Creation and maintenance of visited URLs file, aiding in tracking the progress of the scraping process.
- Merging functionality for combining multiple archive chunks into a single, consolidated file.
- Support for creating empty template files for storing scraped data, enhancing data management efficiency.
- Utilization of the 'lm-dataformat' library for handling JSONL.ZST file format, ensuring high compression and fast access.

Classes:
- ArchiveManager: The primary class in this module, responsible for various archiving operations. 
  It initializes with dataset names and paths, manages temporary and merged archive folders, and provides 
  functionalities for adding URLs to visited files, merging archives, and creating empty files for future data storage.

Dependencies:
- pandas: Used for data manipulation and CSV file operations.
- os, glob, tqdm: Utilized for file system interactions and progress tracking.
- logging: For logging and monitoring the archiving process.
- lm-dataformat: For handling and managing archive formats such as JSONL.ZST .

"""
import os
import glob
import logging
from typing import Tuple

import pandas
from lm_dataformat import Archive, Reader
from tqdm import tqdm

class ArchiveManager:
    """
    ArchiveManager class is to manage:
    1) creating folders:
        1.a) temp folder - for scraper archives chunks 
    2) functions:
        2.a) __init__ - init folders / Archive class
        2.b) add_to_visited_file
        2.c) merge archives after scraping
    3) prepare Archive (with path to specific folder)
    """
    def __init__(self, dataset_name: str, dataset_folder: str, visited_filename: str):
        self.dataset_zst_filename = dataset_name + '.jsonl.zst'
        self.dataset_name = dataset_name
        self.dataset_folder = dataset_folder
        self.temp_data_path = os.path.join(self.dataset_folder, 'temp_scraper_data')
        self.merged_archive_path = os.path.join(self.dataset_folder, 'archive_merged-JSONL_ZST')
        
        self.visited_filename = visited_filename            # File with visited URLs
        # self.create_visited_empty_file()                    # Important - Create file for visited URLs

        self._create_archive_folder()                       # Create folder for Archive (temp_scraper_data)
        self.archive = Archive(self.temp_data_path)         # Archive - manager for temporary chunks of archives
        

    def _create_archive_folder(self) -> None:
        """
        Create the temp archive folder -> 'temp_scraper_data' (if it doesn't exist).
        """
        try:
            if not os.path.exists(self.temp_data_path):
                os.makedirs(self.temp_data_path)
                logging.info(f"Archive // Created archive folder at {self.temp_data_path}")
        except Exception as e:
            logging.error(f"Archive // Error while checking or creating folder for 'temp_scraper_data' -> {e}")

    def add_to_visited_file(self, urls_dataframe: pandas.DataFrame, file_name: str = "Visited_.csv", head = False, mode = 'a') -> None:
        """
        Append visited URLs to CSV file (in dataset folder).

        :param urls_dataframe (pandas.DataFrame): DataFrame containing processed URLs (and other columns).
        :param file_name (str): Name of CSV file.
        :param head (bool): True / False - if use header in df.to_csv function.
        :param mode (char): Mode to use while opening file in df.to_csv function.
        """
        urls_dataframe.to_csv(os.path.join(self.dataset_folder, file_name), sep='\t', header=head, mode=mode, index=False, encoding='utf-8')
        logging.info(f"Archive // Saved file -> DataFrame: {urls_dataframe.shape} -> {file_name}")

    def create_empty_file(self, urls_dataframe: pandas.DataFrame, file_name: str) -> None:
        """
        Create empty CSV file (in dataset folder) for given DataFrame 
        - use DataFrame to write columns names to file.

        :param urls_dataframe (pandas.DataFrame): DataFrame containing processed URLs (and other columns).
        :param file_name (str): Name of CSV file.
        """
        if os.path.exists(os.path.join(self.dataset_folder, file_name)):
            logging.info("Archive // File with visited URLs exist")
        else:
            logging.info("Archive // File with visited URLs don't exist - creating new file")
            self.add_to_visited_file(urls_dataframe = urls_dataframe, file_name = file_name, head = True, mode = 'w')

    def merge_archives(self) -> Tuple[str, int, int]:
        """
        Merge all .zst archive files in the dataset folder into one.

        :return: Tuple containing the path to the merged archive, number of documents,
          and total number of characters across all documents.
        """
        merged_file_path = os.path.join(self.merged_archive_path, f"{self.dataset_name}.jsonl.zst")
        ar_merge = Archive(merged_file_path)

        # Find all .zst files in the temp_scraper_data directory
        data_files = glob.glob(os.path.join(self.temp_data_path, '*.zst'))
        urls_visited = []
        urls_duplicated = 0
        total_docs = 0
        total_chars = 0

        # Re-packing chunks of archive to 1 output file
        for file_path in tqdm(data_files):
            arch_part = Reader(file_path)
            for id, record in enumerate(arch_part.stream_data(get_meta = True)):
                urel = record[1].get('url')
                if urel not in urls_visited:
                    urls_visited.append(urel)
                    ar_merge.add_data(data = record[0], meta = record[1])
                    total_docs += 1
                    total_chars += record[1].get('length')
                else:
                    urls_duplicated += 1
        ar_merge.commit()
        logging.info(f"* Merged {total_docs} documents with a total of {total_chars} characters | Duplicated: {urls_duplicated}")

        # Read merged archive - check if everything is okey
        try:
            data_merge = glob.glob(f'{merged_file_path}/*.zst')
            data_merge.sort()
            if not data_merge[-1]:
                logging.error("Archive // Error! Can't find merged file -> *.jsonl.zst")
                return "", 0, 0
            len_archive_merged = 0
            ar_merge_reader = Reader(merged_file_path)

            # Check number of documents
            for id, doc in enumerate(ar_merge_reader.read_jsonl_zst(data_merge[-1], get_meta=True)):
                len_archive_merged = id
            len_archive_merged = len_archive_merged + 1
        except Exception as e:
            logging.error(f"Archive // Error while checking merged Archive: {e}")

        # Last check everything was okey
        if len_archive_merged == total_docs:
            logging.info(f"Archive // Checked Archive --> joined - DONE! | Docs: {len_archive_merged} | File: {data_merge[-1]}")
        else:
            logging.error(f"Archive // Error! Length of merged Archive is different! -> {total_docs=} != {len_archive_merged=}")

        return data_merge[-1], total_docs, total_chars
