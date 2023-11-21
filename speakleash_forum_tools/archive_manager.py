# archive_manager.py
import os
import glob
from lm_dataformat import Archive, Reader
from tqdm import tqdm
import logging

class ArchiveManager:
    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.data_path = f'./data_{dataset_name}'
        self.merged_archive_path = f"./archive_merged_{dataset_name}"
        self.archive = Archive(self.data_path)

    def create_archive_folder(self):
        """Create the archive folder if it doesn't exist."""
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
            logging.info(f"Created archive folder at {self.data_path}")

    def save_visited_urls(self, urls, file_name):
        """
        Saves the visited URLs to a text file to prevent revisiting.

        Parameters:
        - urls (list): A list of URLs that have been processed.
        - file_name (str): The name of the file to save the URLs to.
        """
        file_path = os.path.join(self.data_path, file_name)
        with open(file_path, 'a') as file:
            for url in urls:
                file.write(url + "\n")
        logging.info(f"Saved visited URLs to {file_path}")

    def merge_archives(self):
        """
        Merge all .zst archive files in the dataset folder into one.

        Returns:
        - A tuple containing the path to the merged archive, number of documents,
          and total number of characters across all documents.
        """
        self.create_archive_folder()  # Ensure the archive folder exists
        merged_file_path = os.path.join(self.merged_archive_path, f"{self.dataset_name}.jsonl.zst")
        ar_merge = Archive(merged_file_path)

        # Find all .zst files in the data directory
        data_files = glob.glob(os.path.join(self.data_path, '*.zst'))
        urls_visited = set()
        total_docs = 0
        total_chars = 0

        for file_path in tqdm(data_files, desc="Merging archives"):
            with Reader(file_path) as reader:
                for record in reader.stream_data(get_meta=True):
                    url, data, meta = record['url'], record['text'], record['meta']
                    if url not in urls_visited:
                        urls_visited.add(url)
                        ar_merge.add_data(data, meta=meta)
                        total_docs += 1
                        total_chars += len(data)

        ar_merge.commit()
        logging.info(f"Merged {total_docs} documents with a total of {total_chars} characters into {merged_file_path}")
        return merged_file_path, total_docs, total_chars

    # Additional methods can be added here as needed for your application.
