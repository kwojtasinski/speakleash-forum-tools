"""
Manifest Manager Module

<Opis>

Key Features:

Classes:

Dependencies:

"""
import json
import logging

from speakleash_forum_tools.src.config_manager import ConfigManager

class ManifestManager:
    def __init__(self, config_manager: ConfigManager):
        """
        
        """
        pass

    def create_manifest(file_name_manifest: str, total_docs: int, file_size: int) -> bool:
        """
        Prepare manifest for dataset.

        :param file_name_manifest (str): Manifest desire filename.
        :param total_docs (int): Total number of documents in dataset file ( *.jsonl.zst ).
        :param file_size (int): Size (in bytes or total length of all documents in archive).

        :return: True or False if everything was OK.
        """

        # Placeholder values, will be updated in postprocessing
        total_len = 0
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
        except Exception as e:
            logging.error(f"MANIFEST // Error while creating manifest!!! | Error: {str(e)}")
            return False

        return True 
