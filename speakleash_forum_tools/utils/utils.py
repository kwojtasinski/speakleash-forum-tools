"""
Utility Module

Provides funcions for other modules.
"""
import requests
from requests.adapters import HTTPAdapter           # install requests
from urllib3.util.retry import Retry                # install urllib3
from typing import Optional, Union

from speakleash_forum_tools.__version__ import __version__


def create_session(retry_total: Optional[Union[bool, int]] = 3, retry_backoff_factor: float = 3.0, verify: bool = False) -> requests.Session:
    """
    Creates and configures a new session with retry logic for HTTP requests.

    This function initializes a `requests.Session` object and sets up a retry mechanism
    for failed requests. It configures the session to retry up to three times with a
    backoff factor to control the delay between retries. The session is equipped to handle
    both HTTP and HTTPS requests.

    The function also ensures that SSL certificate verification is disable for the session.

    :return (requests.Session): A configured session object with retry logic.
    :rtype: requests.Session
    """
    session = requests.Session()
    retry = Retry(total = retry_total, backoff_factor = retry_backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.verify = verify
    return session


def check_for_updates() -> bool:
    """
    Checks for the availability of new updates for the package.

    This function queries a specified online source to determine the latest available version 
    of the package. It then compares this version with the package's current version. If a newer 
    version is available, it notifies the user about the update.

    The function queries the package information from PyPI (or another specified source) and extracts 
    the version number from the response. It assumes that the package follows semantic versioning.

    :return (bool): Return True and outputs a message to the console if an update is available.
    """
    current_version = __version__
    response = requests.get('https://pypi.org/pypi/speakleash-forum-tools/json')
    latest_version = response.json()['info']['version']

    if current_version != latest_version:
        print(f"Update available: {latest_version}. You are currently using version {current_version}.")
        return True
    
    return False