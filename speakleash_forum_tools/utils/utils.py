"""

"""

import requests
from requests.adapters import HTTPAdapter           # install requests
from urllib3.util.retry import Retry                # install urllib3


def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=3)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    session.verify = True

    return session