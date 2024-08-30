import logging
import streamlit as st

from speakleash_forum_tools.core import ForumToolsCore

from streamlit.logger import get_logger

class StreamlitLogHandler(logging.Handler):
    def __init__(self, widget_update_func):
        super().__init__()
        self.widget_update_func = widget_update_func

    def emit(self, record):
        msg = self.format(record)
        self.widget_update_func(msg)

logger = get_logger("sl_forum_tools")

st.write("Speakleah Forum Tools")

with st.form("my_form"):
    dataset_url = st.text_input("Dataset URL")
    forum_engine = st.selectbox("Forum Engine", ["invision", "phpbb", "ipboard", "xenforo", "other"])
    pagination = st.text_input("Pagination")
    time_sleep = st.number_input("Time Sleep", value=0.5)
    processes = st.number_input("Processes", value=4)
    log_level = st.selectbox("Log Level", ["INFO", "DEBUG"])
    dataset_name = st.text_input("Dataset Name")
    submit_button = st.form_submit_button(label="Run Scraper")

handler = StreamlitLogHandler(st.empty().code)
logger.addHandler(handler)

if submit_button:
    ForumToolsCore(
        dataset_url=dataset_url,
        forum_engine=forum_engine,
        processes=processes,
        log_lvl=log_level,
        dataset_name=dataset_name,
    )

