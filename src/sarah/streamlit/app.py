import streamlit as st
import pandas as pd
import numpy as np
from collections import defaultdict

import os
import logging
import argparse
from time import sleep

from src.ogd_client import OGDClient
from src.dataspot_auth import DataspotAuth
from src.common import email_helpers
import json
from src.common import requests_get

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

auth = DataspotAuth()

# Streamlit UI
st.title("dataspot-Verzeichnis")
st.header("OGD-Freigaben")