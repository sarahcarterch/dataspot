"""
Script to .python ..
"""
import os
import logging
import argparse
from time import sleep

from ogd_client import OGDClient
from dataspot_auth import DataspotAuth
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

def main():

    link = "https://bs.dataspot.io/rest/prod/schemes/8386fbc7-2315-4d00-9bf8-47d2b04a6a7d"

    response = requests_get(link, headers=auth.get_headers())
    print(response)
        
    pass
    print("Hello!")
    
    ogd_client = OGDClient()
    endpoint = "/rest/prod/schemes/8386fbc7-2315-4d00-9bf8-47d2b04a6a7d"
    response = ogd_client._get_asset(endpoint=endpoint)
    print(response['description'])

    subject = "Hello!"
    text = response['description']

    
    # Werte aus .env lesen
    receiver = os.getenv("DATASPOT_EMAIL_RECEIVERS")
    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", 587))

    msg = email_helpers.create_email_msg(
    subject="Success",
    text=text
    )

    msg["From"] = sender
    msg["To"] = receiver

    # Email versenden
    email_helpers.send_email(msg)
    print("E-Mail erfolgreich gesendet.")



if __name__ == "__main__":
    main()