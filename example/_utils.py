from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging import getLogger, StreamHandler, INFO
from markdown import markdown
from requests import Session
from requests.adapters import HTTPAdapter
from smtplib import SMTP
from typing import List
from urllib3.util import Retry


with open("_email_template.html", "r") as f:
    email_template = f.read()


def init_logger():
    logger = getLogger("snowkill")

    logger.setLevel(INFO)
    logger.addHandler(StreamHandler())

    return logger


def send_slack_message(slack_token: str, slack_channel: str, message_blocks: List):
    """
    Basic utility function to send Slack messages from SnowKill examples with some retries
    You may consider using Slack SDK package for real world application: https://slack.dev/python-slack-sdk/web/index.html

    Also, you may consider using basic queue services to avoid losing messages if Slack API is temporary unavailable
    SNS in AWS, Pub/Sub in Google Cloud, etc.
    """
    retries = Retry(
        total=3,
        backoff_factor=0.1,
        status_forcelist=[413, 429, 500, 502, 503, 504],
        allowed_methods=["POST"],
    )

    session = Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))

    response = session.post(
        url="https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {slack_token}",
            "Content-type": "application/json; charset=utf-8",
        },
        json={
            "channel": slack_channel,
            "blocks": message_blocks,
        },
    )

    response.raise_for_status()

    return response.json()


def send_email_markdown_message(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    sender_name: str,
    sender_email: str,
    receiver_emails: List[str],
    message_subject: str,
    message_blocks: List[str],
):
    """
    Basic utility function to send email messages from SnowKill examples
    """
    multipart_msg = MIMEMultipart("alternative")
    multipart_msg["Subject"] = message_subject
    multipart_msg["From"] = f"{sender_name} <{sender_email}>"
    multipart_msg["To"] = ",".join(receiver_emails)

    message_markdown_content = "\n\n".join(message_blocks)
    message_html_content = email_template.replace("{{ content }}", markdown(message_markdown_content, extensions=["fenced_code"]))

    multipart_msg.attach(MIMEText(message_markdown_content, "plain"))
    multipart_msg.attach(MIMEText(message_html_content, "html"))

    with SMTP(host=smtp_host, port=smtp_port) as server:
        server.login(smtp_user, smtp_password)
        response = server.send_message(multipart_msg, sender_email, receiver_emails)

    return response
