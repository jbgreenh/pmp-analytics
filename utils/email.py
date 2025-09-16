import base64
import mimetypes
from dataclasses import dataclass, field
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from googleapiclient.discovery import build

from utils import auth


@dataclass
class EmailMessage:
    """
    a dataclass for passing to `send_email()`

    attributes:
        sender: email address of the sender
        to: email address(es) to send to; multiple addresses should be comma separated
        subject: the email subject
        bcc: email address(es) to bcc; multiple addresses should be comma separated
        message_text: the email body
        file_paths: a list of file paths to attachment(s)
        monospace: whether to monospace the email
    """
    sender: str
    to: str
    subject: str
    message_text: str
    bcc: str = ''
    file_paths: list[Path] = field(default_factory=list)
    monospace: bool = False


def send_email(email_message: EmailMessage, *, service=None) -> dict:    # noqa: ANN001 | service is dynamically typed
    """
    sends an email using the details in `email_message`

    args:
        email_message: an `EmailMethod` witht he detals for sending the email
        service: an authorized google email service, generated per email if not provided

    returns:
        the message json as a dict
    """
    if service is None:
        creds = auth.auth()
        service = build('gmail', 'v1', credentials=creds)

    message = MIMEMultipart()
    message['to'] = email_message.to
    message['from'] = email_message.sender
    message['subject'] = email_message.subject

    if email_message.bcc:
        message['bcc'] = email_message.bcc

    if email_message.monospace:
        html_message = f"""
        <html>
        <body>
            <p style="font-family:'Lucida Console','Courier New',monospace; white-space:pre-wrap">{email_message.message_text.replace("\n", "<br>")}</p>
        </body>
        </html>
        """

        msg = MIMEText(html_message, 'html')
    else:
        msg = MIMEText(email_message.message_text)

    message.attach(msg)

    if email_message.file_paths:
        for file_path in email_message.file_paths:
            content_type, encoding = mimetypes.guess_type(file_path)
            if content_type is None or encoding is not None:
                content_type = 'application/octet-stream'
            main_type, sub_type = content_type.split('/', 1)
            msg = MIMEBase(main_type, sub_type)
            msg.set_payload(file_path.read_bytes())
            encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', f'attachment; filename={file_path.name}')
            message.attach(msg)

    message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

    message = service.users().messages().send(userId='me', body=message).execute()
    print(f'message sent, message id: {message['id']}')
    return message
