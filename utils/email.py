import os
import base64
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, List, Optional

def create_message_with_attachments(sender: str, to: str, subject: str, message_text: str, file_paths: Optional[List[str]] = None, bcc: Optional[str] = None, monospace: bool=False) -> Dict[str, str]:
    '''
    returns an email message with the provided sender, to, subject, message_text, and attachments at the file_paths
    '''
    message = MIMEMultipart()
    message['to'] = to  # specify the recipients as a comma-separated string
    message['from'] = sender
    message['subject'] = subject

    if bcc:
        message['bcc'] = bcc

    if monospace:
        html_message = f"""
        <html>
        <body>
            <p style="font-family:'Lucida Console','Courier New',monospace; white-space:pre-wrap">{message_text.replace("\n", "<br>")}</p>
        </body>
        </html>
        """

        msg = MIMEText(html_message, 'html')
    else:
        msg = MIMEText(message_text)

    message.attach(msg)

    if file_paths:
        for file_path in file_paths:
            content_type, encoding = mimetypes.guess_type(file_path)
            if content_type is None or encoding is not None:
                content_type = 'application/octet-stream'
            main_type, sub_type = content_type.split('/', 1)
            with open(file_path, 'rb') as file:
                msg = MIMEBase(main_type, sub_type)
                msg.set_payload(file.read())
            encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
            message.attach(msg)

    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_email(service, message:Dict[str,str]):
    '''sends an email from a message returned from create_message_with_attachment'''
    try:
        message = service.users().messages().send(userId='me', body=message).execute()
        print('message sent, message id: %s' % message['id'])
        return message
    except Exception as error:
        print(f'an error occurred: {error}')
        return None
