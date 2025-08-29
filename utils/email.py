import base64
import mimetypes
import pathlib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# TODO: convert to class, filepaths should be a list of Path objects
def create_message_with_attachments(sender: str, to: str, subject: str, message_text: str, *, file_paths: list[str] | None = None, bcc: str | None = None, monospace: bool = False) -> dict[str, str]:
    """
    creates an email message with aoptional attachments

    args:
        sender: the sender email address, can also be a comma separated list of emails
        to: the to email address, can also be a comma separated list of emails
        subject: the email subject
        message_text: the email body
        file_paths: a list of filepaths to attachments
        bcc: email address(es) to bcc, can also be a comma separated list of emails
        monospace: a boolean indicating weather or not to use a monospace font

    returns:
        returns a message dict for passing to `send_email()`
    """
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
            msg.add_header('Content-Disposition', f'attachment; filename={pathlib.Path(file_path).name}')
            message.attach(msg)

    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}


# TODO: make email service within send_email? maybe optional use if provided make if not? could be good for tons of google drive utils as well
def send_email(service, message: dict[str, str]):   # noqa:ANN001 | service is dynamically typed
    """sends an email from a message returned from create_message_with_attachment"""
    try:
        message = service.users().messages().send(userId='me', body=message).execute()
    except Exception as error:
        print(f'an error occurred: {error}')
        return None
    else:
        print(f'message sent, message id: {message['id']}')
        return message
