from smtplib import SMTP_SSL
from email.message import EmailMessage


class AutoEmailer:

    def __init__(self, email_address: str, email_passwd: str):
        self.email_address = email_address
        self.email_passwd = email_passwd

    def send_email(self, msg_from: str, msg_to: str, msg_subject: str, msg_body: str):
        msg = EmailMessage()
        gmail_smtp_server = 'smtp.gmail.com'
        port = 465
        msg['From'] = msg_from
        msg['To'] = msg_to
        msg['Subject'] = msg_subject
        msg.set_content(msg_body)
        server = SMTP_SSL(gmail_smtp_server, port)
        server.login(self.email_address, self.email_passwd)
        server.send_message(msg)
        server.quit()