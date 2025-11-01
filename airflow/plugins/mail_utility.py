import smtplib
from airflow.models import Variable
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class SimpleMailSender():
    """
    A minimalist utilitary class to send email.

    Methods: 
        send_email(sender, receiver, subject, body):
            Send an email using GMail SMTP.
    """

    def send_email(sender: str, receiver: str, subject: str, body: str) -> None:
        """
        Send an email using GMail SMTP.

        Args:
            sender (str): Email sender address
            receiver (str): Email receiver address
            subject (str): Email subject
            body (str): Email body
            
        Raises:
            Exception: If any error occurs related to the SMTP server (login, sending email).
        """

        # Set email sender, recipient and subject
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = receiver
        msg["Subject"] = subject

        # Set email body
        msg.attach(MIMEText(body, "plain"))

        try:
            # Connect to Gmail SMTP
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(sender, Variable.get("APP_PASSWORD"))

            # Sending email
            text = msg.as_string()
            server.sendmail(sender, receiver, text)

            print("Email sent successfully !")
        except Exception as e:
            print(f"Error while sending : {e}")
        finally:
            server.quit()
