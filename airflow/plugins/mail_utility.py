import smtplib
from airflow.models import Variable
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class SimpleMailSender():

    def send_email(sender, receiver, subject, body):

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

            # Envoi du mail
            text = msg.as_string()
            server.sendmail(sender, receiver, text)

            print("E-mail envoyé avec succès!")
        except Exception as e:
            print(f"Erreur lors de l'envoi : {e}")
        finally:
            server.quit()
