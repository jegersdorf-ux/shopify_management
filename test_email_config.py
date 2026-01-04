import os
import smtplib
from email.mime.text import MIMEText

def test_gmail_connection():
    # Load secrets
    sender = os.getenv('EMAIL_SENDER')
    password = os.getenv('EMAIL_PASSWORD')
    receiver = os.getenv('EMAIL_RECEIVER') # Send to yourself

    print(f"Testing configuration for: {sender}")

    if not sender or not password:
        print("!!! FAIL: Secrets EMAIL_SENDER or EMAIL_PASSWORD are missing.")
        return

    msg = MIMEText("If you are reading this, your Python script can successfully send emails via Gmail!")
    msg['Subject'] = "Test: Gmail Configuration Success"
    msg['From'] = sender
    msg['To'] = receiver

    try:
        # Connect to Gmail SMTP
        print("Connecting to smtp.gmail.com...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        
        # Login
        print("Logging in...")
        server.login(sender, password)
        
        # Send
        print("Sending email...")
        server.send_message(msg)
        server.quit()
        
        print(">>> SUCCESS: Email sent successfully!")
    except smtplib.SMTPAuthenticationError:
        print("!!! FAIL: Authentication Error. Check your App Password.")
    except Exception as e:
        print(f"!!! FAIL: General Error: {e}")

if __name__ == "__main__":
    test_gmail_connection()
