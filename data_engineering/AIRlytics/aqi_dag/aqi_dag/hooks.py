import os
from dagster import HookContext, failure_hook
import smtplib
from email.mime.text import MIMEText


GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_AQI_PASSWORD = os.getenv("GMAIL_AQI_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL")

def send_email(subject, body, to_email):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = to_email

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_AQI_PASSWORD)
            server.sendmail(GMAIL_USER, [to_email], msg.as_string())
            #print("Email sent successfully")
    except Exception as e:
        print("Failed to send email:", e)

@failure_hook
def notify_on_failure(context):
    job_name = context.job_def.name if hasattr(context, "job_def") else "Unknown Job"
    op_name = context.op.name if hasattr(context, "op") else "Unknown Op"
    message = f"""
    !! AQI INGEST JOB FAILURE

    Job: {job_name}
    Failed at op: {op_name}
    Run ID: {context.run_id}
    """

    print("Sending failure email...")
    send_email(
        subject=f"Dagster Job Failed: {job_name}",
        body=message,
        to_email=TO_EMAIL
    )

# if __name__ == "__main__":
#     send_email("Test Email", "This is a test from Sanju's AQI pipeline.",TO_EMAIL)
