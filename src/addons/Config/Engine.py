from dotenv import load_dotenv
import os

load_dotenv()

ACESS_BIGQUERY      = {
    "CREDENTIALS_NAME":     os.getenv("CREDENTIALS_NAME"),
    "PROJECT_NAME":         os.getenv("PROJECT_NAME"),
}

LOG_CONFIG          = {
    "LOG_LEVEL":            eval(os.getenv("LOG_LEVEL"))
}