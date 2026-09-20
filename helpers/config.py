from os import environ


class Config:
    MONGODB_CONNECTION_STRING = environ.get("MONGODB_CONNECTION_STRING")
    MONGODB_MAIN_DB = environ.get("MONGODB_MAIN_DB")

    REDIS_CONNECTION_STRING = environ.get("REDIS_CONNECTION_STRING")

    S3_SERVICE_NAME = environ.get("S3_SERVICE_NAME")
    S3_ACCESS_KEY = environ.get("S3_ACCESS_KEY")
    S3_SECRET_ACCESS_KEY = environ.get("S3_SECRET_ACCESS_KEY")
    S3_ENDPOINT_URL = environ.get("S3_ENDPOINT_URL")
    S3_BUCKET_NAME = environ.get("S3_BUCKET_NAME")
    MEDIA_BASE_URL = environ.get("MEDIA_BASE_URL")

    # i think it can be static
    # why you need to change them?
    S3_UPLOADS_FOLDER = "user-uploads/"
    S3_IMAGES_FOLDER = S3_UPLOADS_FOLDER + "images/"
    S3_VOICES_FOLDER = S3_UPLOADS_FOLDER + "voices/"
    S3_NDCTHEMES_FOLDER = "ndc-themes/"

    MAX_FILE_SIZE = int(environ.get("MAX_FILE_SIZE", 5000000))
    MAX_TEXT_SIZE = int(environ.get("MAX_TEXT_SIZE", 2000))

    SMTP_SERVER = environ.get("SMTP_SERVER")
    SMTP_PORT = environ.get("SMTP_PORT")
    SMTP_USER = environ.get("SMTP_USER")
    SMTP_PSWD = environ.get("SMTP_PSWD")
    SMTP_SNDR = environ.get("SMTP_SNDR")
    SMTP_STARTTLS = environ.get("SMTP_STARTTLS", "True").lower() in ["true", "1"]

    API_DOMAIN = environ.get("API_DOMAIN", "service.altamino.top")
    API_BASE_URL = environ.get("API_BASE_URL", f"https://{API_DOMAIN}")

    SITE_DOMAIN = environ.get("SITE_DOMAIN")
    SITE_BASE_URL = environ.get("SITE_BASE_URL", f"https://{SITE_DOMAIN}")

    WS_LINK = environ.get("WS_LINK")
    WS_ADMIN_KEY = environ.get("WS_ADMIN_KEY")
    WS_ADMIN_VERIFY = environ.get("WS_ADMIN_VERIFY")

    PASSWORD_SALT = environ.get("PASSWORD_SALT")

    AGORA_APP_ID = environ.get("AGORA_APP_ID", "")
    AGORA_APP_CERTIFICATE = environ.get("AGORA_APP_CERTIFICATE", "")
