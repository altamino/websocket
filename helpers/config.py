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

    MAX_FILE_SIZE = environ.get("MAX_FILE_SIZE", 5000000)
    MAX_TEXT_SIZE = environ.get("MAX_TEXT_SIZE", 2000)

    SMTP_SERVER = environ.get("SMTP_SERVER")
    SMTP_PORT = environ.get("SMTP_PORT")
    SMTP_USER = environ.get("SMTP_USER")
    SMTP_PSWD = environ.get("SMTP_PSWD")

    API_DOMAIN = environ.get("API_DOMAIN")
    SITE_DOMAIN = environ.get("SITE_DOMAIN")

    WS_LINK = environ.get("WS_LINK")
    WS_ADMIN_KEY = environ.get("WS_ADMIN_KEY")
    WS_ADMIN_VERIFY = environ.get("WS_ADMIN_VERIFY")

    PASSWORD_SALT = environ.get("PASSWORD_SALT")
