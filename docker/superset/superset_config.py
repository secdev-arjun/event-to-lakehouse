import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "superset-secret-key-change-me")
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
)

# Behind docker networks; keep default secure behavior but allow proxy headers if added later.
ENABLE_PROXY_FIX = True
