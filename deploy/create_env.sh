#!/bin/bash
python3 -m venv ./__venv_etl_google_drive
source __venv_etl_google_drive/bin/activate
pip install -U pip #python3 -m pip install --upgrade pip
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
pip install psycopg2-binary
pip install pandas