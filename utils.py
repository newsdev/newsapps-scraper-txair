import contextlib
import datetime
import importlib
import os
import re
import time

from google.cloud import storage

def get_env():
    return os.environ.get('DEPLOYMENT_ENVIRONMENT', 'dev')

settings = importlib.import_module('config.%s.settings' % get_env())

def persist_file(path):
    client = storage.Client()
    bucket = client.get_bucket(settings.STATIC_BUCKET)
    local_filename = path.split('/')[-1]
    remote_path = '%s/%s' % (settings.REMOTE_STORAGE_PATH, local_filename)
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(filename=path)
    blob.make_public()
    return blob.public_url.replace('apps%2Ftxair%2F', 'apps/txair/')