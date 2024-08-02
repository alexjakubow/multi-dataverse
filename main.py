import os
import sys
import shutil
import requests
import json
from datetime import date
import logging
import csv
from dotenv import load_dotenv, find_dotenv

from pyDataverse.api import NativeApi, DataAccessApi, MetricsApi
from pyDataverse.models import Dataverse, Dataset, Datafile
#from pyDataverse.utils import dataverse_tree_walker, save_tree_data, read_file

load_dotenv(find_dotenv())

# Endpoints and Macros
DATASET_URL = '{}/api/dataverses/{}/datasets'
DATASET_URL_EDIT = '{}/api/datasets/:persistentId/editMetadata?persistentId={}&replace=true'
DATASET_URL_ADD = '{}/api/datasets/{}/add'
LOG_FILE = 'logs/migration.log'
SOURCE_FILE = "hathaway_dois.txt"
RESULTS_FILE = 'bulk_migration_results.csv'

# Logging and results setup
global RESULTS
RESULTS = []
RESULTS.append([
    'Source DOI',
    'Target DOI',
    'Target ID',
    'Status',
    'Error'
])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)

def log_message(source_pid, target_pid=None, target_id=None, msg=None, error=True):
    if not error:
        logging.info('\t' + msg)
    else:
        logging.error('\t' + msg)
        RESULTS.append([source_pid, target_pid, target_id, 'Failed', msg])


# Functions
def setup(env='production', dv='yls', files_dir = 'files', purge_files=True):
    # set globals
    global ENV
    global TARGET_URL
    global TARGET_API
    global TARGET_DATAVERSE
    global SOURCE_URL
    global SOURCE_API
    global FILES_DIR

    # environment check
    if env not in ['production', 'test']:
        raise Exception('Environment must be either "production" or "test"')
    if env == 'production':
        TARGET_URL = 'https://dataverse.yale.edu'
        TARGET_API = os.getenv("DV_YALE_API")
    elif env == 'test':
        TARGET_URL = 'https://dataverse-test.yale.edu'
        TARGET_API = os.getenv("DV_YALE_TEST_API")

    # assign globals
    ENV=env
    TARGET_DATAVERSE = dv
    SOURCE_URL='https://dataverse.harvard.edu'
    SOURCE_API=os.getenv("DV_HARVARD_API")
    FILES_DIR=files_dir
    
    # Create local files directory and optionally purge
    if purge_files==True:
        if os.path.exists(FILES_DIR):
            shutil.rmtree(FILES_DIR)
    if not os.path.exists(FILES_DIR):
        os.makedirs(FILES_DIR)


def make_request(method, url, token, payload=None, file=None, content_type='application/json'):
    REQUEST_HEADERS = {
        'X-Dataverse-key': token,
        'Content-type': content_type
    }

    if method == 'GET':
        return requests.get(
            url=url,
            headers=REQUEST_HEADERS
        )
    elif method == 'POST':
        return requests.post(
            url=url,
            json=payload,
            files=file,
            headers=REQUEST_HEADERS
        )
    elif method == 'PUT':
        return requests.put(
            url=url,
            json=payload,
            headers=REQUEST_HEADERS
        )
    raise Exception('Request method {} not recognized'.format(method))


def dataset_payload(data):
    citation = data['data']['latestVersion']['metadataBlocks']['citation']

    return {
        "datasetVersion": {
            "license": {
                "name": "CC0 1.0",
                "uri": "http://creativecommons.org/publicdomain/zero/1.0"
            },
            'metadataBlocks': {
                'citation': citation
            }
        }
    }


def create_dataset_target(data, dataverse):
    return make_request(method='POST', 
                 payload=data,
                 token=TARGET_API,
                 url= DATASET_URL.format(TARGET_URL, dataverse))


def update_dataset_metadata(pid, source_doi):
    # Check for and replace 'doi:' prefix if available
    doi_url = source_doi.replace('doi:', '')
    doi_url = 'https://doi.org/' + doi_url

    today = date.today().isoformat()

    # Create update payload
    data = {
        'fields': [
            {
                'typeName': 'otherId',
                'typeClass': 'compound',
                'multiple': False,
                'value': [
                    {
                        'otherIdAgency':
                        {
                            'typeName': 'otherIdAgency',
                            'multiple': False,
                            'typeClass': 'primitive',
                            'value': 'Harvard Dataverse'
                        },
                        'otherIdValue':
                        {
                            'typeName': 'otherIdValue',
                            'multiple': False,
                            'typeClass': 'primitive',
                            'value': doi_url
                        }
                    }
                ]
            },
            {
                'typeName': 'depositor',
                'value': 'YLS Library Data Services'
            },
            {
                'typeName': 'dateOfDeposit',
                'value': today
            }
        ]
    }
    return make_request(method='PUT',
                        payload=data,
                        token=TARGET_API,
                        url=DATASET_URL_EDIT.format(TARGET_URL, pid))


def download_files(ds_files, ds_id, api_session, n_retries = 3):
    # Directory check for file downloads
    outdir = '{}/{}'.format(FILES_DIR, ds_id)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    iter = 1
    while iter <= n_retries and len(ds_files) > len(os.listdir(outdir)):
        if iter == 1:
            print('{} dataset files found in source dataset'.format(len(ds_files)))
        print('Download attempt {}/{}'.format(iter, n_retries))
        for file in ds_files:
            file_name = file['dataFile']['filename']
            file_id = file['dataFile']['id']
            file_path = '{}/{}'.format(outdir, file_name)
            if not os.path.exists(file_path):
                response = api_session.get_datafile(file_id, data_format='original')
                if response.status_code != 200:
                    print("Could not download file. Skipping.")
                else:
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                print('Downloaded {}'.format(file_path))
            else:
                print('File {} already found. Skipping download.'.format(file_path))
        iter = iter + 1
        
    # Summary of files that failed to download
    to_retry = []
    for file in ds_files:
        file_name = file['dataFile']['filename']
        file_id = file['dataFile']['id']
        file_path = '{}/{}'.format(outdir, file_name)
        if not os.path.exists(file_path):
            to_retry.append(file_name)
    return to_retry


def upload_target_file(ds_file, ds_id, api_session, dv_path=""):
    local_dir = '{}/{}'.format(FILES_DIR, ds_id)
    dv_file_path = '{}{}'.format(dv_path, ds_file['dataFile']['filename'])
    local_file_path = '{}/{}'.format(local_dir, ds_file['dataFile']['filename'])

    file_payload = {
        'description': ds_file['description'] if ds_file.get('description')  else '',
        'restrict': ds_file['restricted'],
        'tabIngest': False
    }
    json_payload = json.dumps(file_payload)

    df = Datafile()
    df.set({'id': ds_id, 'filename': dv_file_path})
    return api_session.upload_datafile(ds_id, filename=local_file_path, json_str=json_payload, is_pid=False)


def upload_files(ds_files_src, ds_pid, ds_id, api_session, dv_path="", n_retries=3):
    # Get current files (if any) already available from target dataset
    ds = api_session.get_dataset(ds_pid)
    target_files = []
    ds_files_tar = ds.json()['data']['latestVersion']['files']
    for file in ds_files_tar:
        target_files.append(file['dataFile']['filename'])
    
    # Upload
    iter = 1
    to_retry = []
    print("Upload attempt {}/{}...".format(iter, n_retries))
    for file in ds_files_src:
        if file['dataFile']['filename'] in target_files:
            print("{} already found in dataset. Skipping".format(file['dataFile']['filename']))
            continue
        resp = upload_target_file(file, ds_id, api_session)
        if resp.status_code != 200:
            print("Failed to upload file {}".format(file['dataFile']['filename']))
            to_retry.append(file)
        else:
            print("Uploaded file {}".format(file['dataFile']['filename']))

    while iter <= n_retries and len(to_retry) > 0:
        iter = iter + 1
        print("Upload attempt {}/{}...".format(iter, n_retries))
        print("Retrying {} files...".format(len(to_retry)))
        for file in to_retry:
            resp = upload_target_file(file, ds_id, api_session)
            if resp.status_code == 200:
                to_retry.remove(file)
    return to_retry


# Main Function
def main(publish=False):
    setup()

    # Ingest and add doi: prefix
    with open(SOURCE_FILE) as f:
        datasets = []
        for line in f:
            datasets.append('doi:' + str(line))

    # Open API sessions
    source_api = NativeApi(SOURCE_URL)
    source_api_data = DataAccessApi(SOURCE_URL)
    target_api = NativeApi(TARGET_URL, TARGET_API)

    logging.info('*** Copying {} datasets from {} to {} - Target dataverse: {} ***\n'.format(len(datasets), SOURCE_URL, TARGET_URL, TARGET_DATAVERSE))

    # Iterate through list of DOIs to migrate
    for i, doi in enumerate(datasets, 1):

        logging.info('Dataset {}/{} - Source PID {}'.format(i, len(datasets), doi))

        # Get dataset from source
        response_api = source_api.get_dataset(doi)
        if response_api.status_code != 200:
            log_message(doi, msg='Failed to retrieve dataset from source. Skipping. Error: {}\n'.format(response_api.json()))
            continue
        ds_meta = response_api.json()
        log_message(doi, msg='Dataset harvested from source.', error=False)

        # Modify dataset payload
        ds_payload = dataset_payload(ds_meta)

        # Create dataset on target dataverse
        response = create_dataset_target(ds_payload, dataverse=TARGET_DATAVERSE)
        if response.status_code != 201:
            log_message(doi, msg='Failed to create dataset at target. Skipping. Error: {}\n'.format(response.json()))
            continue
        ds_id = response.json()['data']['id']
        ds_pid = response.json()['data']['persistentId']
        log_message(doi, msg='Target dataset {} created at {}'.format(ds_id, ds_pid), error=False)

        # Update dataset metadata
        response = update_dataset_metadata(ds_pid, doi)
        if response.status_code != 200:
            log_message(doi, ds_pid, msg='Failed to update dataset metadata at target. Skipping. Error: {}\n'.format(response.json()))
            continue
        log_message(doi, msg='Target dataset metadata updated.', error=False)

        # Download files from source
        ds_files = response_api.json()['data']['latestVersion']['files']
        result = download_files(ds_files, ds_id, source_api_data)
        if len(result) > 0:
            bad_files = []
            for f in result:
                bad_files.append(f['dataFile']['filename'])
            log_message(doi, ds_pid, msg='Failed to download {}/{} files from source: {} - Skipping.\n'.format(len(result), len(ds_files), bad_files))
            continue
        log_message(doi, ds_pid, msg='All files downloaded from source dataset.', error=False)

        # Upload files to target
        result = upload_files(ds_files, ds_pid, ds_id, target_api)
        if len(result) > 0:
            bad_files = []
            for f in result:
                bad_files.append(f['dataFile']['filename'])
            log_message(doi, ds_pid, msg='Failed to upload {}/{} files to target: {} - Skipping.\n'.format(len(result), len(ds_files), bad_files))
            continue
        log_message(doi, ds_pid, msg='All files uploaded to target dataset.', error=False)

        # Publish
        if publish == True:
            response = target_api.publish_dataset(ds_pid)
            if response.status_code != 200:
                log_message(doi, ds_pid, msg='Failed to publish dataset. Skipping. Error: {}\n'.format(response.json()))

        # Update log
        logging.info('Copied dataset {}/{}\n'.format(i, len(datasets)))
        RESULTS.append([doi, ds_pid, ds_id, 'Success', None])


    with open(RESULTS_FILE, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(RESULTS)


if __name__ == '__main__':
    main()