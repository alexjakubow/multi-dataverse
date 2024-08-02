# dataverse-migration

A Python script/workflow for copying over deposits from an external Dataverse instance (like Harvard's) to Yale Dataverse.  

## Overview

This is still very much a work in progress, but maybe others will find it useful--particularly for the batch upload context.  It leans on the excellent [pyDataverse](https://pydataverse.readthedocs.io/en/latest/) module.

Here is a quick summary of how it works:

Given a list of dataset DOIs from the source dataverse (e.g., Harvard), the program will:

- Harvest dataset metadata from the source dataverse
- Modify the metadata. Specifically:
    - Add the name of the source dataverse and corresponding source DOI in the `otherId` field (e.g., Harvard Dataverse - doi/123414.34)
    - Modify the name of the depositor specified within `update_dataset_metadata()` (defaults to 'YLS Library Data Serivces' for our use case)
    - Change `dateOfDeposit` to current date
- Create dataset entry on Yale Dataverse using the newly modified metadata
- Download data files from source, preserving original format, restricted designation, and file description (if any) from the source file's metadata
- Upload all data files to Yale Dataverse with metadata
- Publish the dataset on Yale Dataverse (optional)

## Getting Started

### Pre-requisites
- Clone this repository
- Create a virtual environment within the main directory and install dependencies (`requirements.txt`)
- Ensure you have valid API tokens and appropriate access privileges on both the [test](https://dataverse-test.yale.edu/) and [production](https://dataverse.yale.edu/) instances of Yale Dataverse.
- Create an `.env` file in the main directory, and respectively save your test and production API-tokens to `DV_YALE_API` and `DV_YALE_TEST_API`
- Run `test.py` to ensure you can access the three API endpoints you will need to run the pipeline:
    - [Native API](https://guides.dataverse.org/en/latest/api/metrics.html) on both the source and target (Yale) dataverses
    - [Data Access API](https://guides.dataverse.org/en/latest/api/dataaccess.html) on the source dataverse
- A list of dataset DOIs from the source dataverse that you would like to harvest

Modify `main.py` and execute as necessary.


## Example
- This [dataset](https://doi.org/10.60600/YU/VQKZPY) was succesfully copied from [here](https://doi.org/10.7910/DVN/DUPKPA) using this script/workflow.


# Issues

**Test Instance Authentication/Endpoint**

- Some combination of endpoints specified near the top of `main.py` and/or `TARGET_URL` specified within `setup()` do not appear to be defined correctly.  Execution results in error of: 

```
Failed to create dataset at target. Skipping. Error: {'status': 'ERROR', 'code': 404, 'message': 'API endpoint does not exist on this server. Please check your code for typos, or consult our API guide at http://guides.dataverse.org.', 'requestUrl': 'https://dataverse-test.yale.edu/api/v1/dataverses/dataverse/yls/datasets', 'requestMethod': 'POST'}
```

**File Upload problems**

- These two files: ([Formal_Nonbindings](https://dataverse.harvard.edu/file.xhtml?fileId=7141192&version=1.1); [Joint_Statements](https://dataverse.harvard.edu/file.xhtml?fileId=7141194&version=1.1)) cannot be uploaded to [this draft dataset](https://dataverse.yale.edu/dataset.xhtml?persistentId=doi:10.60600/YU/010GBX) on Yale Dataverse - both via the API script and manual submission.  They are 2.3G and 150M in size, repsectively.  The 150M file has more than 1K files in it, which appears to be the cause of the error.

**Metrics**

- Not sure whether it is possible to access download/viewing metrics from source dataverse.  Need to explore [Metrics API](https://guides.dataverse.org/en/latest/api/metrics.html) in more detail to see what metrics apply at which levels (dataverse vs. dataset vs. individual data files)
- Would it be possible to upwardly adjust metric counts on Yale Dataverse by adding metrics from source dataverse to metrics to Yale Dataverse?

**Deaccessioning from Source**

- No defined procedure for requesing that the source dataverse deaccession/purge datasets succesfully migrated to Yale Dataverse.


# To-Dos
- [ ] Function documentation
- [ ] Create `test.py` (haha...)
- [ ] Automatically copy over license from source; set CC0 as default if no license found at source
- [ ] Exception handling for API timeouts/`ConnectionError`
- [x] Experiment with [direct API file upload](https://guides.dataverse.org/en/latest/developers/s3-direct-upload-api.html) to see if that can fix large uploads issue using API  *UPDATE 2024-01-22: not currently enabled on Yale Dataverse; will use SWORD API instead*
- [ ] Standardize function argument taxonomy for calls to source vs. target and entity type (dataverse, dataset, datafile)
- [ ] Allow for differentiation between source type in pipeline -- whether pipeline is a migration (current) or an new upload
