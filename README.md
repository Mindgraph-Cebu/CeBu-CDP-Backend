
# CEBU-CDP-BACKEND

## Overview

This backend service is crafted for the CDP (Customer Data Platform) Module and built on Python and FastAPI. It provides secure authentication and efficient management of booker and passenger data. 

## API Endpoints

- **/api/health**: Checks the health of the service.
- **/api/milestones**: Retrieves milestone data.
- **/api/profile**: Fetches profile information based on type (passenger or booker).
- **/api/profile/search**: Searches for profiles based on various parameters.
- **/api/copy_files**: Syncs files from an S3 bucket to the local system.

For more info about API endpoints, visit `http://127.0.0.1:8000/docs`.

## Installation

- Clone the repository:

```bash
git clone https://github.com/Mindgraph-Cebu/CeBu-CDP-Backend.git

cd CeBu-CDP-Backend

```

* Install the dependencies:

```bash
pip install -r requirements.txt

```

* Set up your environment variables in the .env file.

`TENANT_ID = your-tenant-id`

`BUCKET_NAME = your-bucket-name` 

`ATHENA_SCHEMA_NAME = your-athena-schema-name`

`S3_STAGING_DIR = your-s3-staging-directory`

`USER_ENV = your-environment  # 'athena', 'local', or 's3'`

## Usage

- Start the FastAPI server:

```bash
uvicorn main:app --reload

```

- Access the endpoints:

Access the endpoints via http://127.0.0.1:8000/docs in your web browser or API client.

## Tech Stack

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi) 

![JWT](https://img.shields.io/badge/JWT-black?style=for-the-badge&logo=JSON%20web%20tokens)

![DuckDB](https://img.shields.io/badge/DuckDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white) 

![Boto3](https://img.shields.io/badge/Boto3-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white) 

![AWS Athena](https://img.shields.io/badge/AWS%20Athena-232F3E?style=for-the-badge&logo=amazonaws)

## Authors

- Author: Pradish Pranam
- Copyright: MindGraph Technologies
- Version: 0.1.0
- Maintainers: 
  - Pradish Pranam
  - Mohamed Ashif H
- Email: 
  - pradishpranam.s@mind-graph.com
  - ashif.hm@mind-graph.com
- Status: Development
- Date: 04/Apr/2024

## License

- Copyright (c) 2024. MindGraph Technologies. All rights reserved.
- Proprietary and confidential. Copying and distribution is strictly prohibited.

