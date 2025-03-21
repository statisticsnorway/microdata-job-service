# microdata-job-service
Service for managing jobs in the microdata platform.


## Endpoints

> ### **[GET]** `/jobs`
>Query for existing jobs.
><details>
>  <summary>Example request</summary>
>  
>  ```curl <url>/jobs?status=queued&operation=ADD,REMOVE```
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                   |
>  |--------|----------------------------------------|
>  |   200  |```[{...}, {...}]```                    |
>  |   400  |```{"message": "<Error message>"}```    |
>  |   500  |```{"message": "<Error message>"}```    |
>
></details>
><details>
>  <summary>Query Parameters</summary>
>
>  * **status** - filter on job status
>  * **operation[]** - filter on job operation
>  * **ignoreCompleted** - ignore completed jobs True | False
></details>
_____
> ### **[GET]** `/jobs/<jobId>`
>Get one existing job by job id
><details>
>  <summary>Example request</summary>
>  
>  ```curl <url>/jobs/123```
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                   |
>  |--------|----------------------------------------|
>  |   200  |```{"message": "OK"}```                 |
>  |   400  |```{"message": "<Error message>"}```    |
>  |   500  |```{"message": "<Error message>"}```    |
>
></details>
_____
> ### **[POST]** `/jobs`
>Post new jobs
><details>
>  <summary>Example request</summary>
>  
>  ```curl -X POST <url>/jobs -d '{"jobs": [{...}, {...}]}'```
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                                                                        |
>  |--------|---------------------------------------------------------------------------------------------|
>  |   200  |```[{"job_id": "123-123-123", status": "CREATED", "msg": "OK"}, {"status": "FAILED", "msg": "Missing operation"}]``` |
>  |   400  |```{"message": "<Error message>"}```                                                         |
>  |   500  |```{"message": "<Error message>"}```                                                         |
>
></details>
_____
> ### **[PUT]** `/jobs`
>Update an existing job with a new description or a change of status.
><details>
>  <summary>Example request</summary>
>  
>  ```curl -X PUT <url>/jobs -d '{"status": "failed", "log": "Unexpected failure"}'```
></details>
><details>
>  <summary>Request Body</summary>
>  Must include either description or status
>
>  * **description** - Job description
>  * **status** - Updated job status
>  * **log** - Optional log describing update (Optional)
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                   |
>  |--------|----------------------------------------|
>  |   200  |```{"message": "OK"}```                 |
>  |   400  |```{"message": "<Error message>"}```    |
>  |   500  |```{"message": "<Error message>"}```    |
>
></details>
_____
> ### **[GET]** `/importable-datasets`
>Get information about the importable datasets in the input directory.
><details>
>  <summary>Example request</summary>
>  
>  ```curl -X GET <url>/importable-datasets```
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                                                              |
>  |--------|-----------------------------------------------------------------------------------|
>  |   200  |```[{"datasetName": "MY_DATASET", "hasMetadata": false, "hasData": true}, ...]```  |
>  |   400  |```{"message": "<Error message>"}```                                               |
>  |   500  |```{"message": "<Error message>"}```                                               |
>
></details>
_____

> ### **[DELETE]** `/importable-datasets`
>Delete importable datasets in the input directory.
><details>
>  <summary>Example request</summary>
>  
>  ```curl -X DELETE <url>/importable-datasets/<dataset_name>```
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                                                              |
>  |--------|-----------------------------------------------------------------------------------|
>  |   200  |```{"message": "OK, <dataset_name> deleted"}```
>  |   400  |```{"message": "<Error message>"}```                                               |
>  |   500  |```{"message": "<Error message>"}```                                               |
>
></details>
_____

> ### **[GET]** `/targets`
>Get all target objects
><details>
>  <summary>Example request</summary>
>  
>  ```curl -X GET <url>/targets```
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                                                                        |
>  |--------|---------------------------------------------------------------------------------------------|
>  |   200  |```[...targets]``` |
>  |   400  |```{"message": "<Error message>"}```                                                         |
>  |   500  |```{"message": "<Error message>"}```                                                         |
>
></details>
_____
> ### **[GET]** `/targets/<name>`
>Get all jobs for target with given name, or datastore bump jobs containing the target name
> in its datastore updates.
><details>
>  <summary>Example request</summary>
>  
>  ```curl -X GET <url>/targets/<name>/jobs```
></details>
><details>
_____
> ### **[POST]** `/maintenance-status`
> Sets a flagg that prevents starting new jobs. 
> This will typically be the case when new versions of containers are about to be deployed.
><details>
>  <summary>Example request</summary>
>
>  ```curl -X POST <url>/maintenance-status -d '{"msg": "We need to upgrade", "pause": 1}'```
></details>
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                                                                        |
>  |--------|---------------------------------------------------------------------------------------------|
>  |   200  ||
>  |   400  |```{"message": "<Error message>"}```                                                         |
>  |   500  |```{"message": "<Error message>"}```                                                         |
>
></details>
_____
> ### **[GET]** `/maintenance-status`
> Retrieves the flagg preventing starting new jobs.
><details>
>  <summary>Example request</summary>
>
>  ```curl -X GET <url>/maintenance-status```
></details>
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                                                                        |
>  |--------|---------------------------------------------------------------------------------------------|
>  |   200  | |
>  |   400  |```{"message": "<Error message>"}```                                                         |
>  |   500  |```{"message": "<Error message>"}```                                                         |
>
></details>
_____
> ### **[GET]** `/maintenance-history`
> Retrieves a list of all statuses sorted in reverted chronological order. 
><details>
>  <summary>Example request</summary>
>
>  ```curl -X GET <url>/maintenance-history```
></details>
><details>
>  <summary>Responses</summary>
>
>  | status | json                                                                                        |
>  |--------|---------------------------------------------------------------------------------------------|
>  |   200  | |
>  |   400  |```{"message": "<Error message>"}```                                                         |
>  |   500  |```{"message": "<Error message>"}```                                                         |
>
></details>
_____
</br>
</br>

## Database
This service is bundled with an instance of [mongodb run through docker](https://hub.docker.com/_/mongo), and uses two collections under the jobdb database:
* completed: completed jobs
* inprogress: jobs in progress. This collection has an unique index on the datasetName field. In other words there can only be one active import job for a given dataset at any given time.

#### SETUP
On initialization of the database log into the mongodb container, and enter the mongo shell:

```mongo --username <USER> --password <PASSWORD> ```

In the mongo shell create an index for the inprogress collection:
```
> use jobdb
> db.inprogress.createIndex({"datasetName": 1}, {unique: true})
```
## Contribute

### Set up
To work on this repository you need to install [poetry](https://python-poetry.org/docs/):
```
# macOS / linux / BashOnWindows
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

# Windows powershell
(Invoke-WebRequest -Uri https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py -UseBasicParsing).Content | python -
```
Then install the virtual environment from the root directory:
```
poetry install
```


#### Intellij IDEA
Add Python Interpreter "Poetry Environment".


### Running unit tests
Open terminal and go to root directory of the project and run:
````
poetry run pytest --cov=job_service/
````

### Running locally
If you want to test the service completely in your local environment:
* Run `docker compose up` in `test/resources/local` to run a mongodb instance in docker
* Set environmental variables on your system:
```
export MONGODB_URL=mongodb://localhost:27017/jobdb \
export MONGODB_USER=USER \
export MONGODB_PASSWORD=123 \
export INPUT_DIR=tests/resources/input_directory \
export DOCKER_HOST_NAME=localhost \
export JWKS_URL=http://dummy \
export SECRETS_FILE=tests/resources/secrets/secrets.json \
export STACK=local \
export COMMIT_ID=abc123
```
* Run application from root directory: ```poetry run gunicorn job_service.app:app```

## Built with
* [Poetry](https://python-poetry.org/) - Python dependency and package management
* [Gunicorn](https://gunicorn.org/) - Python WSGI-server for UNIX
* [Flask](https://flask.palletsprojects.com) - Web framework
* [Pydantic](https://pydantic-docs.helpmanual.io/) - Data validation

