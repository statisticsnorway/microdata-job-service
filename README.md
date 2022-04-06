# microdata-job-service
Service for managing jobs in the microdata platform.


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
* Run `docker compose up`´` in `test/resources/local` to run a mongodb instance in docker
* Set environmental variables on your system:
MONGODB_URL=mongodb://localhost:27017/jobdb
```
export MONGODB_USER=USER
export MONGODB_PASSWORD=123
export INPUT_DIR=test/resources/input_directory
```
* Run application from root directory: ´´´poetry run gunicorn job_service.app:app´´´

## Built with
* [Poetry](https://python-poetry.org/) - Python dependency and package management
* [Gunicorn](https://gunicorn.org/) - Python WSGI-server for UNIX
* [Flask](https://flask.palletsprojects.com) - Web framework