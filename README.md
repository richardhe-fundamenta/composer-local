# Repo with all your dags

## How to run

## Do these first
Local composer uses tokens generated by these
```
gcloud auth application-default login
gcloud auth login
```
Then install composer-dev in a separate directory
```
git clone https://github.com/GoogleCloudPlatform/composer-local-dev.git
cd local_dags
pip install ../composer-local-dev
```

### Set variables

```
export COMPOSER_DEV_IMAGE=composer-2.8.3-airflow-2.7.3
export DAG_PATH=./dags
```

### Create composer environment

```
composer-dev create \
    --from-image-version ${COMPOSER_DEV_IMAGE} \
    --location europe-west2 \
    --project practical-gcp-sandbox-1 \
    --port 8081 \
    --dags-path ./dags \
    composer-local
```

### Start composer

```
composer-dev start composer-local
```

### Restart composer

```
composer-dev restart composer-local
```