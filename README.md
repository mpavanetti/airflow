# Apache Airflow data pipeline
## Consuming Weather API and Storing on PostgreSql Database.

### This set of code and instructions has the porpouse to instanciate a compiled environment with set of docker images like airflow webserver, airflow scheduler, postgresql, pyspark.

## Overview
Extract the last 5 days of data from the free API: https://api.openweathermap.org/data/2.5/onecall/timemachine (Historical weather data) from 10 different locations.  
Note: OpenWeather retired One Call API 2.5, this endpoint, in June 2024. The DAG needs porting to One Call API 3.0 (a separate subscription) before it can pull data again.  

Build a repository of data where we will keep the data extracted from the API. This repository should only have deduplicated data. Idempotency should also be guaranteed.  
  
Build another repository of data that will contain the results of the following calculations from the data stored in step 2.  
A dataset containing the location, date and temperature of the highest temperatures reported by location and month.  
A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.  
Remote Infrastructure with all components required to the python workflow works.  
  
  
The set of scripts in Airflow DAG takes care of extracting historical data from the weather API based on 10 different location (5 days per location) and store it locally, light pre processing data and lookup with geopy API to enrich data with locations, store pre processed json files locally also, then the medium effort processing work is send to Spark local through airflow Spark Submit, where the spark script is sent to the local spark cluster, do all the data processing, filtering, aggregations and store back locally in the filesystem, then once the files are stored back as csv files, it runs postgres scripts to copy from (ingest) files to the postgres sql database, finally views are created with the aggregated queries in order to attend the request.     

In Addition, the Dockerfile and docker-compose file scripts are in charge of spining up containers with everything needed to run the airflow DAG successfully end to end.  

  
Remember to sign up an account at https://home.openweathermap.org/ and create an API Token.
Put the token in the password of the openweathermapApi connection (Airflow UI, Admin > Connections) after importing the connections, rather than in [airflow_connections.json](variables/airflow_connections.json), which git tracks.
    
  
## Requirements
* Linux Operational System (Ubuntu 20.04 Prefered) or Windows and docker desktop.
* [Git Versioning Software ](https://git-scm.com/download/linux)
* [Docker Container Virtualization Software](https://docs.docker.com/engine/install/ubuntu/)
* [Docker-Compose multi-container Docker applications](https://docs.docker.com/compose/install/)

  

## Instructions AIrflow 2.7.1 (CeleryExecutor) Heavier Version
Clone this repository into your linux working directory and navegate into it.  
  
run commands:
```

# Clone Git repository to the created folder
git clone https://github.com/mpavanetti/airflow .

# Run the containers as your own user, and give Airflow a key to encrypt connection passwords
# (.env is tracked by git: don't commit your key)
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
grep -q '^AIRFLOW__CORE__FERNET_KEY=' .env || echo "AIRFLOW__CORE__FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')" >> .env

# Run docker compose
sudo docker-compose -f docker-compose-celery.yaml up -d

# Import Airflow connections and variables
sudo docker-compose -f docker-compose-celery.yaml exec airflow-worker airflow connections import /opt/airflow/variables/airflow_connections.json
sudo docker-compose -f docker-compose-celery.yaml exec airflow-worker airflow variables import /opt/airflow/variables/airflow_variables.json

# Stop containers
sudo docker-compose -f docker-compose-celery.yaml kill
```
  


## Instructions AIrflow 2.2.3 (LocalExecutor) Lighter Version
Clone this repository into your linux working directory and navegate into it.  
  
run commands:
```

# Clone Git repository to the created folder
git clone https://github.com/mpavanetti/airflow .

# Run the containers as your own user, and give Airflow a key to encrypt connection passwords
# (.env is tracked by git: don't commit your key)
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
grep -q '^AIRFLOW__CORE__FERNET_KEY=' .env || echo "AIRFLOW__CORE__FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')" >> .env

# Run docker compose
sudo docker-compose up -d

# Import Airflow connections and variables
sudo docker-compose exec airflow-scheduler airflow connections import /opt/airflow/variables/airflow_connections.json
sudo docker-compose exec airflow-scheduler airflow variables import /opt/airflow/variables/airflow_variables.json
```
  
In case you have any issues while importing airflow connections and variables, take the json files and import it manually.  
  
Note that you can enter manually latitude and logitude in the airflow varaibles one by one, open the file [airflow_variables.json](variables/airflow_variables.json) and change the parameters weather_data_lat and weather_data_lon respectively or you if you let it as blank, the script will suggest 10 different location.  

Note that the temperature results are in Celsius(units=metric), if you want to change to Fahrenheit open the variable file [airflow_variables.json](variables/airflow_variables.json) and change the parameter weather_data_units to imperial. respectively standard for Kelvin.  

*Important, to Change the variables you can go at the variables section in apache airflow and change the values as you want any time you need or either do in the [airflow_variables.json](variables/airflow_variables.json) and redeploy the containers which might not be very fast.



## Accesses
Access the Airflow UI through the link http://localhost:8080/  

Username: airflow  
Password: airflow
  
Access Postgres Admin through the link http://localhost:15432/  
Username: postgres@email.com  
Password: postgres
  
Note: the ports are published on 127.0.0.1 only, as these logins are development defaults. When the containers run on a cloud virtual machine, reach them through an SSH tunnel (for example `ssh -L 8080:localhost:8080 -L 15432:localhost:15432 you@your-vm`) rather than opening the ports.
  
  
## Data Pipeline Run
Go to airflow DAGs view , turn on the dag weather_data, and trigger it.  

![weather_data](img/DAG.JPG)

## Checking DAG result

Open the postgres admin in a chrome web browser, go to Servers and add a server with the information described in the json file [pgadmin.json](pgadmin.json) (password: airflow)
  
Check the final views  

SELECT * FROM VW_DATASET_1;  

SELECT * FROM VW_DATASET_2;  
  
After running the airflow DAG pipeline, you should expect the following view result in postgresl:  
  
![weather_data](img/views.JPG)

Thanks.  
  

## Images
Airflow 2.7.1 Celery deployment dags:
![airflowcelery](img/airflowcelerydags.png)
  
Weather_data DAG process:
![weather_data](img/airflowcelery.png)
   

## Extra notes

```
# Install docker-compose v2 on linux os  
sudo curl -L https://github.com/docker/compose/releases/download/v2.17.2/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install tiny file manager. It can write DAGs and plugins, which Airflow runs, so it is
# published on this machine only (127.0.0.1); reach it over SSH or a VPN, never the internet.
# Choose your own login: TFM_USER, and TFM_PASSWORD_HASH from
#   php -r "echo password_hash('your password', PASSWORD_DEFAULT);"
# The folders go on /data, outside the web root: under it, the image's PHP server would hand
# them out without a login.
sudo docker run -d -v ./filemanager:/var/www/html -p 127.0.0.1:8081:80 -e TFM_USER=you -e TFM_PASSWORD_HASH='<hash>' -v ./dags:/data/dags -v ./logs:/data/logs -v ./plugins:/data/plugins -v ./spark:/data/spark -v ./jars:/data/jars -v ./variables:/data/variables --restart=always --name filemanager tinyfilemanager/tinyfilemanager:master

There are no default accounts: sign in with TFM_USER and the password behind TFM_PASSWORD_HASH (http://127.0.0.1:8081).

```
