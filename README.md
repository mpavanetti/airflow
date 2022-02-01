# Apache Airflow data pipeline
## Consuming Weather API and Storing on PostgreSql Database.

### This set of code and instructions has the porpouse to instanciate a compiled environment with set of docker images like airflow webserver, airflow scheduler, postgresql, pyspark.
  
<br>  
## Requirements
git, docker and docker-compose  
<br>  

# Instructions
Clone this repository into your linux working directory and navegate into it.  
  
run commands:
```
# Create Local Folder and give permissions
sudo mkdir airflow && sudo chmod -R 777 airflow && cd airflow

# Clone Git repository to the created folder
git clone https://github.com/mpavanetti/weather_pipeline .

# Build custom docker image
sudo docker build . -f Dockerfile --tag my-image:0.0.1

# Run docker compose
sudo docker-compose up -d

# Import Airflow connections and variables
sudo docker-compose run airflow-cli connections import /app/airflow_connections.json && sudo docker-compose run airflow-cli variables import /app/airflow_variables.json

# Add permissions
sudo chmod -R 777 ../airflow
```
  
In case you have any issues while importing airflow connections and variables, take the json files and import it manually.  
  
Note that you can enter manually latitude and logitude in the airflow varaibles one by one, open the file [airflow_variables.json](airflow_variables.json) and change the parameters weather_data_lat and weather_data_lon respectively or you if you let it as blank, the script will suggest 10 different location.  

Note that the temperature results are in Celsius(units=metric), if you want to change to Fahrenheit open the variable file [airflow_variables.json](airflow_variables.json) and change the parameter weather_data_units to imperial. respectively standard for Kelvin.  

*Important, to Change the variables you can go at the variables section in apache airflow and change the values as you want any time you need or either do in the [airflow_variables.json](airflow_variables.json) and redeploy the containers which might not be very fast.



## Accesses
Access the Airflow UI through the link http://localhost:8080/  

Username: airflow  
Password: airflow
  
Access Postgres Admin through the link http://localhost:15432/  
Username: postgres@email.com  
Password: postgres


## Data Pipeline Run
Go to airflow DAGs view , turn on the dag weather_data, and trigger it.  

![weather_data](img/DAG.JPG)

## Checking DAG result

Open the postgres admin in a chrome web browser, go to Servers and add a server with the information described in the json file [pgadmin.json](pgadmin.json)
  
Check the final views  

SELECT * FROM VW_DATASET_1;  

SELECT * FROM VW_DATASET_2;  
  
After running the airflow DAG pipeline, you should expect the following view result in postgresl:  
  
![weather_data](img/views.JPG)

Thanks.
