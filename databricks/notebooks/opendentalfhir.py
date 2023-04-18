# Databricks notebook source
# MAGIC %md
# MAGIC ### Open Dental API - FHIR r4 implementation
# MAGIC <p>Matheus Pavanetti</p>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Architecture
# MAGIC ![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)
# MAGIC 
# MAGIC - **Bronze** tables contain raw data ingested from various sources (JSON data from open dental api call).
# MAGIC 
# MAGIC - **Silver** tables provide a more refined view of our data. (parsed json, cleaned data, schema enforcement).
# MAGIC 
# MAGIC - **Gold** tables provide business level aggregates often used for reporting and dashboarding.
# MAGIC   
# MAGIC ### Documentation Reference
# MAGIC - **API Implementation** https://www.opendental.com/site/apiimplementation.html  
# MAGIC   
# MAGIC - **Open Dental Specs FHIR**  https://www.opendental.com/resources/OpenDentalFHIR19-3Spec.pdf
# MAGIC     
# MAGIC - **FHIR Standard** http://hl7.org/fhir/R4/

# COMMAND ----------

# DBTITLE 1,Setting up storage account on spark session
# Run this to Authenticate on storage account
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get('FILLIN', 'FILLIN'))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get('FILLIN', 'FILLIN'))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{dbutils.secrets.get('FILLIN', 'FILLIN')}/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Environment Class
import requests
import json
import pandas as pd
import os
from datetime import datetime

class Env:
  
  # Datalake
  datalakeDir="abfss://datalake@FILLIN.core.windows.net/"
  workdir=f"{datalakeDir}opendentalfhir/" 
  
  # Unity Catalog
  catalog="FILLIN"
  catalog_bronze=f"{catalog}.bronze"
  catalog_silver=f"{catalog}.silver"
  catalog_gold=f"{catalog}.gold"
  
  # Driver
  tmp_dir = "/tmp/opendentalfhir/"
  
  # Multi-Hop Architecture variables.
  raw=f"{workdir}raw/"
  bronze=f"{workdir}bronze/"
  silver=f"{workdir}silver/"
  gold=f"{workdir}gold/"
  
  # Open Dental API variables.
  endpoint="https://api.opendental.com/fhir/v2/"
  token="ODFHIR NFF6i0KrXrxDkZHt/VzkmZEaUWOjnQX2z"
  resources=["organization","location",
            "patient","procedure","practitioner",
            "ServiceRequest"]
  
  def __init__(self):
    self.tmp_dir = Env.tmp_dir
  
  def createTmp(self):
    if not os.path.exists(self.tmp_dir):
        os.mkdir(self.tmp_dir)
  
  def wipeTmp(self):
    if os.path.exists(self.tmp_dir):
      for file in os.listdir(self.tmp_dir):
        os.remove(os.path.join(self.tmp_dir, file))
  

# COMMAND ----------

# DBTITLE 1,Extract Class
class Extract(Env):
  
  def __init__(self):
    Env.__init__(self)
    Env.createTmp(self)
    
  def cleanRaw(self):
    return dbutils.fs.rm(Env.raw,True)
  
  def cleanTmp(self):
    return Env.wipeTmp()
  
  def pullData(self):
    headers={"Accept": "application/json",
             "Content-Type": "application/json",
             "Authorization": Env.token}
    
    Env.wipeTmp(self)
    
    for item in Env.resources:
      time = datetime.now().strftime(("%Y%m%d%H%M%S")) # current date and time of file download (UTC).
      result = requests.get(f"{Env.endpoint}{item}",headers=headers)
      
      if result.status_code == 200:
        resultjson = json.loads(result.text)
        pd_df = pd.json_normalize(resultjson)
        
        # Download temp json
        pd_df.to_json(f"{Env.tmp_dir}/{item}_{time}.json")
        
        # Upload Datalake
        dbutils.fs.cp(f"file:////{Env.tmp_dir}/{item}_{time}.json",f"{Env.raw}{item}/{item}_{time}.json")
        print(f"RAW Downloaded {resultjson['total']} {item} records at file: {Env.raw}{item}/{item}_{time}.json")
    

# COMMAND ----------

# DBTITLE 1,Transform Class
#from pyspark.sql.functions import explode, cast, flatten, collect_set

class Transform(Env):
  
    def __init__(self):
      Env.__init__(self)
    
    def rawToBronze(self):
      for item in Env.resources:
        df = (spark.read.json(f"{Env.raw}{item}/"))
        transformed_df = df.selectExpr("explode(flatten(collect_set(entry.*))) as data")
        transformed_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{Env.bronze}{item}/")#.saveAsTable(f"{Env.catalog_bronze}.{item}")
        print(f"BRONZE records for entity {item} were persisted at {Env.bronze}{item}/")
        
    def patientToSilver(self):
      df = (spark.read.format("delta").load(f"{Env.bronze}/patient"))
      
      transformed_df = (df.selectExpr("CAST(data.resource.id as LONG) as id",
                               "data.resource.active as active",
                               "data.resource.name as name",
                               "data.resource.resourceType as resourceType",
                               "data.resource.address as address",
                               "CAST(data.resource.birthdate as DATE) as birthdate",
                               "data.resource.careProvider as careProvider",
                               "data.resource.gender as gender",
                               "data.resource.identifier as identifier",
                               "data.resource.managingOrganization as managingOrganization",
                               "data.resource.maritalStatus as maritalStatus",
                               "data.resource.meta as meta",
                              "data.resource.telecom as telecom",
                              "data.search as search")
                          .distinct())
      (transformed_df
         .write
         .mode("overwrite")
         .save(f"{Env.silver}patient/"))
         #.saveAsTable(f"{Env.catalog_silver}.patient"))
      print(f"SILVER patient records were persisted at {Env.silver}patient")
      

# COMMAND ----------

# Main File
if __name__ == "__main__":
  
  ext = Extract()
  trans = Transform()
  
  #ext.cleanRaw()
  
  ext.pullData()
  trans.rawToBronze()
  
  trans.patientToSilver()
  
