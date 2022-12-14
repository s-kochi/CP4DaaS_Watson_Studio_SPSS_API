###Use this code to import data from Modeler to Python
import modelerpy
modelerData = modelerpy.readPandasDataframe()
modelerDataModel = modelerpy.getDataModel()
df = modelerData
print(df)
df.to_csv("./result_01")

import glob
print(glob.glob("./**"))

import sys
import subprocess
modelerpy.installPackage("ibm-cos-sdk")

import ibm_boto3
from ibm_botocore.client import Config

#ICOSサービス情報の記入
credencials={
  "apikey": "***",
  "endpoints": "***",
  "iam_apikey_description": "***",
  "iam_apikey_name": "***",
  "iam_role_crn": "***",
  "iam_serviceid_crn": "***",
  "resource_instance_id": "***"
}

COS_ENDPOINT = "https://s3.jp-tok.cloud-object-storage.appdomain.cloud"
COS_API_KEY_ID = credencials["apikey"] 
COS_INSTANCE_CRN =  credencials["resource_instance_id"]  

resource = ibm_boto3.resource("s3",
    ibm_api_key_id=COS_API_KEY_ID,
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)

target_bucket = "kochi1122"

modelerpy.installPackage("datetime")
import datetime
modelerpy.installPackage("pytz")
import pytz
now = datetime.datetime.now(pytz.timezone('Asia/Tokyo'))

import pandas as pd

df = pd.read_csv("./result_01")
df.to_csv("1.csv")

import glob
print(glob.glob("./**"))

src_file = "1.csv"
target_file = now.strftime('%Y%m%d_%H%M%S')+".csv"

bucket = resource.Bucket(target_bucket)
bucket.upload_file(src_file, target_file)
