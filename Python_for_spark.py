#生存可否モデル計算結果の読み込み
import spss.pyspark.runtime
asContext = spss.pyspark.runtime.getContext()
df = asContext.getSparkInputData()
print(df.toPandas())

#必要ライブラリのインポート
import sys
import subprocess

subprocess.check_call([sys.executable, '-m', 'pip', "install",'ibm-cos-sdk', '--quiet', '--no-input'])

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

#出力先ICOS_バケット指定
target_bucket = "****"

#タイムスタンプ準備
import datetime
import pytz
now = datetime.datetime.now(pytz.timezone('Asia/Tokyo'))

df.toPandas().to_csv("1.csv")

import glob
print(glob.glob("./**"))

src_file = "1.csv"

#ICOSに出力した際のファイルネーム（ファイルネームをタイムスタンプにする）
target_file = now.strftime('%Y%m%d_%H%M%S')+".csv"

bucket = resource.Bucket(target_bucket)
bucket.upload_file(src_file, target_file)
