import json
import logging
import os
import subprocess
from typing import List, Dict, Any


import boto3
import pandas as pd
from botocore.exceptions import ClientError

KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"]

try:
   UPLOAD_BUCKET = os.environ["UPLOAD_BUCKET"]
except:
   UPLOAD_BUCKET = None

s3 = boto3.client("s3")


def action(event, context) -> Dict[str, Any]:
   """
   Purpose:
       Entrypoint for action on cluster
   Args:
       event - data from lambda
       context - context data from lambda
   Returns:
       response - JSON response
   """

   body = {
       "message": "Running Action",
       "input": event,
   }
   
   print(event)
   print(context)
   data = None
   
   
   if "action" in event:
      curr_action = event["action"]
      topic = event["topic"]
   else:
      curr_action = "produce_and_consume"
      topic = "s3_upload"
      
      # S3 data
      bucket = event["Records"][0]["s3"]["bucket"]["name"]
      key = event["Records"][0]["s3"]["object"]["key"]
      
      file_name = f"/tmp/{key}"
      
      print("Metadata....")
      print(bucket)
      print(key)
      print(file_name)
      
      # Download file
      print("Downloading file")
      s3.download_file(bucket, key, file_name)
      print("Downloaded")
      
      #open file, and save data
      with open(file_name, 'r') as file:
          data = file.read()
          
   # If data is none
   if not data:
      # Sample data
      data = '{"user":"Alice","number":105,"timestampInEpoch":1650880895}\n{"user":"Bob","number":5,"timestampInEpoch":1650880324}'

#   print("Current data...")
#   print(data)
   # If creating topic, can specify number of partitions
   if "num_partitions" in event:
       num_partitions = event["num_partitions"]
   else:
       num_partitions = 1

   if curr_action == "produce":
       print("Produce")
       response = produce(topic, num_partitions, data)
   elif curr_action == "consume":
       print("Consume")
       response = consume(topic)
   elif curr_action == "produce_and_consume":
      print("Producing...") 
      produce(topic, num_partitions, data)
      print("Consuming...")
      consume(topic)
      # upload?
      if UPLOAD_BUCKET:
         print("Uplopading to S3")
         new_name = file_name.replace(".json",".csv")
         turn_json_to_df(file_name,new_name)
         upload_name = new_name.replace("/tmp/","")
         upload_file_to_s3(new_name,UPLOAD_BUCKET,upload_name)
         
         body = {
           "message": "Data produced and consumed !!",
           "file_name": upload_name,
         }
         response = {"statusCode": 200, "body": json.dumps(body)}

      
   else:
       raise ValueError("Invalid action")

   return response


def consume(topic: str) -> Dict[str, Any]:
   """
   Purpose:
       Consume data in a topic
   Args:
       topic - topic to consume on
   Returns:
       response - JSON response
   """
   body = {
       "message": "Data consumed!!!",
   }

   # TODO can play with the timeout, on how long you want to collect data
   # TODO can also configure if you want to get data from the beginning or not --from-beginning
   cmd = f"./kafka_2.12-2.8.1/bin/kafka-console-consumer.sh --topic {topic} --from-beginning --bootstrap-server {KAFKA_ENDPOINT} --consumer.config client.properties --timeout-ms 20000 > /tmp/output.json"

   os.system(cmd)

   # TODO
   # Do what you need to do with output.json i.e upload to s3, run analytics, etc..

   response = {"statusCode": 200, "body": json.dumps(body)}
   logging.info(response)

   return response


def produce(topic: str, num_partitions: int, data) -> Dict[str, Any]:
   """
   Purpose:
       Produce data in a topic
   Args:
       topic - topic to create
       num_partitions - number of num_partitions to use
   Returns:
       response - JSON response
   """
   
   # Write sample output to temp file
   write_to_file("/tmp/test.json", data)

   body = {
       "message": "Data produced!!!",
       "input": data,
   }

   # Check if topic exists, if not create it
   topics = list_topics()
   if not topic in topics:
       if not create_topic(topic, num_partitions):
           raise RuntimeError("Topic not created")

   produce_topic_command = f"./kafka_2.12-2.8.1/bin/kafka-console-producer.sh  --topic {topic} --bootstrap-server {KAFKA_ENDPOINT} --producer.config client.properties < /tmp/test.json"

   os.system(produce_topic_command)

   response = {"statusCode": 200, "body": json.dumps(body)}
   logging.info(response)

   return response


def create_topic(topic:str, num_partitions:int) -> bool:
   """
   Purpose:
       Create topic in cluster
   Args:
       topic - topic to create
       num_partitions - number of num_partitions to use
   Returns:
       bool - True if created, false if not
   """
  
   cmd = f"./kafka_2.12-2.8.1/bin/kafka-topics.sh --bootstrap-server {KAFKA_ENDPOINT} --command-config client.properties --create --topic {topic} --partitions {num_partitions}"

   output = subprocess.check_output(cmd, shell=True)
   output_string = output.decode("utf-8")
   outputs = output_string.split("\n")

   # Check if created
   success_string = f"Created topic {topic}."
   if success_string in outputs:
       logging.info(outputs)
       return True
   else:
       logging.error(outputs)
       return False


def list_topics() -> List[str]:
   """
   Purpose:
       List topics in cluster
   Args:
       N/A
   Returns:
       topics - list of topics
   """
   cmd = f"./kafka_2.12-2.8.1/bin/kafka-topics.sh --list --bootstrap-server {KAFKA_ENDPOINT} --command-config client.properties"

   # Run the command to get list of topics
   output = subprocess.check_output(cmd, shell=True)
   output_string = output.decode("utf-8")
   topics = output_string.split("\n")  # turn output to array

   return topics


def test_produce_consume() -> None:
   """
   Purpose:
       Test producing and consuming
   Args:
       N/A
   Returns:
       N/A
   """
   logging.info("testing produce")
   data = '{"user":"Alice","number":105,"timestampInEpoch":1650880895}\n{"user":"Bob","number":5,"timestampInEpoch":1650880324}'
   response = produce("my_topic", 1, data)
   logging.info(response)
   logging.info("testing consume")
   consume("my_topic")
   logging.info("Done and Done")


def write_to_file(file_path: str, file_text: str) -> bool:
   """
   Purpose:
       Write text from a file
   Args/Requests:
        file_path: file path
        file_text: Text of file
   Return:
       Status: True if written, False if failed
   """

   try:
       with open(file_path, "w") as myfile:
           myfile.write(file_text)
           return True

   except Exception as error:
       logging.error(error)
       return False



def turn_json_to_df(file_name,output_name):

   #  new_name = file_name.replace(".json",".csv")

    with open(file_name) as file:

        df_map = {}
        index = False
        for line in file:

            json_obj = json.loads(line)
            # print(json_obj)
            keys = json_obj.keys()

            if not index:
                for key in keys:
                    df_map[key] = []
                    index = True

            # for val in json_obj[key]:
            for key in keys:
                val = json_obj[key]
                df_map[key].append(val)

        df = pd.DataFrame.from_dict(df_map)
        df.to_csv(output_name, index=False)


def upload_file_to_s3(file_name: str, bucket: str, object_name: str = None) -> bool:
    """
    Purpose:
        Uploads a file to s3
    Args/Requests:
         file_name: name of file
         bucket: s3 bucket name
         object_name: s3 name of object
    Return:
        Status: True if uploaded, False if failure
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3.upload_file(file_name, bucket, object_name)
        logging.info(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == "__main__":
   loglevel = logging.INFO
   logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
   test_produce_consume()