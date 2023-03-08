import boto3
import json
import logging
import sys
import uuid
import botocore

from utils.utility import create_aws_resources, create_states

logging.basicConfig(
    level = logging.INFO,
    format= '%(levelname)s %(asctime)s \t %(name)s - > %(message)s'
)

resource_stack = {}

# try:
with open("configuarions.json") as f:
    conf = json.load(f)

if "resources" not in conf:
    logging.info("No resources to create. Exiting execution.")
    sys.exit()

if "region" not in conf:
    logging.error("Configuration file does not contains the required variable: region.")

resource_stack["region"] = conf["region"]
region = resource_stack["region"]

iam = boto3.client("iam", region_name=region)
s3 = boto3.client("s3", region_name=region)
glue = boto3.client("glue", region_name=region)
lambda_ = boto3.client("lambda", region_name=region)
sfn = boto3.client("stepfunctions", region_name=region)

if "name" not in conf:
    logging.error("Configuration file does not contains the required variable: name.")

bucket_name = conf["name"].replace(' ', '-') + "-" + str(uuid.uuid4())

resource_stack['bucket_name'] = bucket_name

bucket = s3.create_bucket(
    Bucket=bucket_name,
    CreateBucketConfiguration={
        'LocationConstraint': resource_stack['region']
    },
)
logging.info("Bucket with name : " + bucket_name + " has been created successfully.")

with open("role_document.json") as f:
    policy = json.load(f)

role = iam.create_role(
    RoleName = conf["name"].replace(' ', '-') + "-execution-role",
    AssumeRolePolicyDocument = json.dumps(policy),
    MaxSessionDuration = 3600
)

resource_stack["role_arn"] = role["Role"]["Arn"]
logging.info("Role with name : " + conf["name"].replace(' ', '-') + "-execution-role has been created successfully.")

create_aws_resources(resource_stack, conf["resources"], s3, lambda_, glue)

create_states(resource_stack, conf["states"], sfn)

print(resource_stack, sfn)


# except FileNotFoundError:
#     logging.error("The configuration file is not found. Please make sure the project is not corrupted.")

