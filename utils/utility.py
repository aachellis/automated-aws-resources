import boto3
import logging
import shutil
import os
import json

def create_aws_resources(resource_stack, conf, s3, lambda_, glue):
    print(resource_stack)
    for resource in conf:
        if resource["type"] not in ('lambda_function', 'glue_job'):
            logging.error("Can't find specification the the certain resource: " + resource["type"])

        if resource["type"] == "lambda_function":

            resource_conf = resource["configuration"]

            shutil.make_archive("lambda_zip", "zip", os.path.join("lambda_functions", resource_conf["entry_point"]))
            s3.upload_file("lambda_zip.zip", resource_stack["bucket_name"], "lambda_functions/" + resource_conf["entry_point"] + "/lambda_code.zip")

            lambda_function = lambda_.create_function(
                FunctionName = resource["name"],
                Runtime = resource_conf.get("runtime", "python3.9"),
                Role = resource_stack['role_arn'],
                Handler = resource_conf.get("module", "lambda_module") + "." + resource_conf.get("handler", "handler"),
                Code = {
                    "S3Bucket": resource_stack["bucket_name"],
                    "S3Key": "lambda_functions/" + resource_conf["entry_point"] + "/lambda_code.zip"
                },
                Timeout = int(resource_conf.get('timeout', 3))
            )

            resource_stack[resource["name"]] = {
                'type': 'lambda_function',
                'resp': lambda_function
            }

            os.remove("lambda_zip.zip")

        if resource["type"] == "glue_job":
            resource_conf = resource["configuration"]

            shutil.make_archive("glue_helper", "zip", os.path.join("glue_jobs", resource_conf["working_directory"], "utility"))
            s3.upload_file("glue_helper.zip", resource_stack["bucket_name"], "glue_jobs/" + resource_conf["working_directory"] + "/glue_helper.zip")
            s3.upload_file(os.path.join("glue_jobs", resource_conf["working_directory"], "glue_main.py"), resource_stack["bucket_name"], "glue_jobs/" + resource_conf["working_directory"] + "/glue_main.py")

            glue_job = glue.create_job(
                Name = resource["name"],
                Role = resource_stack['role_arn'],
                ExecutionProperty={
                    'MaxConcurrentRuns': int(resource_stack.get("concurrent_runs", "1"))
                },
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{resource_stack["bucket_name"]}/glue_jobs/{resource_conf["working_directory"]}/glue_main.py',
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--extra-py-file': f's3://{resource_stack["bucket_name"]}/glue_jobs/{resource_conf["working_directory"]}/glue_helper.zip'
                },
                Timeout=int(resource_conf.get("timeout", "60")),
                GlueVersion=resource_conf.get("glue_version", "3"),
                NumberOfWorkers=int(resource_conf.get("num_workers", "10")),
                WorkerType=resource_conf.get("worker_type", "G.1X"),

            )

            os.remove("glue_helper.zip")

            resource_stack[resource["name"]] = {
                'type': 'glue_job',
                'resp': glue_job
            }

def create_states(resource_stack, conf, sfn):
    language_dict = {}
    language_dict["States"] = {}
    flag = 0
    for index, states in enumerate(conf["jobs"]):
        if flag == 0:
            language_dict["StartAt"] = states["name"]
            flag = 1

        resourse_state = resource_stack[states["entity"]]

        if resourse_state["type"] == "lambda_function":
            temp_state = {
                "Type": "Task",
                "Resource": resource_stack["resp"]["FunctionArn"]
            }

        elif resourse_state["type"] == "glue_job":
            temp_state = {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": resourse_state["resp"]["Name"]
                }
            }

        if index != len(conf["jobs"]) - 1:
            temp_state["Next"] = states[index+1]["name"]
        else:  
            temp_state["End"] = True
        
        language_dict["States"][states["name"]] = temp_state

    state_machine = sfn.create_state_machine(
        name = conf["name"],
        definition = json.dumps(language_dict),
        roleArn = resource_stack['role_arn'],
        type = 'STANDARD'
    )

    resource_stack["sfn"] = state_machine
        