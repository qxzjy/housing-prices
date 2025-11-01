import boto3
import paramiko
import time
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.ec2 import EC2CreateInstanceOperator, EC2TerminateInstanceOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime

# Jenkins Configuration: Load from Airflow Variables
JENKINS_URL = Variable.get("JENKINS_URL")
JENKINS_USER = Variable.get("JENKINS_USER")
JENKINS_TOKEN = Variable.get("JENKINS_TOKEN")
JENKINS_JOB_NAME = Variable.get("JENKINS_JOB_NAME")

# Get AWS connection details from Airflow
KEY_PAIR_NAME = Variable.get("KEY_PAIR_NAME")
KEY_PATH = Variable.get("KEY_PATH")  # Path to your private key inside the container
AMI_ID = Variable.get("AMI_ID")
SECURITY_GROUP_ID = Variable.get("SECURITY_GROUP_ID")
INSTANCE_TYPE = Variable.get("INSTANCE_TYPE")
SUBNET_ID = Variable.get("SUBNET_ID")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
REGION_NAME = Variable.get("REGION_NAME")

# Retrieve other env variables for MLFlow to run
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_NAME = Variable.get("MLFLOW_EXPERIMENT_NAME")
DB_URI = Variable.get("DB_URI")

if not all([JENKINS_URL, JENKINS_USER, JENKINS_TOKEN]):
    raise ValueError("Missing one or more Jenkins configuration environment variables")

with DAG(dag_id="training_ec2", start_date=datetime(2025, 8, 28), schedule_interval=None, catchup=False) as dag:
    start = DummyOperator(task_id="start")

    # Step 1: Poll Jenkins Job Status
    @task
    def poll_jenkins_job() -> bool:
        """
        Poll Jenkins for the job status and check for successful build.

        Returns:
            bool: Return True if the last Jenkins buidl was successfull, else raise an Exception.

        Raises:
            ValueError: If their is no build number found in Jenkins.
            Exception: If the build failed.
            RequestException: If an error related to the Jenkins API occurs.
        """
        try:
            # Step 1.1: Get the latest build number from the job API
            job_url = f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/api/json"
            response = requests.get(job_url, auth=(JENKINS_USER, JENKINS_TOKEN), timeout=30)
            response.raise_for_status()

            job_info = response.json()
            latest_build_number = job_info.get('lastBuild', {}).get('number')
            if not latest_build_number:
                raise ValueError("No build number found in Jenkins response")

            # Step 1.2: Poll the latest build's status
            build_url = f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/{latest_build_number}/api/json"

            while True:
                try:
                    response = requests.get(build_url, auth=(JENKINS_USER, JENKINS_TOKEN), timeout=30)
                    response.raise_for_status()
                    build_info = response.json()
                    
                    if not build_info['building']:  # Build is finished
                        if build_info['result'] == 'SUCCESS':
                            print("Jenkins build successful!")
                            return True
                        else:
                            raise Exception(f"Jenkins build failed with result: {build_info['result']}")
                    
                    time.sleep(30)  # Poll every 30 seconds
                except requests.RequestException as e:
                    raise Exception(f"Failed to poll build status: {str(e)}")
                
        except requests.RequestException as e:
            raise Exception(f"Failed to query Jenkins API: {str(e)}")
        except (KeyError, ValueError) as e:
            raise Exception(f"Invalid Jenkins API response: {str(e)}")

    # Step 2: Create EC2 Instance Using EC2 Operator
    create_ec2_instance = EC2CreateInstanceOperator(
        task_id="create_ec2_instance",
        image_id=AMI_ID,
        region_name=REGION_NAME,  
        max_count=1,
        min_count=1,
        config={
            "InstanceType": INSTANCE_TYPE,
            "KeyName": KEY_PAIR_NAME,  
            "NetworkInterfaces": [
                {
                    "AssociatePublicIpAddress": True,
                    "DeleteOnTermination": True,
                    "DeviceIndex": 0,
                    "SubnetId": SUBNET_ID,
                    "Groups": [SECURITY_GROUP_ID]
                }
            ],            
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Purpose", "Value": "ML-Training"}]
                }
            ]
        },
        wait_for_completion=True,
    )
    
    # Step 3: Use EC2 Sensor to Check if Instance is Running
    @task
    def check_ec2_status(instance_id: str) -> bool:
        """
        Check if the EC2 instance has passed both status checks.
        
        Args:
            instance_id (str): String containing the EC2 instance ID create via the EC2CreateInstanceOperator.  

        Returns:
            bool: Return True if the EC2 instance is created, else retry.

        Raises:
            Exception: If an error related to the AWS API occurs.
        """
        try:
            ec2_client = boto3.client(
                'ec2', 
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME
            )  
            passed_checks = False
            
            while not passed_checks:
                response = ec2_client.describe_instance_status(InstanceIds=instance_id)

                if response['InstanceStatuses']:
                    instance_status = response['InstanceStatuses'][0]
                    system_status = instance_status['SystemStatus']['Status']
                    instance_status_check = instance_status['InstanceStatus']['Status']
                    
                    print(f"System Status: {system_status}, Instance Status: {instance_status_check}")
                    
                    if system_status == 'ok' and instance_status_check == 'ok':
                        print(f"Instance {instance_id} has passed 2/2 status checks.")
                        return True
                
                time.sleep(15)

        except boto3.exceptions.Boto3Error as e:
            raise Exception(f"AWS API error: {str(e)}")

    @task
    def get_ec2_public_ip(instance_id: str) -> str:
        """
        Retrieve the EC2 instance public IP for SSH.
        
        Args:
            instance_id (str): String containing the EC2 instance ID create via the EC2CreateInstanceOperator.  

        Returns:
            str: Return the public IP of the created EC2 instance.

        Raises:
            ValueError: If no public IP address assigned to instance.
            Exception: If an error related to the AWS API occurs.
        """
        try:
            ec2_client = boto3.resource(
                'ec2', 
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME
            )

            instance = ec2_client.Instance(instance_id[0])
            instance.wait_until_running()
            instance.reload()

            public_ip = instance.public_ip_address
            if not public_ip:
                raise ValueError("No public IP address assigned to instance")
                
            print(f"Public IP of EC2 Instance: {public_ip}")
            return public_ip

        except (boto3.exceptions.Boto3Error, ValueError) as e:
            raise Exception(f"Failed to get EC2 instance IP: {str(e)}")
    
    @task
    def run_training_via_paramiko(public_ip: str) -> None:
        """
        Use Paramiko to SSH into the EC2 instance and run ML training.
        
        Args:
            public_ip (str): String containing public IP of the created EC2 instance.  

        Raises:
            SSHException: If an error related to the SSH connection occurs.
            Exception: If an error related to the training of the model occurs.
        """
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            private_key = paramiko.RSAKey.from_private_key_file(KEY_PATH)
            ssh_client.connect(
                hostname=public_ip, 
                username="ubuntu", 
                pkey=private_key,
                timeout=60
            )

            command = f'''
            export PATH=$PATH:/home/ubuntu/.local/bin && \
            export MLFLOW_TRACKING_URI="{MLFLOW_TRACKING_URI}" && \
            export MLFLOW_EXPERIMENT_NAME="{MLFLOW_EXPERIMENT_NAME}" && \
            export AWS_ACCESS_KEY_ID="{AWS_ACCESS_KEY_ID}" && \
            export AWS_SECRET_ACCESS_KEY="{AWS_SECRET_ACCESS_KEY}" && \
            export DB_URI="{DB_URI}" && \
            mlflow run https://github.com/qxzjy/housing-prices-ml --build-image
            '''
            
            stdin, stdout, stderr = ssh_client.exec_command(command, timeout=3600)
            
            for line in stdout:
                print(line.strip())
            for line in stderr:
                print(line.strip())

            if stdout.channel.recv_exit_status() != 0:
                raise Exception("Training command failed")

        except paramiko.SSHException as e:
            raise Exception(f"SSH connection error: {str(e)}")
        except Exception as e:
            raise Exception(f"Training execution error: {str(e)}")
        finally:
            ssh_client.close()
            
    terminate_instance = EC2TerminateInstanceOperator(
        task_id="terminate_ec2_instance",
        instance_ids="{{ task_instance.xcom_pull(task_ids='create_ec2_instance', key='return_value')[0] }}",
        region_name=REGION_NAME, 
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    jenkins_poll = poll_jenkins_job()
    check_instance = check_ec2_status(create_ec2_instance.output)
    get_ip = get_ec2_public_ip(create_ec2_instance.output)
    run_train = run_training_via_paramiko(get_ip)

    end = DummyOperator(task_id="end")

    start >> jenkins_poll >> create_ec2_instance >> check_instance >> get_ip >> run_train >> terminate_instance >> end