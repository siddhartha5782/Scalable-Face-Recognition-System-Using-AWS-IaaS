import time
import threading
import logging
import boto3

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
AMI_ID = "AMDID"
INSTANCE_TYPE = "t2.micro"
KEY_NAME = "KEYPAIR"
SECURITY_GROUP_ID = "SECURITYID"
MAX_INSTANCES = 15
REGION = "us-east-1"
REQ_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/020034636866/1233628905-req-queue"
TAG_NAME = "app-tier-instance"
USER_DATA = '''#!/bin/bash
cd /home/ec2-user
python3 backend.py > backend.log 2>&1 &
'''

# AWS setup
ec2 = boto3.resource("ec2", region_name=REGION)
ec2_client = boto3.client("ec2", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)

# Helpers
def get_queue_size():
    try:
        resp = sqs.get_queue_attributes(
            QueueUrl=REQ_QUEUE_URL,
            AttributeNames=["ApproximateNumberOfMessages"]
        )
        return int(resp["Attributes"]["ApproximateNumberOfMessages"])
    except Exception as e:
        logger.error(f"Queue size error: {e}")
        return 0

def get_running_instances():
    filters = [
        {'Name': 'tag:Name', 'Values': [f'{TAG_NAME}*']},
        {'Name': 'instance-state-name', 'Values': ['running', 'pending']}
    ]
    return list(ec2.instances.filter(Filters=filters))

def launch_instance(tag):
    try:
        logger.info(f"Launching instance {tag}")
        ec2_client.run_instances(
            ImageId=AMI_ID,
            InstanceType=INSTANCE_TYPE,
            MinCount=1,
            MaxCount=1,
            KeyName=KEY_NAME,
            SecurityGroupIds=[SECURITY_GROUP_ID],
            UserData=USER_DATA,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': tag}]
            }]
        )
    except Exception as e:
        logger.error(f"Error launching instance: {e}")

def terminate_instance(instance_id):
    try:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        logger.info(f"Terminating instance: {instance_id}")
        instance = ec2.Instance(instance_id)
        instance.wait_until_terminated()
        logger.info(f"Instance {instance_id} terminated.")
    except Exception as e:
        logger.warning(f"Error terminating instance {instance_id}: {e}")

def terminate_all_instances():
    instances = get_running_instances()
    if not instances:
        return
    ids = [inst.id for inst in instances]
    logger.info(f"Terminating instances: {ids}")
    threads = []
    for iid in ids:
        t = threading.Thread(target=terminate_instance, args=(iid,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

# Autoscaler Logic
def controller_loop():
    logger.info("Starting autoscaling controller...")
    terminate_all_instances()

    empty_queue_counter = 0
    scaled_out_once = False

    while True:
        queue_size = get_queue_size()
        running_instances = get_running_instances()
        current_count = len(running_instances)

        logger.info(f"[AutoScale] Messages in queue: {queue_size} | Running instances: {current_count}")

        if queue_size > current_count:
            if current_count >= MAX_INSTANCES:
                logger.info("Reached MAX_INSTANCES. Skipping launch.")
            else:
                to_launch = min(queue_size - current_count, MAX_INSTANCES - current_count)
                threads = []
                for i in range(to_launch):
                    tag = f"{TAG_NAME}-{int(time.time())}-{i}"
                    t = threading.Thread(target=launch_instance, args=(tag,))
                    t.start()
                    threads.append(t)
                for t in threads:
                    t.join()
                scaled_out_once = True
                empty_queue_counter = 0

        elif queue_size == 0 and scaled_out_once:
            empty_queue_counter += 1
            if empty_queue_counter >= 5 and current_count > 0:
                logger.info("Queue has been empty for 5 consecutive checks. Scaling in...")
                terminate_all_instances()
                empty_queue_counter = 0
                scaled_out_once = False
        else:
            empty_queue_counter = 0

        time.sleep(1)

if __name__ == "__main__":
    controller_loop()
