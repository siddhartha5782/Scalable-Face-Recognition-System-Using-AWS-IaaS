import boto3
from flask import Flask, request
import json
import uuid
import threading
import subprocess
import os
import logging
import signal
import time
from botocore.config import Config

# Flask setup
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS region and resources
region = "us-east-1"
req_queue = "https://sqs.us-east-1.amazonaws.com/020034636866/1233628905-req-queue"
resp_queue = "https://sqs.us-east-1.amazonaws.com/020034636866/1233628905-resp-queue"
bucket_name = "1233628905-in-bucket"

# Custom AWS config to increase connection pool
aws_config = Config(
    region_name=region,
    retries={'max_attempts': 10},
    max_pool_connections=100
)

# Global SQS and S3 clients
sqs = boto3.client("sqs", config=aws_config)
s3 = boto3.client("s3", config=aws_config)

# Global response tracking
responses = {}
response_lock = threading.Lock()
autoscaler_process = None

# Start autoscaler at startup using a thread
def start_autoscaler():
    global autoscaler_process
    try:
        logger.info("Starting autoscaler from server.py...")
        # Use Popen to be able to terminate it later
        autoscaler_process = subprocess.Popen(["python3", "controller.py"])
        logger.info(f"Autoscaler started with PID: {autoscaler_process.pid}")
    except Exception as e:
        logger.error(f"Could not start autoscaler: {e}")

# Shutdown hook to clean up resources
def shutdown_hook():
    logger.info("Server shutting down, cleaning up resources...")
    global autoscaler_process
    
    # Terminate autoscaler if running
    if autoscaler_process:
        logger.info(f"Terminating autoscaler process (PID: {autoscaler_process.pid})...")
        try:
            autoscaler_process.terminate()
            autoscaler_process.wait(timeout=10)
            logger.info("Autoscaler process terminated.")
        except Exception as e:
            logger.error(f"Error terminating autoscaler: {e}")
            try:
                autoscaler_process.kill()
            except:
                pass

# Register the shutdown hook
atexit_registered = False
try:
    import atexit
    atexit.register(shutdown_hook)
    atexit_registered = True
except:
    logger.warning("Could not register atexit hook")

# Register signal handlers
def signal_handler(sig, frame):
    logger.info(f"Received signal {sig}, shutting down...")
    shutdown_hook()
    os._exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Start autoscaler on server startups
start_autoscaler()

@app.route("/", methods=["POST"])
def request_handler():
    try:
        if "inputFile" not in request.files:
            return "No file part", 400

        input_file = request.files["inputFile"]
        filename = input_file.filename
        unique_message_id = str(uuid.uuid4())

        # Upload to S3
        s3_key = f"uploads/{filename}"
        s3.upload_fileobj(input_file, bucket_name, s3_key)
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"

        # Send to SQS request queue
        msg = {
            "file_name": filename,
            "s3_url": s3_url,
            "message_id": unique_message_id
        }
        sqs.send_message(QueueUrl=req_queue, MessageBody=json.dumps(msg))
        logger.info(f"Request for {filename} sent to SQS.")

        # Wait for response
        result = wait_for_response(unique_message_id)
        return f"{filename.split('.')[0]}:{result}"

    except Exception as e:
        logger.error(f"Exception occurred: {str(e)}")
        return f"Error: {str(e)}", 500

def wait_for_response(message_id, timeout=300):
    """Wait for a response with a timeout"""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        poll_response_queue()
        
        with response_lock:
            if message_id in responses:
                return responses.pop(message_id)
        
        # Short sleep to prevent tight loop
        time.sleep(0.5)
    
    # If timeout occurs
    logger.error(f"Timeout waiting for response to message {message_id}")
    return "Error: Response timeout"

def poll_response_queue():
    try:
        messages = sqs.receive_message(
            QueueUrl=resp_queue,
            MaxNumberOfMessages=10,  # Increased batch size
            WaitTimeSeconds=5  # Reduced wait time
        )
        
        if "Messages" in messages:
            for msg in messages["Messages"]:
                try:
                    body = json.loads(msg["Body"])
                    msg_id = body.get("message_id")
                    result = body.get("result")
                    
                    with response_lock:
                        responses[msg_id] = result
                    
                    # Delete the message after processing
                    sqs.delete_message(QueueUrl=resp_queue, ReceiptHandle=msg["ReceiptHandle"])
                    logger.info(f"Response for {msg_id} processed and deleted.")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
    except Exception as e:
        logger.error(f"Error polling response queue: {e}")

if __name__ == "__main__":
    logger.info("Starting Flask server...")
    try:
        # Start with threading to handle concurrent requests
        app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        # Ensure shutdown cleanup happens
        if not atexit_registered:
            shutdown_hook()
