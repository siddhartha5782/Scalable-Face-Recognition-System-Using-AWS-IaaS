import boto3
from io import BytesIO
from PIL import Image
import json
import traceback
import time
import logging
from face_recognition import face_match

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

region = 'us-east-1'
req_queue = 'https://sqs.us-east-1.amazonaws.com/020034636866/1233628905-req-queue'
resp_queue = 'https://sqs.us-east-1.amazonaws.com/020034636866/1233628905-resp-queue'
bucket_in = '1233628905-in-bucket'
bucket_out = '1233628905-out-bucket'

# AWS clients with configuration for optimal throughput
sqs = boto3.client("sqs", region_name=region, 
                  config=boto3.session.Config(
                      retries={'max_attempts': 10},
                      max_pool_connections=20
                  ))
s3 = boto3.client("s3", region_name=region,
                 config=boto3.session.Config(
                     retries={'max_attempts': 10},
                     max_pool_connections=20
                 ))

def process_message(msg):
    try:
        data = json.loads(msg['Body'])
        file_name = data.get("file_name")
        s3_url = data.get("s3_url")
        message_id = data.get("message_id")
        
        logger.info(f"Processing message: {message_id}, file: {file_name}")
        
        # Extract S3 key from URL
        key = s3_url.split(f"https://{bucket_in}.s3.amazonaws.com/")[1]
        
        # Get image from S3
        s3_response = s3.get_object(Bucket=bucket_in, Key=key)
        image_bytes = s3_response['Body'].read()
        image = Image.open(BytesIO(image_bytes))
        
        # Process image with face recognition
        result = face_match(image, 'data.pt')[0]
        logger.info(f"Face recognition result for {file_name}: {result}")
        
        # Save result to output bucket
        output_key = file_name.split(".")[0]
        s3.put_object(Bucket=bucket_out, Key=output_key, Body=result)
        
        # Send response back to queue
        response_data = json.dumps({"result": result, "message_id": message_id})
        sqs.send_message(QueueUrl=resp_queue, MessageBody=response_data)
        logger.info(f"Response for {message_id} sent to response queue")
        
        return True
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        traceback.print_exc()
        
        # If possible, send error response
        try:
            if message_id:
                error_response = json.dumps({"result": f"Error: {str(e)}", "message_id": message_id})
                sqs.send_message(QueueUrl=resp_queue, MessageBody=error_response)
                logger.info(f"Error response for {message_id} sent to response queue")
        except:
            pass
        
        return False

def main():
    empty_receives = 0
    backoff_time = 1  # Initial backoff time in seconds
    max_backoff = 10  # Maximum backoff time
    
    logger.info("Starting app-tier backend service...")
    
    while True:
        try:
            # Receive messages with long polling
            response = sqs.receive_message(
                QueueUrl=req_queue,
                MaxNumberOfMessages=10,  # Process multiple messages at once
                WaitTimeSeconds=20  # Long polling to reduce empty receives
            )
            
            # Process messages if any
            if "Messages" in response:
                empty_receives = 0  # Reset backoff
                backoff_time = 1    # Reset backoff time
                
                logger.info(f"Received {len(response['Messages'])} messages")
                
                # Process each message
                for message in response["Messages"]:
                    success = process_message(message)
                    
                    # Delete message if processed successfully
                    if success:
                        sqs.delete_message(
                            QueueUrl=req_queue, 
                            ReceiptHandle=message["ReceiptHandle"]
                        )
                        logger.info(f"Deleted message from request queue")
            else:
                # Implement backoff for empty receives
                empty_receives += 1
                if empty_receives > 5:  # After 5 consecutive empty receives
                    logger.info(f"No messages received, backing off for {backoff_time} seconds")
                    time.sleep(backoff_time)
                    backoff_time = min(backoff_time * 2, max_backoff)  # Exponential backoff
        
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            time.sleep(5)  # Sleep before retry on error

if __name__ == '__main__':
    main()
