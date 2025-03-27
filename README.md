# Scalable Face Recognition System Using AWS IaaS

This project demonstrates a fully custom Infrastructure-as-a-Service (IaaS) deployment on AWS for performing facial recognition at scale. Developed as part of the CSE 546 Cloud Computing course at Arizona State University, the system highlights manual orchestration of AWS resources to build an elastic cloud-native pipeline.

## 🧱 Architecture Overview

The system architecture comprises three main components:

- **Web Tier (EC2 instance)**:
  - Accepts HTTP POST requests containing images.
  - Uploads images to Amazon S3 (input bucket).
  - Publishes job metadata to an Amazon SQS request queue.
  - Listens for prediction results from an SQS response queue and returns them as plain-text HTTP responses.

- **App Tier (EC2 instances with PyTorch)**:
  - Instances launched from a pre-configured AMI.
  - Each instance handles one recognition task at a time: fetches image from S3, runs model inference, stores result in an S3 output bucket, and sends output to the response queue.

- **Autoscaling Controller (Python Script)**:
  - Monitors the SQS request queue in real time.
  - Automatically scales the number of app-tier EC2 instances (between 0 and 15) based on active job load.
  - Ensures instances terminate quickly after completing their tasks to reduce cost and latency.

## 🛠️ Tools & Technologies

- AWS EC2, S3, SQS (no managed services used)
- Python (boto3, http.server)
- PyTorch (for deep learning model inference)
- Shell scripting for deployment automation

## ✅ Performance Summary

- Successfully processed 100 concurrent image recognition requests
- Achieved 100% classification accuracy
- Maintained an average latency below 1 second
- Instances scaled from 0 to 15 dynamically and scaled back to 0 within 5 seconds

## 🚀 Key Takeaway

This project emphasizes building IaaS solutions from the ground up without relying on managed services like Lambda or AWS Auto Scaling. It provides deep insight into infrastructure control, cost efficiency, and cloud-native system design.
