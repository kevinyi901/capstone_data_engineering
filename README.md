Summary

Goal: Containerize a PDF→Parquet extractor and run it on AWS Fargate. Outputs land in S3 (env=prod/zone=text/), logs go to CloudWatch.

Prereqs:
Docker Desktop, AWS CLI v2 (configured), ECR repo name (pdf-extractor-worker), S3 bucket with the raw/text prefixes.

Build & Push:
Set shell vars (AWS_REGION, ACCOUNT_ID, REPO_NAME, IMAGE_TAG).
Login to ECR → docker buildx build --platform linux/amd64,linux/arm64 -t <ECR_URI> --push .
Use a unique tag per deploy (e.g., timestamp) to avoid stale images.

IAM:

ecsTaskExecutionRole with AmazonECSTaskExecutionRolePolicy (pull image, write logs).

pdfExtractorTaskRole with S3 perms: ListBucket on the bucket, GetObject on env=prod/zone=raw/*, PutObject on env=prod/zone=text/*.
Add SQS perms later if you switch to event-driven.

Networking:
Use your default VPC (two subnets) and a security group that allows all egress.
For quick starts, run in public subnets with assignPublicIp=ENABLED.

Logs:
Create log group /ecs/pdf-extractor; the container logs stream there.

Task Definition:
Family: pdf-extractor-worker (Fargate).
CPU/Mem: 1024/4096 (tune as needed).
Ephemeral storage: 60 GiB (adjust for large PDFs).
Container command (sweep):
--mode sweep --bucket berkeley-capstone-unbarred-2.0-data --in-prefix env=prod/zone=raw/ --out-prefix env=prod/zone=text/ --region us-east-1
Env tuning: OCR_DPI=200, BATCH_SIZE=50, MIN_TEXT_LEN=20, SKIP_IF_EXISTS=true.

Run a One-off Backfill:
Create ECS cluster pdf-extractor.
aws ecs run-task with your subnets/SG and public IP enabled.
Monitor with CloudWatch Logs and aws ecs describe-tasks.

Verify Outputs:
Check S3 at env=prod/zone=text/ for _raw_text.parquet files.

Redeploy New Code:
Build & push a new image tag, register a new task-definition revision pointing to that tag, and run another task.

Scale / Resilience:
If you need continuous processing, create an ECS Service and switch the container to --mode sqs-worker (wire S3 → SQS → ECS).
Increase task memory and ephemeral storage for heavy OCR workloads. Keep SKIP_IF_EXISTS=true for idempotent re-runs.

Common gotchas:

Using latest can cause stale pulls → always use a unique tag.

Missing task role perms (S3/KMS) → check IAM if uploads/downloads fail.

Low visibility timeout with SQS → set to exceed your longest single-PDF runtime.

Not enough temp disk → bump ephemeralStorage beyond 20 GiB.
