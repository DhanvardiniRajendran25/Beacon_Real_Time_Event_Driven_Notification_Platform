# Infrastructure — Distributed Real-Time Notification System

This folder contains Terraform code to deploy the full notification system stack on AWS. Each teammate can deploy their own independent stack by running these commands in their own AWS account.

---

## What Gets Deployed

| Resource               | Purpose                                              |
| ---------------------- | ---------------------------------------------------- |
| VPC + 2 public subnets | Network for all services                             |
| ECS Fargate Cluster    | Runs all three services                              |
| ECR (3 repos)          | Docker image storage for gateway, ingestion, fanout  |
| ALB + Target Group     | Routes WebSocket traffic to gateway with stickiness  |
| ElastiCache (Redis)    | Pub/Sub routing + user registration                  |
| MSK (Kafka)            | Event streaming between ingestion and fan-out worker |
| CloudWatch Log Groups  | Logs per service, 7 day retention                    |
| IAM Roles              | ECS task execution and runtime permissions           |
| Security Groups        | Traffic rules between all services                   |

---

## Prerequisites

- Terraform >= 1.3.0 installed (`terraform -version`)
- AWS CLI installed and configured
- Docker Desktop running (for building and pushing images)
- `wscat` installed for testing (`npm install -g wscat`)

---

## Deploy Steps

### 1. Configure AWS credentials

For sandbox:

```bash
aws configure
# enter your Access Key ID, Secret Access Key, region: us-east-1
```

For Learner Lab:

```bash
# paste credentials from Learner Lab "AWS Details" into ~/.aws/credentials
[default]
aws_access_key_id = ASIA...
aws_secret_access_key = ...
aws_session_token = ...
```

### 2. Initialize Terraform

```bash
cd infra
terraform init
```

### 3. Preview the deployment

```bash
terraform plan
```

### 4. Deploy

```bash
terraform apply
```

Type `yes` when prompted. MSK takes 15-25 minutes — let it run.

### 5. Note the outputs

After apply completes, Terraform prints the key endpoints:

```
alb_dns                 = "ws://notif-system-alb-xxx.us-east-1.elb.amazonaws.com:8080/ws?user_id=YOUR_USER_ID"
redis_endpoint          = "notif-system-redis.xxx.use1.cache.amazonaws.com:6379"
kafka_bootstrap_brokers = "b-1.notif-system-kafka.xxx.kafka.us-east-1.amazonaws.com:9092,..."
ecr_gateway_url         = "ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-gateway"
ecr_ingestion_url       = "ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-ingestion"
ecr_fanout_url          = "ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-fanout"
```

---

## Build and Push Docker Images

After the stack is deployed, build and push each service image to ECR.

### Authenticate Docker to ECR

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
```

### Gateway

```bash
docker build -t notif-system-gateway -f gateway/Dockerfile .
docker tag notif-system-gateway:latest ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-gateway:latest
docker push ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-gateway:latest
```

### Ingestion

```bash
docker build -t notif-system-ingestion -f ingestion/Dockerfile .
docker tag notif-system-ingestion:latest ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-ingestion:latest
docker push ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-ingestion:latest
```

### Fan-out Worker

```bash
docker buildx build --platform linux/amd64 --load --target worker -t notif-system-fanout -f fanout/Dockerfile .
docker tag notif-system-fanout:latest ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-fanout:latest
docker push ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/notif-system-fanout:latest
```

Replace `ACCOUNT_ID` with your AWS account ID from the Terraform outputs.

---

## Testing End-to-End

Once all images are pushed and ECS tasks are running:

```bash
# connect a WebSocket client to the gateway via the ALB
wscat -c "ws://YOUR_ALB_DNS:8080/ws?user_id=user123"

# fire a test event at the ingestion API
curl -X POST http://INGESTION_TASK_IP:3000/event \
  -H "Content-Type: application/json" \
  -d '{"type":"POST","from_user":"alice","detail":"Alice posted a photo"}'
```

The notification should appear instantly in the wscat terminal.

---

## Tear Down

Always destroy the stack after testing to avoid unnecessary AWS charges.

```bash
terraform destroy
```

Type `yes` when prompted. MSK takes a few minutes to delete as well.

---

## Customizing the Deployment

All configurable parameters are in `variables.tf`. The most useful ones:

| Variable              | Default          | Notes                                                                        |
| --------------------- | ---------------- | ---------------------------------------------------------------------------- |
| `project_name`        | `notif-system`   | Change this if two teammates deploy simultaneously to avoid naming conflicts |
| `aws_region`          | `us-east-1`      | Do not change for Learner Lab                                                |
| `ecs_task_cpu`        | `256`            | Increase for experiments                                                     |
| `ecs_task_memory`     | `512`            | Increase for Experiment 2                                                    |
| `kafka_instance_type` | `kafka.t3.small` | Minimum supported by Learner Lab                                             |
| `redis_node_type`     | `cache.t3.micro` | Sufficient for all experiments                                               |

---

## File Structure

```
infra/
├── main.tf             — provider and backend config
├── variables.tf        — all configurable parameters
├── outputs.tf          — printed endpoints after apply
├── vpc.tf              — VPC, subnets, internet gateway, route tables
├── security_groups.tf  — traffic rules between all services
├── ecr.tf              — ECR repos for all three services
├── iam.tf              — ECS execution and task IAM roles
├── elasticache.tf      — managed Redis
├── msk.tf              — managed Kafka (MSK)
├── alb.tf              — load balancer for WebSocket gateway
├── ecs.tf              — ECS cluster, task definitions, services
└── infra.md            — this file
```
