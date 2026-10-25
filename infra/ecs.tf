# ECS Cluster — logical grouping for all three services
resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-cluster"

  # Enable CloudWatch Container Insights for CPU/memory metrics per task
  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name    = "${var.project_name}-cluster"
    Project = var.project_name
  }
}

# ─────────────────────────────────────────────
# GATEWAY SERVICE
# ─────────────────────────────────────────────

resource "aws_ecs_task_definition" "gateway" {
  family                   = "${var.project_name}-gateway"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.ecs_task_cpu
  memory                   = var.ecs_task_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name  = "gateway"
      image = var.gateway_image != "" ? var.gateway_image : "${aws_ecr_repository.gateway.repository_url}:latest"

      portMappings = [
        {
          containerPort = var.gateway_port
          protocol      = "tcp"
        }
      ]

      environment = [
        { name = "REDIS_ADDR", value = "${aws_elasticache_cluster.redis.cache_nodes[0].address}:6379" },
        { name = "TASK_ID", value = "ecs-gateway" },
        { name = "DB_DSN", value = "postgres://notif:${var.db_password}@${aws_db_instance.postgres.address}:5432/notifications?sslmode=require" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.gateway.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "gateway"
        }
      }

      essential = true
    }
  ])

  tags = {
    Project = var.project_name
  }
}

resource "aws_ecs_service" "gateway" {
  name            = "${var.project_name}-gateway"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.gateway.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.public_1.id, aws_subnet.public_2.id]
    security_groups  = [aws_security_group.gateway.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.gateway.arn
    container_name   = "gateway"
    container_port   = var.gateway_port
  }

  depends_on = [aws_lb_listener.gateway]

  tags = {
    Project = var.project_name
  }
}

# ─────────────────────────────────────────────
# INGESTION SERVICE
# ─────────────────────────────────────────────

resource "aws_ecs_task_definition" "ingestion" {
  family                   = "${var.project_name}-ingestion"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.ecs_task_cpu
  memory                   = var.ecs_task_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name  = "ingestion"
      image = var.ingestion_image != "" ? var.ingestion_image : "${aws_ecr_repository.ingestion.repository_url}:latest"

      portMappings = [
        {
          containerPort = var.ingestion_port
          protocol      = "tcp"
        }
      ]

      environment = [
        { name = "KAFKA_BROKERS", value = aws_msk_cluster.main.bootstrap_brokers },
        { name = "KAFKA_TOPIC", value = "worker-events" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ingestion.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ingestion"
        }
      }

      essential = true
    }
  ])

  tags = {
    Project = var.project_name
  }
}

resource "aws_ecs_service" "ingestion" {
  name            = "${var.project_name}-ingestion"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.ingestion.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.public_1.id, aws_subnet.public_2.id]
    security_groups  = [aws_security_group.ingestion.id]
    assign_public_ip = true
  }

  tags = {
    Project = var.project_name
  }
}

# ─────────────────────────────────────────────
# FAN-OUT WORKER SERVICE
# ─────────────────────────────────────────────

resource "aws_ecs_task_definition" "fanout" {
  family                   = "${var.project_name}-fanout"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.ecs_task_cpu
  memory                   = var.ecs_task_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name  = "fanout"
      image = var.fanout_image != "" ? var.fanout_image : "${aws_ecr_repository.fanout.repository_url}:latest"

      environment = [
        { name = "KAFKA_BROKERS", value = aws_msk_cluster.main.bootstrap_brokers },
        { name = "KAFKA_TOPIC", value = "worker-events" },
        { name = "KAFKA_GROUP_ID", value = "worker-skeleton" },
        { name = "REDIS_ADDR", value = "${aws_elasticache_cluster.redis.cache_nodes[0].address}:6379" },
        { name = "DB_DSN", value = "postgres://notif:${var.db_password}@${aws_db_instance.postgres.address}:5432/notifications?sslmode=require" },
        { name = "FANOUT_THRESHOLD", value = "1000" },
        { name = "NOTIFICATION_MODE", value = "FAN_OUT_WRITE" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.fanout.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "fanout"
        }
      }

      essential = true
    }
  ])

  tags = {
    Project = var.project_name
  }
}

resource "aws_ecs_service" "fanout" {
  name            = "${var.project_name}-fanout"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.fanout.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.public_1.id, aws_subnet.public_2.id]
    security_groups  = [aws_security_group.fanout.id]
    assign_public_ip = true
  }

  tags = {
    Project = var.project_name
  }
}