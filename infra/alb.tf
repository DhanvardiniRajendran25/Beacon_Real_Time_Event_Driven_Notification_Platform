# Application Load Balancer — routes incoming WebSocket and HTTP traffic to the gateway
resource "aws_lb" "gateway" {
  name               = "${var.project_name}-alb"
  internal           = false # internet-facing so clients can connect
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = [aws_subnet.public_1.id, aws_subnet.public_2.id]

  tags = {
    Name    = "${var.project_name}-alb"
    Project = var.project_name
  }
}

# Target Group — the ALB forwards traffic to ECS gateway tasks here
# Stickiness is enabled so WebSocket connections stay on the same ECS task
resource "aws_lb_target_group" "gateway" {
  name        = "${var.project_name}-gateway-tg"
  port        = var.gateway_port
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip" # required for ECS Fargate — tasks don't have fixed instance IDs

  # Stickiness — critical for WebSocket connections
  # Once a client connects to a gateway task, all subsequent requests
  # must go to the same task for the lifetime of the connection
  stickiness {
    type            = "lb_cookie"
    cookie_duration = 86400 # 1 day in seconds
    enabled         = true
  }

  # Health check — ALB pings /health to verify the gateway task is alive
  # ECS will replace tasks that fail health checks
  health_check {
    path                = "/health"
    protocol            = "HTTP"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = {
    Name    = "${var.project_name}-gateway-tg"
    Project = var.project_name
  }
}

# Listener — tells the ALB to forward all traffic on port 8080 to the target group
resource "aws_lb_listener" "gateway" {
  load_balancer_arn = aws_lb.gateway.arn
  port              = 8080
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.gateway.arn
  }
}