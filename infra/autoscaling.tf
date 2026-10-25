# Auto Scaling for ECS services — CPU-based target tracking.
# All three services share the same configuration:
#   - Target: 70% average CPU utilization
#   - Scale-out cooldown: 60s (conservative — avoid thrashing on traffic spikes)
#   - Scale-in cooldown:  10s (aggressive — shed capacity quickly after load drops)
#   - Min tasks: 1, Max tasks: 5
#
# Note: the fanout worker consumes from a Kafka topic with 3 partitions, so
# scaling beyond 3 tasks will not increase Kafka throughput — extra tasks will
# sit idle as consumers. CPU-based scaling may still add tasks under heavy
# fan-out-on-write load (DB writes + Redis publishes), so the max of 5 is kept
# consistent but real Kafka parallelism is capped at 3.

# ─────────────────────────────────────────────
# GATEWAY
# ─────────────────────────────────────────────

resource "aws_appautoscaling_target" "gateway" {
  max_capacity       = 5
  min_capacity       = 1
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.gateway.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "gateway_cpu" {
  name               = "${var.project_name}-gateway-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.gateway.resource_id
  scalable_dimension = aws_appautoscaling_target.gateway.scalable_dimension
  service_namespace  = aws_appautoscaling_target.gateway.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 70.0
    scale_out_cooldown = 60
    scale_in_cooldown  = 10
  }
}

# ─────────────────────────────────────────────
# INGESTION
# ─────────────────────────────────────────────

resource "aws_appautoscaling_target" "ingestion" {
  max_capacity       = 5
  min_capacity       = 1
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.ingestion.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "ingestion_cpu" {
  name               = "${var.project_name}-ingestion-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ingestion.resource_id
  scalable_dimension = aws_appautoscaling_target.ingestion.scalable_dimension
  service_namespace  = aws_appautoscaling_target.ingestion.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 70.0
    scale_out_cooldown = 60
    scale_in_cooldown  = 10
  }
}

# ─────────────────────────────────────────────
# FANOUT WORKER
# ─────────────────────────────────────────────

resource "aws_appautoscaling_target" "fanout" {
  max_capacity       = 5
  min_capacity       = 1
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.fanout.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "fanout_cpu" {
  name               = "${var.project_name}-fanout-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.fanout.resource_id
  scalable_dimension = aws_appautoscaling_target.fanout.scalable_dimension
  service_namespace  = aws_appautoscaling_target.fanout.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 70.0
    scale_out_cooldown = 60
    scale_in_cooldown  = 10
  }
}
