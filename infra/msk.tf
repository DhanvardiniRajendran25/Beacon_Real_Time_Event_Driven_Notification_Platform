# MSK Configuration — enables auto topic creation
# By default MSK has auto.create.topics.enable=false unlike local Docker Kafka
resource "aws_msk_configuration" "main" {
  name           = "${var.project_name}-kafka-config"
  kafka_versions = ["3.5.1"]

  server_properties = <<-EOF
    auto.create.topics.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=3
  EOF
}

# MSK Cluster — managed Kafka on AWS
resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project_name}-kafka"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type  = var.kafka_instance_type
    client_subnets = [aws_subnet.public_1.id, aws_subnet.public_2.id]
    storage_info {
      ebs_storage_info {
        volume_size = 10
      }
    }
    security_groups = [aws_security_group.msk.id]
  }

  client_authentication {
    unauthenticated = true
  }

  # Reference the configuration above to enable auto topic creation
  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = false
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.fanout.name
      }
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  tags = {
    Name    = "${var.project_name}-kafka"
    Project = var.project_name
  }
}