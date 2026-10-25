# RDS Subnet Group — tells RDS which subnets it can live in
# Requires at least 2 subnets in different AZs
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-rds-subnet-group"
  subnet_ids = [aws_subnet.public_1.id, aws_subnet.public_2.id]

  tags = {
    Project = var.project_name
  }
}

# RDS PostgreSQL instance
# Single instance — no replication needed for Week 2 testing
resource "aws_db_instance" "postgres" {
  identifier        = "${var.project_name}-postgres"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = "db.t3.micro" # cheapest option — sufficient for testing
  allocated_storage = 20            # 20GB minimum for PostgreSQL on RDS

  db_name  = "notifications"
  username = "notif"
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  # allow public access so we can run init.sql from local machine
  publicly_accessible = true

  # skip final snapshot on destroy — saves time when tearing down for experiments
  skip_final_snapshot = true

  tags = {
    Name    = "${var.project_name}-postgres"
    Project = var.project_name
  }
}

# Run init.sql once when the RDS instance is first created.
# Triggered by the RDS instance ID — only re-runs if the DB is destroyed and recreated.
resource "null_resource" "db_init" {
  triggers = {
    db_instance_id = aws_db_instance.postgres.id
  }

  provisioner "local-exec" {
    command = "psql postgresql://${aws_db_instance.postgres.username}:${var.db_password}@${aws_db_instance.postgres.endpoint}/${aws_db_instance.postgres.db_name} -f ${path.module}/../db/init.sql"
  }
}