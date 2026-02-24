provider "aws" {
  region = var.aws_region
}

resource "aws_lb" "lb" {
  name               = var.lb_name
  internal           = var.lb_internal
  load_balancer_type = var.lb_type
  security_groups    = [var.security_group_id]
  subnets            = var.public_subnet_ids
  tags = {
    Name = var.lb_name
  }
}

resource "aws_lb_target_group" "tg" {
  name        = var.tg_name
  port        = var.tg_port
  protocol    = var.tg_protocol
  target_type = var.tg_type
  vpc_id      = var.vpc_id
  health_check {
    healthy_threshold   = 5
    unhealthy_threshold = 2
    timeout             = 3
    interval            = 10
    path                = "/"
    matcher             = "200"
  }
  tags = {
    Name = var.tg_name
  }
}

resource "aws_lb_listener" "listener" {
  default_action {
    type             = var.tg_action_type
    target_group_arn = aws_lb_target_group.tg.arn
  }
  load_balancer_arn = aws_lb.lb.arn
  port              = var.lb_port
  protocol          = var.lb_protocol
}

resource "aws_ami_from_instance" "ami" {
  source_instance_id = var.source_instance_id
  name               = "ami"
  tags = {
    Name = "ami"
  }
}

resource "aws_launch_template" "launch_template" {
  name          = "launch_template"
  image_id      = aws_ami_from_instance.ami.id
  instance_type = var.instance_type
  key_name      = var.key_name
  monitoring {
    enabled = true
  }

  network_interfaces {
    associate_public_ip_address = true
    security_groups             = [var.security_group_id]
  }

  user_data = base64encode(<<-EOF
              #!/bin/bash
              sleep 180
              stress-ng --cpu 0 --timeout 600 &
              EOF
  )
  tags = {
    Name = "launch_template"
  }
}

resource "aws_autoscaling_group" "asg" {
  name                = "asg"
  min_size            = var.min_size
  max_size            = var.max_size
  desired_capacity    = var.desired_capacity
  vpc_zone_identifier = var.public_subnet_ids
  launch_template {
    id = aws_launch_template.launch_template.id
  }
  target_group_arns = [aws_lb_target_group.tg.arn]
  instance_maintenance_policy {
    min_healthy_percentage = var.min_healthy_percentage
    max_healthy_percentage = var.max_healthy_percentage
  }
  tag {
    key                 = "Name"
    value               = "Web-Server"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_policy" "scale_up" {
  name                   = var.policy_name
  autoscaling_group_name = aws_autoscaling_group.asg.name
  policy_type            = var.policy_type
  target_tracking_configuration {
    target_value = var.target_value
    predefined_metric_specification {
      predefined_metric_type = var.predefined_metric_type
    }
  }
}
