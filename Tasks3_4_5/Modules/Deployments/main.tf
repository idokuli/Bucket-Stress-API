provider "aws" {
  region = var.aws_region
}

data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023*-x86_64"]
  }
}

module "custom_vpc_ec2" {
  source           = "../Custom_vpc_ec2"
  vpc_cidr         = "10.0.0.0/16"
  subnet_count     = 3
  ami_id           = data.aws_ami.amazon_linux_2023.id
  instance_type    = "t3.micro"
  assign_public_ip = true
}
module "lb_tg_as" {
  source                 = "../LB_TG_AS"
  aws_region             = var.aws_region
  vpc_id                 = module.custom_vpc_ec2.vpc_id
  public_subnet_ids      = module.custom_vpc_ec2.public_subnet_id
  security_group_id      = module.custom_vpc_ec2.sg_id
  source_instance_id     = module.custom_vpc_ec2.instance_id
  instance_type          = "t3.micro"
  lb_name                = "lb"
  lb_type                = "application"
  lb_internal            = false
  tg_name                = "tg"
  tg_port                = 80
  tg_protocol            = "HTTP"
  tg_type                = "instance"
  min_size               = 1
  max_size               = 3
  desired_capacity       = 1
  tg_arn                 = "tg_arn"
  tg_id                  = "tg_id"
  lb_port                = 80
  lb_protocol            = "HTTP"
  lb_arn                 = "lb_arn"
  tg_action_type         = "forward"
  tg_action_forward      = "tg_arn"
  policy_name            = "policy"
  policy_type            = "TargetTrackingScaling"
  target_value           = 50
  predefined_metric_type = "ASGAverageCPUUtilization"
  min_healthy_percentage = 75
  max_healthy_percentage = 125
  key_name               = module.custom_vpc_ec2.key_name
}
resource "aws_s3_bucket" "bucket" {
  bucket        = var.bucket_name
  force_destroy = true
}
