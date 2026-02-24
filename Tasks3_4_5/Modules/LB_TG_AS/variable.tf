variable "aws_region" {
  type = string
}

variable "instance_type" {
  type = string
}
variable "vpc_id" {
  type = string
}
variable "public_subnet_ids" {
  type = list(string)
}
variable "security_group_id" {
  type = string
}
variable "source_instance_id" {
  type = string
}
variable "lb_name" {
  type = string
}
variable "lb_type" {
  type = string
}
variable "lb_internal" {
  type = bool
}
variable "tg_name" {
  type = string
}
variable "tg_port" {
  type = number
}
variable "tg_protocol" {
  type = string
}
variable "tg_type" {
  type = string
}
variable "min_size" {
  type = number
}
variable "max_size" {
  type = number
}
variable "desired_capacity" {
  type = number
}
variable "tg_arn" {
  type = string
}
variable "tg_id" {
  type = string
}
variable "lb_port" {
  type = number
}
variable "lb_protocol" {
  type = string
}
variable "lb_arn" {
  type = string
}
variable "tg_action_type" {
  type = string
}
variable "tg_action_forward" {
  type = string
}
variable "policy_name" {
  type = string
}
variable "policy_type" {
  type = string
}
variable "target_value" {
  type = number
}
variable "predefined_metric_type" {
  type = string
}
variable "min_healthy_percentage" {
  type = number
}

variable "max_healthy_percentage" {
  type = number
}

variable "key_name" {
  type = string
}
