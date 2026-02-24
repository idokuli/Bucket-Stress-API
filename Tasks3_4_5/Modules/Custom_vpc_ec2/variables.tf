variable "vpc_cidr" {
  type = string
}
variable "subnet_count" {
  type = number
}
variable "instance_type" {
  type = string
}
variable "ami_id" {
  type = string
}
variable "assign_public_ip" {
  type = bool
}
