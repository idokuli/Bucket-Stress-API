output "vpc_id" {
  value = aws_vpc.main_vpc.id
}
output "instance_id" {
  value = aws_instance.instance1.id
}
output "instance_public_ip" {
  value = aws_instance.instance1.public_ip
}
output "sg_id" {
  value = aws_security_group.sg.id
}
output "public_subnet_id" {
  value = aws_subnet.public_subnet[*].id
}
output "security_group_id" {
  value = aws_security_group.sg.id
}
output "vpc_cidr" {
  value = aws_vpc.main_vpc.cidr_block
}
output "key_name" {
  value = aws_key_pair.deployer.key_name
}
