#!/bin/bash
set -o errexit

yum install -y pssh python27-scipy python27-numpy python27-pip vim patch
HADOOP_HOME=/root/ephemeral-hdfs pip-2.7 install pydoop
pip-2.7 install awscli

# spark-ec2 -r us-west-2 -t r3.4xlarge -s 12 -k jey@kallisti -i ~/.ssh/id_rsa --copy-aws-credentials --user-data=$PWD/setup_ec2.sh launch sc2015
