#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Instructions:
    1. Do not forget to add the --auto-terminate field because EMR clusters are costly. 
    Once you run this script, you’ll be given a unique cluster ID. 
    Check the status of your cluster using `aws emr --cluster-id <cluster_id>`.
    2. We'll be creating an EMR cluster for the exercise.
    3. First, install `awscli` using pip.  You can get instructions for MacOS, Windows, Linux here  on [AWS Documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
    4. This will give you access to create an EMR cluster and EC2 cluster. The EC2 cluster shows a status of all the clusters with your keys, etc. It does a ton of things!
    5. Once it's installed, run the script below to launch your cluster. Be sure to include the appropriate file names within the <> in the code.
"""

# Add your cluster name
aws emr create-cluster 
--name my-udacity-cluster # name of cluster
--use-default-roles  
--release-label emr-5.28.0 
--applications Name=Spark  
--ec2-attributes KeyName=sc-keypair # sc-keypair.pem found in .ssh folder (get this from creating EC2 KeyPair on AWS console)
--instance-type m5.xlarge 
--instance-count 3 
--auto-terminate

# Copy this
aws emr create-cluster --name my-udacity-cluster --use-default-roles  --release-label emr-5.28.0 --applications Name=Spark  --ec2-attributes KeyName=sc-keypair --instance-type m5.xlarge --instance-count 3 --auto-terminate