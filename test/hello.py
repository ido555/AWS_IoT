# goal: retrieve Car_1 info
import boto3
import json

# initialize client
from setuptools._distutils.log import warn

iot = boto3.client('iot')

# get current logging levels, format them as JSON, and write them to stdout
response = iot.get_v2_logging_options()
print(json.dumps(response, indent=4))