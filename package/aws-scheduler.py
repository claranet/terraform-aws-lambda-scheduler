#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import sys
import os
import json
import logging
import datetime
import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)

aws_region = None

create_schedule_tag_force = os.getenv('SCHEDULE_TAG_FORCE', 'False')
create_schedule_tag_force = create_schedule_tag_force.capitalize()
logger.info("create_schedule_tag_force is %s." % create_schedule_tag_force)

rds_schedule = os.getenv('RDS_SCHEDULE', 'True')
rds_schedule = rds_schedule.capitalize()
logger.info("rds_schedule is %s." % rds_schedule)

ec2_schedule = os.getenv('EC2_SCHEDULE', 'True')
ec2_schedule = ec2_schedule.capitalize()
logger.info("ec2_schedule is %s." % ec2_schedule)


default_schedule = {
    "mon": {
        "start": 7,
        "stop": 20
    },
    "tue": {
        "start": 7,
        "stop": 20
    },
    "wed": {
        "start": 7,
        "stop": 20
    },
    "thu": {
        "start": 7,
        "stop": 20
    },
    "fri": {
        "start": 7,
        "stop": 20
    }
}


def init():
    """
        Setup AWS connection
    """
    aws_region = os.getenv('AWS_REGION', 'us-east-1')

    global ec2
    logger.info('-----> Connecting to region "{}"'.format(aws_region))
    ec2 = boto3.resource('ec2', region_name=aws_region)
    logger.info('-----> Connected to region "{}"'.format(aws_region))


def create_schedule_tag(instance):
    """
    Add default 'schedule' tag to instance.
    (Only if instance.id not excluded
    and create_schedule_tag_force variable is True.
    """
    exclude_list = os.environ.get('EXCLUDE').split(',')

    autoscaling = False
    for tag in instance.tags:
        if 'aws:autoscaling:groupName' in tag['Key']:
            autoscaling = True

    if (
        (create_schedule_tag_force == 'True')
        and (instance.id not in exclude_list)
        and (not autoscaling)
    ):
        try:
            schedule_tag = os.getenv('TAG', 'schedule')
            tag_value = os.getenv('DEFAULT', json.dumps(default_schedule))
            logger.info(
                'About to create {} tag on '
                'EC2 instance {} with value: {}'.format(
                    schedule_tag,
                    instance.id,
                    tag_value
                )
            )
            tags = [{
                'Key': schedule_tag,
                'Value': tag_value
            }]
            instance.create_tags(Tags=tags)
        except Exception as e:
            logger.error('Error adding Tag to EC2 instance: {}'.format(e))
    else:
        if (autoscaling):
            logger.info(
                'Ignoring EC2 instance {}. '
                'It is part of an auto scaling group'.format(
                    instance.id
                )
            )
        else:
            logger.info(
                "No 'schedule' tag found on EC2 instance {}. "
                "Use create_schedule_tag_force option "
                "to create the tag automagically".format(instance.id)
            )


def check():
    """
    Loop EC2 instances and check if a 'schedule' tag has been set.
    Next, evaluate value and start/stop instance if needed.
    """
    # Get all reservations.
    instances = ec2.instances.filter(
        Filters=[{
            'Name': 'instance-state-name',
            'Values': ['pending', 'running', 'stopping', 'stopped']
        }]
    )

    # Get current day + hour
    # (using gmt by default if time parameter not set to local)
    time_zone = os.getenv('TIME', 'gmt')
    if time_zone == 'local':
        hh = int(time.strftime("%H", time.localtime()))
        day = time.strftime("%a", time.localtime()).lower()
        logger.info(
            '-----> Checking for EC2 instances to '
            'start or stop for "local time" hour "{}"'.format(hh)
        )
    else:
        hh = int(time.strftime("%H", time.gmtime()))
        day = time.strftime("%a", time.gmtime()).lower()
        logger.info(
            '-----> Checking for EC2 instances to '
            'start or stop for "gmt" hour "{}"'.format(hh)
        )

    started = []
    stopped = []

    schedule_tag = os.getenv('TAG', 'schedule')
    logger.info("-----> schedule tag is called \"%s\"", schedule_tag)
    if not instances:
        logger.error(
            'Unable to find any EC2 Instances, please check configuration'
        )

    for instance in instances:
        logger.info('Evaluating EC2 instance "{}"'.format(instance.id))

        try:
            data = {}
            for tag in instance.tags:
                if schedule_tag in tag['Key']:
                    data = tag['Value']
                    break
            else:
                # 'schedule' tag not found, create if appropriate.
                create_schedule_tag(instance)

            schedule = json.loads(data)

            try:
                if (
                    hh == schedule[day]['start']
                    and not instance.state["Name"] == 'running'
                ):
                    logger.info(
                        'Starting EC2 instance "{}".'.format(instance.id))
                    started.append(instance.id)
                    ec2.instances.filter(InstanceIds=[instance.id]).start()
            except:  # noqa: E722
                pass  # catch exception if 'start' is not in schedule.

            try:
                if hh == schedule[day]['stop']:
                    logger.info("Stopping time matches")
                if (
                    hh == schedule[day]['stop']
                    and instance.state["Name"] == 'running'
                ):
                    logger.info(
                        'Stopping EC2 instance "{}".'.format(instance.id))
                    stopped.append(instance.id)
                    ec2.instances.filter(InstanceIds=[instance.id]).stop()
            except:  # noqa: E722
                pass  # catch exception if 'stop' is not in schedule.

        except ValueError as e:
            # invalid JSON
            logger.error(
                'Invalid value for tag "schedule" '
                'on EC2 instance "{}", please check!'.format(instance.id))


def rds_init():
    # Setup AWS connection
    aws_region = os.getenv('AWS_REGION', 'us-east-1')

    logger.info('-----> Connecting rds to region "{}"'.format(aws_region))
    global rds
    rds = boto3.client('rds', region_name=aws_region)
    logger.info('-----> Connected rds to region "{}"'.format(aws_region))


def rds_create_schedule_tag(instance):
    """
    Add default 'schedule' tag to instance.
    Only if instance.id not excluded and
    create_schedule_tag_force variable is True.
    """
    exclude_list = os.environ.get('EXCLUDE').split(',')

    if (
        (create_schedule_tag_force == 'True')
        and (instance['DBInstanceIdentifier'] not in exclude_list)
    ):
        try:
            schedule_tag = os.getenv('TAG', 'schedule')
            tag_default = os.getenv('DEFAULT', json.dumps(default_schedule))
            logger.info('json tag_value: {}'.format(tag_default))
            tag = json.loads(tag_default)
            tag_dict = flattenjson(tag, "_")
            tag_value = dict_to_string(tag_dict)
            logger.info(
                'About to create {} tag on '
                'RDS instance {} with value: {}'.format(
                    schedule_tag, instance['DBInstanceIdentifier'], tag_value)
            )
            tags = [{
                "Key": schedule_tag,
                "Value": tag_value
            }]
            rds.add_tags_to_resource(
                ResourceName=instance['DBInstanceArn'], Tags=tags)
        except Exception as e:
            logger.error('Error adding Tag to RDS instance: {}'.format(e))
    else:
        logger.info(
            "No 'schedule' tag found on RDS instance {}. "
            "Use create_schedule_tag_force option "
            "to create the tag automagically".format(
                instance['DBInstanceIdentifier'])
        )


def flattenjson(b, delim):
    val = {}
    for i in b.keys():
        if isinstance(b[i], dict):
            get = flattenjson(b[i], delim)
            for j in get.keys():
                val[i + delim + j] = get[j]
        else:
            val[i] = b[i]
    return val


def dict_to_string(d):
    val = ""
    for k, v in d.items():
        if len(val) == 0:
            val = k + "=" + str(v)
        else:
            val = val + " " + k + "=" + str(v)
    return val


def rds_check():
    """
    Loop RDS instances and check if a 'schedule' tag has been set.
    Next, evaluate value and start/stop instance if needed.
    """
    # Get all reservations.
    instances = rds.describe_db_instances()

    # Get current day + hour
    # (using gmt by default if time parameter not set to local)
    time_zone = os.getenv('TIME', 'gmt')
    if time_zone == 'local':
        hh = time.strftime("%H", time.localtime())
        day = time.strftime("%a", time.localtime()).lower()
        logger.info(
            '-----> Checking for RDS instances to '
            'start or stop for "local time" hour "{}"'.format(hh)
        )
    else:
        hh = time.strftime("%H", time.gmtime())
        day = time.strftime("%a", time.gmtime()).lower()
        logger.info(
            '-----> Checking for RDS instances to '
            'start or stop for "gmt" hour "{}"'.format(hh)
        )

    started = []
    stopped = []

    schedule_tag = os.getenv('TAG', 'schedule')
    logger.info('-----> schedule tag is called "{}"'.format(schedule_tag))
    if not instances:
        logger.error(
            'Unable to find any RDS Instances, please check configuration')

    for instance in instances['DBInstances']:
        logger.info('Evaluating RDS instance "{}".'.format(
            instance['DBInstanceIdentifier']))
        response = rds.list_tags_for_resource(
            ResourceName=instance['DBInstanceArn'])
        taglist = response['TagList']

        try:
            data = ""
            for tag in taglist:
                if schedule_tag in tag['Key']:
                    data = tag['Value']
                    break
            else:
                rds_create_schedule_tag(instance)

            if data == "":
                schedule = []
            else:
                schedule = dict(x.split('=') for x in data.split(' '))

            try:
                if (
                    hh == schedule[day+'_'+'start']
                    and not instance['DBInstanceStatus'] == 'available'
                ):
                    logger.info('Starting RDS instance "{}".'.format(
                        instance['DBInstanceIdentifier']))
                    started.append(instance['DBInstanceIdentifier'])
                    rds.start_db_instance(
                        DBInstanceIdentifier=instance['DBInstanceIdentifier'])
            except:  # noqa: E722
                pass  # catch exception if 'start' is not in schedule.

            try:
                if hh == schedule[day+'_'+'stop']:
                    logger.info("Stopping time matches")
                if (
                    hh == schedule[day+'_'+'stop']
                    and instance['DBInstanceStatus'] == 'available'
                ):
                    logger.info('Stopping RDS instance "%s".'.format(
                        instance['DBInstanceIdentifier']))
                    stopped.append(instance['DBInstanceIdentifier'])
                    rds.stop_db_instance(
                        DBInstanceIdentifier=instance['DBInstanceIdentifier'])
            except:  # noqa: E722
                pass  # catch exception if 'stop' is not in schedule.

        except ValueError as e:
            # invalid JSON
            logger.error(
                'Invalid value for tag "schedule" on RDS instance "{}", '
                'please check!'.format(instance['DBInstanceIdentifier'])
            )


def handler(event, context):
    """
    Main function. Entrypoint for Lambda
    """

    if (ec2_schedule == 'True'):
        init()
        check()

    if (rds_schedule == 'True'):
        rds_init()
        rds_check()


# Manual invocation of the script (only used for testing)
if __name__ == "__main__":
    # Test data
    test = {}
    handler(test, None)
