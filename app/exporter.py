import time
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from prometheus_client import Gauge
import logging


class MetricExporter:
    def __init__(self, polling_interval_seconds, metric_name, aws_assumed_role_name, group_by, targets):
        self.polling_interval_seconds = polling_interval_seconds
        self.metric_name = metric_name
        self.targets = targets
        self.aws_assumed_role_name = aws_assumed_role_name
        self.group_by = group_by
        self.labels = set(targets[0].keys())
        self.labels.add("ChargeType")
        if group_by["enabled"]:
            for group in group_by["groups"]:
                self.labels.add(group["label_name"])
        self.aws_daily_cost_usd = Gauge(
            self.metric_name, "Daily cost of an AWS account in USD", self.labels)

    def run_metrics_loop(self):
        while True:
            for aws_account in self.targets:
                logging.info(f"Querying cost data for AWS account: {aws_account['Publisher']}")
                try:
                    self.fetch(aws_account)
                except Exception as e:
                    logging.error(f"Error fetching data for account {aws_account['Publisher']}: {e}")
                    continue
            time.sleep(self.polling_interval_seconds)

    def get_aws_account_session(self, account_id):
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=f"arn:aws:iam::{account_id}:role/{self.aws_assumed_role_name}",
            RoleSessionName=f"AssumeRoleSession_{account_id}"  # Unique session name
        )
        return boto3.Session(
            aws_access_key_id=assumed_role_object['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role_object['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role_object['Credentials']['SessionToken']
        )

    def query_aws_cost_explorer(self, aws_client, group_by):
        end_date = datetime.today() - relativedelta(days=2)  # Set end_date to 1 day before yesterday
        start_date = end_date - relativedelta(days=1)  # Set start_date to 2 days ago
        groups = list()
        if group_by["enabled"]:
            for group in group_by["groups"]:
                groups.append({"Type": group["type"], "Key": group["key"]})
    
        return aws_client.get_cost_and_usage(
            TimePeriod={"Start": start_date.strftime("%Y-%m-%d"), "End": end_date.strftime("%Y-%m-%d")},
            Filter={"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            GroupBy=groups
        )["ResultsByTime"]

    def fetch(self, aws_account):
        session = self.get_aws_account_session(aws_account["Publisher"])
        aws_client = session.client("ce", region_name="us-east-1")
        cost_response = self.query_aws_cost_explorer(aws_client, self.group_by)
        for result in cost_response:
            if not self.group_by["enabled"]:
                cost = float(result["Total"]["UnblendedCost"]["Amount"])
                self.aws_daily_cost_usd.labels(**aws_account, ChargeType="Usage").set(cost)
            else:
                merged_minor_cost = 0
                for item in result["Groups"]:
                    cost = float(item["Metrics"]["UnblendedCost"]["Amount"])

                    group_key_values = dict()
                    for i in range(len(self.group_by["groups"])):
                        if self.group_by["groups"][i]["type"] == "TAG":
                            value = item["Keys"][i].split("$")[1]
                        else:
                            value = item["Keys"][i]
                        group_key_values.update({self.group_by["groups"][i]["label_name"]: value})

                    if self.group_by["merge_minor_cost"]["enabled"] and cost < self.group_by["merge_minor_cost"]["threshold"]:
                        merged_minor_cost += cost
                    else:
                        self.aws_daily_cost_usd.labels(**aws_account, **group_key_values, ChargeType="Usage").set(cost)

                if merged_minor_cost > 0:
                    group_key_values = dict()
                    for i in range(len(self.group_by["groups"])):
                        group_key_values.update({self.group_by["groups"][i]["label_name"]: self.group_by["merge_minor_cost"]["tag_value"]})
                    self.aws_daily_cost_usd.labels(**aws_account, **group_key_values, ChargeType="Usage").set(merged_minor_cost)
