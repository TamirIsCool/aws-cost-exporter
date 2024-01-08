#!/usr/bin/python
# -*- coding:utf-8 -*-
# Filename: exporter.py

import os
import sys
import argparse
from app.exporter import MetricExporter
from envyaml import EnvYAML
from prometheus_client import start_http_server
import logging


def get_configs():
    parser = argparse.ArgumentParser(
        description="AWS Cost Exporter, exposing AWS cost data as Prometheus metrics.")
    parser.add_argument("-c", "--config", required=True,
                        help="The config file (exporter_config.yaml) for the exporter")
    args = parser.parse_args()

    if (not os.path.exists(args.config)):
        logging.error(
            "AWS Cost Exporter config file does not exist, or it is not a file!")
        sys.exit(1)

    config = EnvYAML(args.config)

    # config validation
    if config["group_by.enabled"]:
        if len(config["group_by.groups"]) < 1 or len(config["group_by.groups"]) > 2:
            logging.error(
                "If group_by is enabled, there should be at leaest one group, and at most two groups!")
            sys.exit(1)

    if len(config["target_aws_accounts"]) == 0:
        logging.error(
            "There should be at leaest one target AWS accounts defined in the config!")
        sys.exit(1)

    labels = config["target_aws_accounts"][0].keys()

    if "Publisher" not in labels:
        logging.error("Publisher is a mandatory key in target_aws_accounts!")
        sys.exit(1)

    for i in range(1, len(config["target_aws_accounts"])):
        if labels != config["target_aws_accounts"][i].keys():
            logging.error(
                "All the target AWS accounts should have the same set of keys (labels)!")
            sys.exit(1)

    return config


def main(config):
    app_metrics = MetricExporter(
        polling_interval_seconds=config["polling_interval_seconds"],
        metric_name=config["metric_name"],
        aws_assumed_role_name=config["aws_assumed_role_name"],
        group_by=config["group_by"],
        targets=config["target_aws_accounts"]
    )
    start_http_server(config["exporter_port"])
    app_metrics.run_metrics_loop()


if __name__ == "__main__":
    logger_format = "%(asctime)-15s %(levelname)-8s %(message)s"
    logging.basicConfig(level=logging.INFO, format=logger_format)
    config = get_configs()
    main(config)
