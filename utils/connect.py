#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 - Present Sepine Tam, Inc. All Rights Reserved
#
# @Author : Sepine Tam
# @Email  : sepinetam@gmail.com
# @File   : connect.py
from typing import Tuple, Any

import os
import yaml
import sys

import pymysql
from pymysql import Connection, MySQLError
from pymysql.cursors import DictCursor, Cursor

from utils import replace_config_dict


defaults = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": None,
    "password": None,
    "database": None,
    "charset": "utf8mb4",
    "cursorclass": DictCursor
}


def get_config(source: str = "yaml"):
    if source == "yaml":  # 从config.yaml获取配置信息
        try:
            with open('config.yaml', 'r') as config_file:
                config = yaml.safe_load(config_file)
            source_dict: dict = config['database']
        except FileNotFoundError:
            sys.exit("config.yaml not found.")
    elif source == ".env" or source == "sys_env":
        if source == ".env":
            import dotenv
            dotenv.load_dotenv()
        else:
            pass
        source_dict: dict = {
            "host": os.getenv("host"),
            "port": os.getenv("port"),
            "user": os.getenv("user"),
            "password": os.getenv("password"),
            "database": os.getenv("database"),
            "charset": os.getenv("charset"),
        }
    else:
        exit("Invalid source. Config the source of connection")
    return source_dict


def connect(config_dict) -> bool:
    connection_params = replace_config_dict(defaults, config_dict)

    try:
        connection = pymysql.connect(**connection_params)
        connection.close()
        return True
    except pymysql.MySQLError as e:
        return False


if __name__ == "__main__":
    status = connect(config_dict=get_config())
