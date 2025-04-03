import sys
import os
import yaml
import json
from typing import List, Optional, Dict, Any, Tuple

import pymysql
from pymysql.cursors import DictCursor

from mcp.server.fastmcp import FastMCP

from utils import *
from utils import connect
from utils.connect import defaults as _defaults

mcp = FastMCP(name='sql_mcp')


args = sys.argv[1:]
try:
    _source = args[0]
except:
    _source = "yaml"


def _connection(database_name=None):
    source_dict = connect.get_config(source=_source)
    connection_params = replace_config_dict(_defaults, source_dict)
    connection = pymysql.connect(**connection_params)
    if database_name is not None:
        connection.select_db(database_name)
    return connection


@mcp.tool(name="test_connection")
def test_connection(database=None, source: str = _source) -> bool:
    """
    Test if a MySQL database connection can be successfully established.

    This function loads MySQL connection parameters from a specified configuration source
    and attempts to establish a connection to the database. It can be used to verify
    if the configuration information is correct and if the network connection is functional.

    Args:
        database: str, optional
            The name of database to connect to.
            If None, it will only test the connection to the MySQL server without specifying a particular database.
            Default if None.

        source: str, optional
            Specifies the source from which to load MySQL configuration information. Options include:
            - "yaml": Load configuration from config.yaml file (default)
            - ".env": Load configuration from .env environment variable file
            - "sys_env": Load configuration from system environment variables
            Default is "yaml" (config from base command).

    Returns:
        bool
            Connection test result:
            - True: Connection successful
            - False: Connection failed

    Examples:
        >>> test_connection()  # Test connection using default configuration source
        True

        >>> test_connection(database="test_database")  # Test connection to a specific database
        False

        >>> test_connection(source=".env")  # Load configuration from .env file and test connection
        True

    Notes:
        This function depends on the get_config and connect functions from the connect module.

        Possible reasons for connection failure include:
        - Incorrect configuration information (hostname, port, username, password, etc.)
        - Specified database does not exist
        - MySQL server is not running or cannot be accessed
        - Network connection issues
    """
    source_dict = connect.get_config(source=source)
    source_dict["database"] = database
    return connect.connect(source_dict)


@mcp.tool(name="get_sql_table_info")
def get_sql_table_info(table_name: str, database: str, cols: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    获取MySQL表的详细信息，包括表注释和指定列的统计摘要信息（类似Stata的`summarize`命令）。

    此函数从指定的MySQL表中获取：
    1. 表本身的注释信息
    2. 所有列名的列表（或筛选后的列名列表）
    3. 每个列的详细统计信息（最小值、最大值、平均值）及其注释

    如果未提供`cols`参数，则包含表中的所有列。

    Args:
        table_name (str): 要查询的表名。
        database (str): 数据库名称。
        cols (Optional[List[str]]): 要摘要的列名列表。如果为None，则包括所有列。

    Returns:
        Dict[str, Any]: 一个包含三个键的字典：
            - "table_info": 包含表注释的字典
            - "col_names": 所有有效列名的列表（按顺序）
            - "col_info": 以列名为键的字典，每个值包含该列的统计摘要和注释信息
                         格式为: {"col_name": {"min": 值, "max": 值, "mean": 值, "comment": "列注释"}}

        如果提供的所有列名都无效，"col_names"将为空列表，"col_info"将为空字典。
        如果连接失败，函数将终止程序执行。

    Example:
        >>> get_sql_table_info("employees", database="test", cols=["salary", "age"])
        {
            "table_info": {"comment": "员工基本信息表"},
            "col_names": ["salary", "age"],
            "col_info": {
                "salary": {"min": 30000, "max": 120000, "mean": 75000, "comment": "Monthly salary in USD"},
                "age": {"min": 22, "max": 65, "mean": 42, "comment": "Employee age"}
            }
        }

    Notes:
        - 此函数依赖于connect模块中的get_config和connect函数。
        - 所有数值计算（最小值、最大值、平均值）仅适用于数值类型的列。非数值列将返回NULL（在Python中表现为None）。
        - 函数会自动过滤掉不存在的列名，仅处理有效的列。
        - 表的注释信息从INFORMATION_SCHEMA.TABLES表中获取。
    """
    try:
        connection = _connection()
    except pymysql.MySQLError as e:
        sys.exit(e)

    cursor = connection.cursor()

    # 创建结果字典框架
    result = {
        "table_info": {},
        "col_names": [],
        "col_info": {}
    }

    # 获取表注释信息
    cursor.execute(f"""
        SELECT TABLE_COMMENT
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name}'
        AND TABLE_SCHEMA = DATABASE()
    """)
    table_comment_row = cursor.fetchone()
    if table_comment_row:
        result["table_info"]["comment"] = table_comment_row["TABLE_COMMENT"]
    else:
        result["table_info"]["comment"] = ""

    # 获取所有有效列名及其注释
    cursor.execute(f"""
        SELECT COLUMN_NAME, COLUMN_COMMENT, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        AND TABLE_SCHEMA = DATABASE()
        ORDER BY ORDINAL_POSITION
    """)
    columns_data = cursor.fetchall()

    # 创建列名到注释的映射
    column_info = {row['COLUMN_NAME']: row['COLUMN_COMMENT'] for row in columns_data}

    # 获取所有列名（按原始顺序）
    all_columns = [row['COLUMN_NAME'] for row in sorted(columns_data, key=lambda x: x['ORDINAL_POSITION'])]

    # 筛选有效的列名
    if cols is not None:
        valid_cols = [col for col in cols if col in column_info]
        if not valid_cols:
            # 如果没有有效的列，返回空结果结构
            cursor.close()
            connection.close()
            result["col_names"] = []
            result["col_info"] = {}
            return result
    else:
        valid_cols = all_columns

    # 更新结果中的列名列表
    result["col_names"] = valid_cols

    # 为每个有效列生成统计查询
    summary_queries = []
    for col in valid_cols:
        summary_queries.append(f"""
            MIN(`{col}`) AS `{col}_min`,
            MAX(`{col}`) AS `{col}_max`,
            AVG(`{col}`) AS `{col}_mean`
        """)

    # 执行统计查询
    query = f"SELECT {', '.join(summary_queries)} FROM `{table_name}`"
    cursor.execute(query)
    summary_results = cursor.fetchone()

    # 格式化统计结果
    for col in valid_cols:
        result["col_info"][col] = {
            "min": summary_results[f'{col}_min'],
            "max": summary_results[f'{col}_max'],
            "mean": summary_results[f'{col}_mean'],
            "comment": column_info.get(col, "")
        }

    # 关闭数据库连接
    cursor.close()
    connection.close()

    return result


if __name__ == "__main__":
    mcp.run(transport="stdio")
