#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库连接器模块
负责管理与MySQL数据库的连接
"""

import mysql.connector
from mysql.connector import Error
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """数据库连接器类，负责管理与MySQL数据库的连接"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化数据库连接器
        
        Args:
            db_config: 数据库配置字典
        """
        self.db_config = db_config
        self.connection = None
    
    def get_connection(self):
        """
        获取数据库连接
        
        Returns:
            mysql.connector.connection.MySQLConnection: 数据库连接对象
        """
        try:
            if self.connection is not None and self.connection.is_connected():
                return self.connection
            
            self.connection = mysql.connector.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database_name'],
                charset=self.db_config['charset']
            )
            
            if self.connection.is_connected():
                logger.debug("数据库连接成功")
                return self.connection
                
        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def close_connection(self):
        """关闭数据库连接"""
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()
            logger.debug("数据库连接已关闭")
    
    def execute_query(self, query: str, params: tuple = None) -> Optional[list]:
        """
        执行查询语句
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果列表，如果是非查询操作则返回None
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        result = None
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # 检查是否是SELECT查询
            if query.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
            else:
                connection.commit()
                
            return result
            
        except Error as e:
            logger.error(f"执行查询失败: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()
    
    def execute_many(self, query: str, params_list: list) -> int:
        """
        执行批量操作
        
        Args:
            query: SQL语句
            params_list: 参数列表
            
        Returns:
            受影响的行数
        """
        connection = self.get_connection()
        cursor = connection.cursor()
        
        try:
            cursor.executemany(query, params_list)
            connection.commit()
            return cursor.rowcount
            
        except Error as e:
            logger.error(f"执行批量操作失败: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()
    
    def initialize_database(self, schema_file: str) -> bool:
        """
        初始化数据库，执行schema文件中的SQL语句
        
        Args:
            schema_file: schema文件路径
            
        Returns:
            是否成功初始化
        """
        try:
            with open(schema_file, 'r', encoding='utf-8') as file:
                schema_sql = file.read()
            
            connection = self.get_connection()
            cursor = connection.cursor()
            
            # 分割SQL语句
            sql_commands = schema_sql.split(';')
            
            for command in sql_commands:
                if command.strip():
                    cursor.execute(command)
            
            connection.commit()
            logger.info("数据库初始化成功")
            return True
            
        except Error as e:
            logger.error(f"数据库初始化失败: {e}")
            if connection:
                connection.rollback()
            return False
        except Exception as e:
            logger.error(f"读取schema文件失败: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
