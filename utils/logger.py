#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志工具模块
负责配置和管理日志
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(log_level=logging.INFO, log_file=None, max_size_mb=10, backup_count=5):
    """
    设置日志记录器
    
    Args:
        log_level: 日志级别
        log_file: 日志文件路径
        max_size_mb: 日志文件最大大小（MB）
        backup_count: 保留的日志文件数量
    """
    # 创建根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 添加控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 如果指定了日志文件，添加文件处理器
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 创建旋转文件处理器
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger
