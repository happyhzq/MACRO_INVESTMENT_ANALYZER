#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
RSS数据源抓取模块
负责从配置的RSS源获取新闻数据
"""

import feedparser
import datetime
import time
import logging
import sys
import os
from typing import List, Dict, Any, Optional
import yaml
import mysql.connector
from mysql.connector import Error

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)

class RSSFetcher:
    """RSS数据抓取类，负责从配置的RSS源获取新闻数据"""
    
    def __init__(self, config_path: str):
        """
        初始化RSS抓取器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.sources = self.config['data_sources']['rss']['sources']
        self.enabled = self.config['data_sources']['rss']['enabled']
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            配置字典
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise
    
    def fetch_all_sources(self) -> List[Dict[str, Any]]:
        """
        抓取所有配置的RSS源数据
        
        Returns:
            所有RSS源的新闻条目列表
        """
        if not self.enabled:
            logger.info("RSS数据源已禁用，跳过抓取")
            return []
        
        all_entries = []
        for source in self.sources:
            try:
                logger.info(f"正在抓取RSS源: {source['name']} ({source['url']})")
                entries = self.fetch_source(source)
                all_entries.extend(entries)
                
                # 更新数据源状态
                self._update_source_status(source['name'], 'active')
                
                # 按照配置的更新间隔暂停，避免过于频繁的请求
                time.sleep(1)  # 短暂暂停，避免连续请求
                
            except Exception as e:
                logger.error(f"抓取RSS源 {source['name']} 失败: {e}")
                self._update_source_status(source['name'], 'error', str(e))
        
        return all_entries
    
    def fetch_source(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        抓取单个RSS源数据
        
        Args:
            source: RSS源配置字典
            
        Returns:
            该RSS源的新闻条目列表
        """
        try:
            feed = feedparser.parse(source['url'])
            
            if hasattr(feed, 'status') and feed.status != 200:
                logger.warning(f"RSS源 {source['name']} 返回非200状态码: {feed.status}")
            
            if not feed.entries:
                logger.warning(f"RSS源 {source['name']} 没有返回任何条目")
                return []
            
            processed_entries = []
            for entry in feed.entries:
                processed_entry = self._process_entry(entry, source)
                if processed_entry:
                    processed_entries.append(processed_entry)
            
            logger.info(f"从 {source['name']} 成功抓取 {len(processed_entries)} 条新闻")
            return processed_entries
            
        except Exception as e:
            logger.error(f"抓取RSS源 {source['name']} 时出错: {e}")
            raise
    
    def _process_entry(self, entry: Dict[str, Any], source: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单个RSS条目
        
        Args:
            entry: feedparser解析的RSS条目
            source: RSS源配置
            
        Returns:
            处理后的条目字典，如果处理失败则返回None
        """
        try:
            # 提取发布日期
            published_date = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                published_date = datetime.datetime(*entry.published_parsed[:6])
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                published_date = datetime.datetime(*entry.updated_parsed[:6])
            else:
                published_date = datetime.datetime.now()
            
            # 提取内容
            content = ""
            if hasattr(entry, 'content') and entry.content:
                content = entry.content[0].value
            elif hasattr(entry, 'summary') and entry.summary:
                content = entry.summary
            elif hasattr(entry, 'description') and entry.description:
                content = entry.description
            
            # 提取作者
            author = ""
            if hasattr(entry, 'author') and entry.author:
                author = entry.author
            
            # 构建处理后的条目
            processed_entry = {
                'title': entry.title if hasattr(entry, 'title') else "",
                'content': content,
                'url': entry.link if hasattr(entry, 'link') else "",
                'published_date': published_date,
                'source': source['name'],
                'category': source.get('category', ''),
                'author': author,
                'language': source.get('language', 'zh'),
                'fetched_date': datetime.datetime.now()
            }
            
            return processed_entry
            
        except Exception as e:
            logger.error(f"处理RSS条目时出错: {e}")
            return None
    
    def save_entries_to_db(self, entries: List[Dict[str, Any]]) -> int:
        """
        将抓取的条目保存到数据库
        
        Args:
            entries: 处理后的新闻条目列表
            
        Returns:
            成功保存的条目数量
        """
        if not entries:
            logger.info("没有新条目需要保存")
            return 0
        
        saved_count = 0
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            for entry in entries:
                # 检查URL是否已存在，避免重复
                check_query = "SELECT id FROM news_articles WHERE url = %s"
                cursor.execute(check_query, (entry['url'],))
                result = cursor.fetchone()
                
                if result:
                    logger.debug(f"条目已存在，跳过: {entry['title']}")
                    continue
                
                # 插入新条目
                insert_query = """
                INSERT INTO news_articles 
                (title, content, source, url, published_date, fetched_date, language, category, author)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_query, (
                    entry['title'],
                    entry['content'],
                    entry['source'],
                    entry['url'],
                    entry['published_date'],
                    entry['fetched_date'],
                    entry['language'],
                    entry['category'],
                    entry['author']
                ))
                
                saved_count += 1
            
            connection.commit()
            logger.info(f"成功保存 {saved_count} 条新闻到数据库")
            
        except Error as e:
            logger.error(f"保存到数据库时出错: {e}")
            connection.rollback()
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
        
        return saved_count
    
    def _update_source_status(self, source_name: str, status: str, error_message: str = None) -> None:
        """
        更新数据源状态
        
        Args:
            source_name: 数据源名称
            status: 状态 ('active', 'error', 'disabled')
            error_message: 错误信息（如果有）
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            # 检查是否已有记录
            check_query = "SELECT id FROM data_source_status WHERE source_name = %s AND source_type = 'rss'"
            cursor.execute(check_query, (source_name,))
            result = cursor.fetchone()
            
            now = datetime.datetime.now()
            
            if result:
                # 更新现有记录
                update_query = """
                UPDATE data_source_status 
                SET last_update = %s, status = %s, error_message = %s, updated_at = %s
                WHERE source_name = %s AND source_type = 'rss'
                """
                cursor.execute(update_query, (now, status, error_message, now, source_name))
            else:
                # 插入新记录
                insert_query = """
                INSERT INTO data_source_status 
                (source_name, source_type, last_update, status, error_message, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (source_name, 'rss', now, status, error_message, now, now))
            
            connection.commit()
            
        except Error as e:
            logger.error(f"更新数据源状态时出错: {e}")
            connection.rollback()
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()


def main():
    """主函数，用于独立运行测试"""
    # 设置日志
    setup_logger()
    
    # 获取配置文件路径
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                              'config', 'config.yaml')
    
    if not os.path.exists(config_path):
        logger.error(f"配置文件不存在: {config_path}")
        sys.exit(1)
    
    try:
        # 初始化RSS抓取器
        rss_fetcher = RSSFetcher(config_path)
        
        # 抓取所有RSS源
        entries = rss_fetcher.fetch_all_sources()
        
        # 保存到数据库
        saved_count = rss_fetcher.save_entries_to_db(entries)
        
        logger.info(f"RSS抓取完成，共抓取 {len(entries)} 条新闻，保存 {saved_count} 条到数据库")
        
    except Exception as e:
        logger.error(f"RSS抓取过程中出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
