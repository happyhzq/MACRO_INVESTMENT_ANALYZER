#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
NewsAPI数据源抓取模块
负责通过NewsAPI获取新闻数据
"""

import requests
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

class NewsAPIFetcher:
    """NewsAPI数据抓取类，负责通过NewsAPI获取新闻数据"""
    
    def __init__(self, config_path: str):
        """
        初始化NewsAPI抓取器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.api_key = self.config['data_sources']['newsapi']['api_key']
        self.sources = self.config['data_sources']['newsapi']['sources']
        self.categories = self.config['data_sources']['newsapi']['categories']
        self.update_interval = self.config['data_sources']['newsapi']['update_interval']
        self.max_articles = self.config['data_sources']['newsapi']['max_articles_per_request']
        self.enabled = self.config['data_sources']['newsapi']['enabled']
        self.base_url = "https://newsapi.org/v2/"
        
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
    
    def fetch_everything(self, query: str = None, from_date: datetime.datetime = None, 
                        to_date: datetime.datetime = None, language: str = 'zh') -> List[Dict[str, Any]]:
        """
        使用NewsAPI的everything端点获取新闻
        
        Args:
            query: 搜索关键词
            from_date: 开始日期
            to_date: 结束日期
            language: 语言代码
            
        Returns:
            新闻文章列表
        """
        if not self.enabled:
            logger.info("NewsAPI数据源已禁用，跳过抓取")
            return []
        
        # 构建API请求参数
        params = {
            'apiKey': self.api_key,
            'pageSize': self.max_articles,
            'language': language,
            'sortBy': 'publishedAt'
        }
        
        # 添加可选参数
        if query:
            params['q'] = query
        
        if self.sources:
            params['sources'] = ','.join(self.sources)
            
        if from_date:
            params['from'] = from_date.strftime('%Y-%m-%dT%H:%M:%S')
            
        if to_date:
            params['to'] = to_date.strftime('%Y-%m-%dT%H:%M:%S')
        
        try:
            # 发送API请求
            response = requests.get(f"{self.base_url}everything", params=params)
            response.raise_for_status()  # 如果请求失败，抛出异常
            
            data = response.json()
            
            if data['status'] != 'ok':
                logger.error(f"NewsAPI返回错误: {data.get('message', '未知错误')}")
                return []
            
            articles = data.get('articles', [])
            logger.info(f"从NewsAPI获取了 {len(articles)} 篇文章")
            
            # 处理文章
            processed_articles = []
            for article in articles:
                processed_article = self._process_article(article)
                if processed_article:
                    processed_articles.append(processed_article)
            
            return processed_articles
            
        except requests.exceptions.RequestException as e:
            logger.error(f"NewsAPI请求失败: {e}")
            self._update_source_status('newsapi', 'error', str(e))
            return []
        except Exception as e:
            logger.error(f"处理NewsAPI数据时出错: {e}")
            self._update_source_status('newsapi', 'error', str(e))
            return []
    
    def fetch_top_headlines(self, country: str = None, category: str = None) -> List[Dict[str, Any]]:
        """
        使用NewsAPI的top-headlines端点获取头条新闻
        
        Args:
            country: 国家代码
            category: 新闻类别
            
        Returns:
            头条新闻列表
        """
        if not self.enabled:
            logger.info("NewsAPI数据源已禁用，跳过抓取")
            return []
        
        # 构建API请求参数
        params = {
            'apiKey': self.api_key,
            'pageSize': self.max_articles
        }
        
        # 添加可选参数
        if country:
            params['country'] = country
            
        if category:
            params['category'] = category
            
        if self.sources:
            params['sources'] = ','.join(self.sources)
        
        try:
            # 发送API请求
            response = requests.get(f"{self.base_url}top-headlines", params=params)
            response.raise_for_status()  # 如果请求失败，抛出异常
            
            data = response.json()
            
            if data['status'] != 'ok':
                logger.error(f"NewsAPI返回错误: {data.get('message', '未知错误')}")
                return []
            
            articles = data.get('articles', [])
            logger.info(f"从NewsAPI获取了 {len(articles)} 篇头条文章")
            
            # 处理文章
            processed_articles = []
            for article in articles:
                processed_article = self._process_article(article)
                if processed_article:
                    processed_articles.append(processed_article)
            
            return processed_articles
            
        except requests.exceptions.RequestException as e:
            logger.error(f"NewsAPI请求失败: {e}")
            self._update_source_status('newsapi', 'error', str(e))
            return []
        except Exception as e:
            logger.error(f"处理NewsAPI数据时出错: {e}")
            self._update_source_status('newsapi', 'error', str(e))
            return []
    
    def fetch_all_categories(self) -> List[Dict[str, Any]]:
        """
        获取所有配置的新闻类别的头条新闻
        
        Returns:
            所有类别的头条新闻列表
        """
        all_articles = []
        
        for category in self.categories:
            try:
                logger.info(f"正在获取类别 '{category}' 的头条新闻")
                articles = self.fetch_top_headlines(category=category)
                all_articles.extend(articles)
                
                # 短暂暂停，避免API请求过于频繁
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"获取类别 '{category}' 的头条新闻时出错: {e}")
        
        return all_articles
    
    def fetch_by_keywords(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """
        根据关键词列表获取新闻
        
        Args:
            keywords: 关键词列表
            
        Returns:
            匹配关键词的新闻列表
        """
        all_articles = []
        
        for keyword in keywords:
            try:
                logger.info(f"正在获取关键词 '{keyword}' 的新闻")
                # 获取最近7天的新闻
                from_date = datetime.datetime.now() - datetime.timedelta(days=7)
                articles = self.fetch_everything(query=keyword, from_date=from_date)
                all_articles.extend(articles)
                
                # 短暂暂停，避免API请求过于频繁
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"获取关键词 '{keyword}' 的新闻时出错: {e}")
        
        return all_articles
    
    def _process_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单个新闻文章
        
        Args:
            article: NewsAPI返回的文章字典
            
        Returns:
            处理后的文章字典，如果处理失败则返回None
        """
        try:
            # 解析发布日期
            published_date = None
            if article.get('publishedAt'):
                try:
                    published_date = datetime.datetime.strptime(
                        article['publishedAt'], '%Y-%m-%dT%H:%M:%SZ'
                    )
                except ValueError:
                    published_date = datetime.datetime.now()
            else:
                published_date = datetime.datetime.now()
            
            # 构建处理后的文章
            processed_article = {
                'title': article.get('title', ''),
                'content': article.get('content', article.get('description', '')),
                'url': article.get('url', ''),
                'published_date': published_date,
                'source': article.get('source', {}).get('name', 'NewsAPI'),
                'category': '',  # NewsAPI不直接提供类别
                'author': article.get('author', ''),
                'language': article.get('language', 'zh'),
                'fetched_date': datetime.datetime.now()
            }
            
            return processed_article
            
        except Exception as e:
            logger.error(f"处理新闻文章时出错: {e}")
            return None
    
    def save_articles_to_db(self, articles: List[Dict[str, Any]]) -> int:
        """
        将抓取的文章保存到数据库
        
        Args:
            articles: 处理后的新闻文章列表
            
        Returns:
            成功保存的文章数量
        """
        if not articles:
            logger.info("没有新文章需要保存")
            return 0
        
        saved_count = 0
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            for article in articles:
                # 检查URL是否已存在，避免重复
                check_query = "SELECT id FROM news_articles WHERE url = %s"
                cursor.execute(check_query, (article['url'],))
                result = cursor.fetchone()
                
                if result:
                    logger.debug(f"文章已存在，跳过: {article['title']}")
                    continue
                
                # 插入新文章
                insert_query = """
                INSERT INTO news_articles 
                (title, content, source, url, published_date, fetched_date, language, category, author)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_query, (
                    article['title'],
                    article['content'],
                    article['source'],
                    article['url'],
                    article['published_date'],
                    article['fetched_date'],
                    article['language'],
                    article['category'],
                    article['author']
                ))
                
                saved_count += 1
            
            connection.commit()
            logger.info(f"成功保存 {saved_count} 篇新闻到数据库")
            
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
            check_query = "SELECT id FROM data_source_status WHERE source_name = %s AND source_type = 'newsapi'"
            cursor.execute(check_query, (source_name,))
            result = cursor.fetchone()
            
            now = datetime.datetime.now()
            
            if result:
                # 更新现有记录
                update_query = """
                UPDATE data_source_status 
                SET last_update = %s, status = %s, error_message = %s, updated_at = %s
                WHERE source_name = %s AND source_type = 'newsapi'
                """
                cursor.execute(update_query, (now, status, error_message, now, source_name))
            else:
                # 插入新记录
                insert_query = """
                INSERT INTO data_source_status 
                (source_name, source_type, last_update, status, error_message, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (source_name, 'newsapi', now, status, error_message, now, now))
            
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
        # 初始化NewsAPI抓取器
        news_api_fetcher = NewsAPIFetcher(config_path)
        
        # 获取所有类别的头条新闻
        articles = news_api_fetcher.fetch_all_categories()
        
        # 获取特定关键词的新闻
        keywords = ['关税', '贸易政策', '央行', '利率']
        keyword_articles = news_api_fetcher.fetch_by_keywords(keywords)
        
        # 合并所有文章
        all_articles = articles + keyword_articles
        
        # 保存到数据库
        saved_count = news_api_fetcher.save_articles_to_db(all_articles)
        
        logger.info(f"NewsAPI抓取完成，共抓取 {len(all_articles)} 篇新闻，保存 {saved_count} 篇到数据库")
        
        # 更新数据源状态
        news_api_fetcher._update_source_status('newsapi', 'active')
        
    except Exception as e:
        logger.error(f"NewsAPI抓取过程中出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
