#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
关键词过滤分析模块
负责对新闻文章进行关键词匹配和过滤
"""

import logging
import sys
import os
import re
from typing import List, Dict, Any, Optional, Tuple
import yaml
import mysql.connector
from mysql.connector import Error

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)

class KeywordFilter:
    """关键词过滤分析类，负责对新闻文章进行关键词匹配和过滤"""
    
    def __init__(self, config_path: str):
        """
        初始化关键词过滤器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.keywords = self.config['analysis']['keyword_filter']['keywords']
        self.enabled = self.config['analysis']['keyword_filter']['enabled']
        
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
    
    def analyze_article(self, article_id: int, title: str, content: str) -> List[Dict[str, Any]]:
        """
        分析单篇文章的关键词匹配情况
        
        Args:
            article_id: 文章ID
            title: 文章标题
            content: 文章内容
            
        Returns:
            关键词匹配结果列表
        """
        if not self.enabled:
            logger.info("关键词过滤已禁用，跳过分析")
            return []
        
        # 合并标题和内容进行分析
        full_text = f"{title} {content}"
        matches = []
        
        # 遍历所有关键词类别
        for category, keywords in self.keywords.items():
            for keyword in keywords:
                # 使用正则表达式进行匹配，确保是独立的词
                pattern = r'\b' + re.escape(keyword) + r'\b'
                found_matches = re.finditer(pattern, full_text, re.IGNORECASE)
                
                match_count = 0
                contexts = []
                
                # 提取匹配的上下文
                for match in found_matches:
                    match_count += 1
                    start = max(0, match.start() - 50)
                    end = min(len(full_text), match.end() + 50)
                    context = full_text[start:end].replace(match.group(), f"**{match.group()}**")
                    contexts.append(context)
                
                if match_count > 0:
                    match_result = {
                        'article_id': article_id,
                        'keyword': keyword,
                        'keyword_category': category,
                        'match_count': match_count,
                        'context': '\n'.join(contexts[:5])  # 最多保存5个上下文
                    }
                    matches.append(match_result)
                    logger.debug(f"文章ID {article_id} 匹配关键词 '{keyword}' {match_count} 次")
        
        return matches
    
    def analyze_new_articles(self) -> int:
        """
        分析所有新文章的关键词匹配情况
        
        Returns:
            分析的文章数量
        """
        if not self.enabled:
            logger.info("关键词过滤已禁用，跳过分析")
            return 0
        
        # 获取尚未进行关键词分析的文章
        articles = self._get_unanalyzed_articles()
        
        if not articles:
            logger.info("没有新文章需要进行关键词分析")
            return 0
        
        logger.info(f"开始对 {len(articles)} 篇文章进行关键词分析")
        
        analyzed_count = 0
        for article in articles:
            try:
                # 分析文章
                matches = self.analyze_article(article['id'], article['title'], article['content'])
                
                # 保存匹配结果
                if matches:
                    self._save_matches(matches)
                
                analyzed_count += 1
                
            except Exception as e:
                logger.error(f"分析文章ID {article['id']} 时出错: {e}")
        
        logger.info(f"完成 {analyzed_count} 篇文章的关键词分析")
        return analyzed_count
    
    def _get_unanalyzed_articles(self) -> List[Dict[str, Any]]:
        """
        获取尚未进行关键词分析的文章
        
        Returns:
            未分析的文章列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询尚未进行关键词分析的文章
            query = """
            SELECT a.id, a.title, a.content
            FROM news_articles a
            LEFT JOIN (
                SELECT DISTINCT article_id
                FROM keyword_matches
            ) k ON a.id = k.article_id
            WHERE k.article_id IS NULL
            ORDER BY a.published_date DESC
            LIMIT 100
            """
            
            cursor.execute(query)
            articles = cursor.fetchall()
            
            return articles
            
        except Error as e:
            logger.error(f"获取未分析文章时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
    
    def _save_matches(self, matches: List[Dict[str, Any]]) -> bool:
        """
        保存关键词匹配结果到数据库
        
        Args:
            matches: 关键词匹配结果列表
            
        Returns:
            是否成功保存
        """
        if not matches:
            return True
        
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            # 插入匹配结果
            insert_query = """
            INSERT INTO keyword_matches 
            (article_id, keyword, keyword_category, match_count, context)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            values = [
                (match['article_id'], match['keyword'], match['keyword_category'], 
                 match['match_count'], match['context'])
                for match in matches
            ]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            
            logger.debug(f"成功保存 {len(matches)} 条关键词匹配结果")
            return True
            
        except Error as e:
            logger.error(f"保存关键词匹配结果时出错: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                cursor.close()
    
    def get_keyword_stats(self, days: int = 30) -> Dict[str, Dict[str, int]]:
        """
        获取关键词统计信息
        
        Args:
            days: 统计的天数范围
            
        Returns:
            按类别分组的关键词统计信息
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询关键词统计信息
            query = """
            SELECT km.keyword_category, km.keyword, SUM(km.match_count) as total_count
            FROM keyword_matches km
            JOIN news_articles na ON km.article_id = na.id
            WHERE na.published_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY km.keyword_category, km.keyword
            ORDER BY km.keyword_category, total_count DESC
            """
            
            cursor.execute(query, (days,))
            results = cursor.fetchall()
            
            # 按类别分组
            stats = {}
            for row in results:
                category = row['keyword_category']
                keyword = row['keyword']
                count = row['total_count']
                
                if category not in stats:
                    stats[category] = {}
                
                stats[category][keyword] = count
            
            return stats
            
        except Error as e:
            logger.error(f"获取关键词统计信息时出错: {e}")
            return {}
        finally:
            if connection.is_connected():
                cursor.close()
    
    def get_trending_keywords(self, days: int = 7, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取趋势关键词
        
        Args:
            days: 统计的天数范围
            limit: 返回的关键词数量
            
        Returns:
            趋势关键词列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询趋势关键词
            query = """
            SELECT km.keyword, km.keyword_category, 
                   COUNT(DISTINCT km.article_id) as article_count,
                   SUM(km.match_count) as total_count
            FROM keyword_matches km
            JOIN news_articles na ON km.article_id = na.id
            WHERE na.published_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY km.keyword, km.keyword_category
            ORDER BY article_count DESC, total_count DESC
            LIMIT %s
            """
            
            cursor.execute(query, (days, limit))
            trending = cursor.fetchall()
            
            return trending
            
        except Error as e:
            logger.error(f"获取趋势关键词时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
    
    def get_articles_by_keyword(self, keyword: str, days: int = 30, limit: int = 20) -> List[Dict[str, Any]]:
        """
        获取包含特定关键词的文章
        
        Args:
            keyword: 关键词
            days: 统计的天数范围
            limit: 返回的文章数量
            
        Returns:
            包含该关键词的文章列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询包含特定关键词的文章
            query = """
            SELECT na.id, na.title, na.source, na.url, na.published_date,
                   km.match_count, km.context
            FROM news_articles na
            JOIN keyword_matches km ON na.id = km.article_id
            WHERE km.keyword = %s
              AND na.published_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY na.published_date DESC
            LIMIT %s
            """
            
            cursor.execute(query, (keyword, days, limit))
            articles = cursor.fetchall()
            
            return articles
            
        except Error as e:
            logger.error(f"获取包含关键词 '{keyword}' 的文章时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()


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
        # 初始化关键词过滤器
        keyword_filter = KeywordFilter(config_path)
        
        # 分析新文章
        analyzed_count = keyword_filter.analyze_new_articles()
        
        # 获取关键词统计信息
        stats = keyword_filter.get_keyword_stats(days=30)
        
        # 获取趋势关键词
        trending = keyword_filter.get_trending_keywords(days=7, limit=10)
        
        logger.info(f"关键词分析完成，共分析 {analyzed_count} 篇文章")
        logger.info(f"趋势关键词: {trending}")
        
    except Exception as e:
        logger.error(f"关键词分析过程中出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
