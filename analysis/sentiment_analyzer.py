#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
情绪分析模块
负责对新闻文章进行情绪分析
"""

import logging
import sys
import os
from typing import List, Dict, Any, Optional, Tuple
import yaml
import mysql.connector
from mysql.connector import Error
from textblob import TextBlob
import jieba
import re

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """情绪分析类，负责对新闻文章进行情绪分析"""
    
    def __init__(self, config_path: str):
        """
        初始化情绪分析器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.method = self.config['analysis']['sentiment_analysis']['method']
        self.language = self.config['analysis']['sentiment_analysis']['language']
        self.enabled = self.config['analysis']['sentiment_analysis']['enabled']
        
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
    
    def analyze_text(self, text: str) -> Dict[str, float]:
        """
        分析文本的情绪
        
        Args:
            text: 待分析的文本
            
        Returns:
            情绪分析结果，包含极性和主观性
        """
        if not self.enabled:
            logger.info("情绪分析已禁用，跳过分析")
            return {'polarity': 0.0, 'subjectivity': 0.0, 'confidence': 0.0}
        
        # 预处理文本
        cleaned_text = self._preprocess_text(text)
        
        if not cleaned_text:
            logger.warning("文本预处理后为空，无法进行情绪分析")
            return {'polarity': 0.0, 'subjectivity': 0.0, 'confidence': 0.0}
        
        # 根据配置选择分析方法
        if self.method == 'textblob':
            return self._analyze_with_textblob(cleaned_text)
        elif self.method == 'vader':
            return self._analyze_with_vader(cleaned_text)
        elif self.method == 'custom':
            return self._analyze_with_custom(cleaned_text)
        else:
            logger.warning(f"未知的情绪分析方法: {self.method}，使用TextBlob作为默认方法")
            return self._analyze_with_textblob(cleaned_text)
    
    def _preprocess_text(self, text: str) -> str:
        """
        预处理文本
        
        Args:
            text: 原始文本
            
        Returns:
            预处理后的文本
        """
        if not text:
            return ""
        
        # 移除HTML标签
        text = re.sub(r'<[^>]+>', '', text)
        
        # 移除URL
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # 移除多余的空白字符
        text = re.sub(r'\s+', ' ', text).strip()
        
        # 如果是中文，使用jieba进行分词
        if self.language == 'zh':
            words = jieba.cut(text)
            text = ' '.join(words)
        
        return text
    
    def _analyze_with_textblob(self, text: str) -> Dict[str, float]:
        """
        使用TextBlob进行情绪分析
        
        Args:
            text: 预处理后的文本
            
        Returns:
            情绪分析结果
        """
        try:
            # 创建TextBlob对象
            blob = TextBlob(text)
            
            # 获取极性和主观性
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # 计算置信度（简单方法：主观性越高，置信度越高）
            confidence = (abs(polarity) + subjectivity) / 2
            
            return {
                'polarity': polarity,
                'subjectivity': subjectivity,
                'confidence': confidence
            }
            
        except Exception as e:
            logger.error(f"TextBlob情绪分析失败: {e}")
            return {'polarity': 0.0, 'subjectivity': 0.0, 'confidence': 0.0}
    
    def _analyze_with_vader(self, text: str) -> Dict[str, float]:
        """
        使用VADER进行情绪分析
        
        Args:
            text: 预处理后的文本
            
        Returns:
            情绪分析结果
        """
        # 注意：此方法需要安装vaderSentiment包
        # 由于当前未安装，此处仅提供实现框架
        logger.warning("VADER情绪分析方法未实现，使用TextBlob作为替代")
        return self._analyze_with_textblob(text)
    
    def _analyze_with_custom(self, text: str) -> Dict[str, float]:
        """
        使用自定义方法进行情绪分析
        
        Args:
            text: 预处理后的文本
            
        Returns:
            情绪分析结果
        """
        # 自定义情绪分析方法，可以根据需要实现
        # 此处仅提供实现框架
        logger.warning("自定义情绪分析方法未实现，使用TextBlob作为替代")
        return self._analyze_with_textblob(text)
    
    def analyze_article(self, article_id: int, title: str, content: str) -> Dict[str, float]:
        """
        分析单篇文章的情绪
        
        Args:
            article_id: 文章ID
            title: 文章标题
            content: 文章内容
            
        Returns:
            情绪分析结果
        """
        # 标题通常更能反映文章的情绪，给予更高的权重
        title_weight = 0.6
        content_weight = 0.4
        
        # 分析标题和内容
        title_sentiment = self.analyze_text(title)
        content_sentiment = self.analyze_text(content)
        
        # 加权平均
        polarity = title_sentiment['polarity'] * title_weight + content_sentiment['polarity'] * content_weight
        subjectivity = title_sentiment['subjectivity'] * title_weight + content_sentiment['subjectivity'] * content_weight
        confidence = title_sentiment['confidence'] * title_weight + content_sentiment['confidence'] * content_weight
        
        return {
            'article_id': article_id,
            'polarity': polarity,
            'subjectivity': subjectivity,
            'confidence': confidence
        }
    
    def analyze_new_articles(self) -> int:
        """
        分析所有新文章的情绪
        
        Returns:
            分析的文章数量
        """
        if not self.enabled:
            logger.info("情绪分析已禁用，跳过分析")
            return 0
        
        # 获取尚未进行情绪分析的文章
        articles = self._get_unanalyzed_articles()
        
        if not articles:
            logger.info("没有新文章需要进行情绪分析")
            return 0
        
        logger.info(f"开始对 {len(articles)} 篇文章进行情绪分析")
        
        analyzed_count = 0
        for article in articles:
            try:
                # 分析文章
                sentiment = self.analyze_article(article['id'], article['title'], article['content'])
                
                # 保存分析结果
                self._save_sentiment(sentiment)
                
                analyzed_count += 1
                
            except Exception as e:
                logger.error(f"分析文章ID {article['id']} 时出错: {e}")
        
        logger.info(f"完成 {analyzed_count} 篇文章的情绪分析")
        return analyzed_count
    
    def _get_unanalyzed_articles(self) -> List[Dict[str, Any]]:
        """
        获取尚未进行情绪分析的文章
        
        Returns:
            未分析的文章列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询尚未进行情绪分析的文章
            query = """
            SELECT a.id, a.title, a.content
            FROM news_articles a
            LEFT JOIN (
                SELECT DISTINCT article_id
                FROM sentiment_analysis
            ) s ON a.id = s.article_id
            WHERE s.article_id IS NULL
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
    
    def _save_sentiment(self, sentiment: Dict[str, Any]) -> bool:
        """
        保存情绪分析结果到数据库
        
        Args:
            sentiment: 情绪分析结果
            
        Returns:
            是否成功保存
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            # 插入分析结果
            insert_query = """
            INSERT INTO sentiment_analysis 
            (article_id, polarity, subjectivity, confidence)
            VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                sentiment['article_id'],
                sentiment['polarity'],
                sentiment['subjectivity'],
                sentiment['confidence']
            ))
            
            connection.commit()
            
            logger.debug(f"成功保存文章ID {sentiment['article_id']} 的情绪分析结果")
            return True
            
        except Error as e:
            logger.error(f"保存情绪分析结果时出错: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                cursor.close()
    
    def get_sentiment_stats(self, days: int = 30) -> Dict[str, Any]:
        """
        获取情绪统计信息
        
        Args:
            days: 统计的天数范围
            
        Returns:
            情绪统计信息
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询情绪统计信息
            query = """
            SELECT 
                AVG(sa.polarity) as avg_polarity,
                AVG(sa.subjectivity) as avg_subjectivity,
                MIN(sa.polarity) as min_polarity,
                MAX(sa.polarity) as max_polarity,
                COUNT(*) as article_count
            FROM sentiment_analysis sa
            JOIN news_articles na ON sa.article_id = na.id
            WHERE na.published_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            """
            
            cursor.execute(query, (days,))
            stats = cursor.fetchone()
            
            return stats
            
        except Error as e:
            logger.error(f"获取情绪统计信息时出错: {e}")
            return {}
        finally:
            if connection.is_connected():
                cursor.close()
    
    def get_sentiment_trend(self, days: int = 30, interval: str = 'day') -> List[Dict[str, Any]]:
        """
        获取情绪趋势
        
        Args:
            days: 统计的天数范围
            interval: 统计间隔 ('day', 'week', 'month')
            
        Returns:
            情绪趋势列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 根据间隔选择日期格式
            if interval == 'week':
                date_format = '%Y-%u'  # ISO周格式
                group_by = "YEARWEEK(na.published_date)"
            elif interval == 'month':
                date_format = '%Y-%m'
                group_by = "DATE_FORMAT(na.published_date, '%Y-%m')"
            else:  # 默认按天
                date_format = '%Y-%m-%d'
                group_by = "DATE(na.published_date)"
            
            # 查询情绪趋势
            query = f"""
            SELECT 
                DATE_FORMAT(na.published_date, %s) as period,
                AVG(sa.polarity) as avg_polarity,
                AVG(sa.subjectivity) as avg_subjectivity,
                COUNT(*) as article_count
            FROM sentiment_analysis sa
            JOIN news_articles na ON sa.article_id = na.id
            WHERE na.published_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY {group_by}
            ORDER BY na.published_date
            """
            
            cursor.execute(query, (date_format, days))
            trend = cursor.fetchall()
            
            return trend
            
        except Error as e:
            logger.error(f"获取情绪趋势时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
    
    def get_sentiment_by_source(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        获取按来源分组的情绪统计
        
        Args:
            days: 统计的天数范围
            
        Returns:
            按来源分组的情绪统计列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询按来源分组的情绪统计
            query = """
            SELECT 
                na.source,
                AVG(sa.polarity) as avg_polarity,
                AVG(sa.subjectivity) as avg_subjectivity,
                COUNT(*) as article_count
            FROM sentiment_analysis sa
            JOIN news_articles na ON sa.article_id = na.id
            WHERE na.published_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY na.source
            ORDER BY avg_polarity DESC
            """
            
            cursor.execute(query, (days,))
            by_source = cursor.fetchall()
            
            return by_source
            
        except Error as e:
            logger.error(f"获取按来源分组的情绪统计时出错: {e}")
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
        # 初始化情绪分析器
        sentiment_analyzer = SentimentAnalyzer(config_path)
        
        # 分析新文章
        analyzed_count = sentiment_analyzer.analyze_new_articles()
        
        # 获取情绪统计信息
        stats = sentiment_analyzer.get_sentiment_stats(days=30)
        
        # 获取情绪趋势
        trend = sentiment_analyzer.get_sentiment_trend(days=30, interval='day')
        
        logger.info(f"情绪分析完成，共分析 {analyzed_count} 篇文章")
        logger.info(f"情绪统计: {stats}")
        
    except Exception as e:
        logger.error(f"情绪分析过程中出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
