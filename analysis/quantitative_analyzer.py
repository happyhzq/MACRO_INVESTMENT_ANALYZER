#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
量化分析模块
负责对新闻文章和宏观事件进行量化分析
"""

import logging
import sys
import os
import datetime
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
import yaml
import mysql.connector
from mysql.connector import Error

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)

class QuantitativeAnalyzer:
    """量化分析类，负责对新闻文章和宏观事件进行量化分析"""
    
    def __init__(self, config_path: str):
        """
        初始化量化分析器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.historical_window = self.config['analysis']['quantitative_analysis']['historical_correlation_window']
        self.decay_factor = self.config['analysis']['quantitative_analysis']['impact_decay_factor']
        self.confidence_threshold = self.config['analysis']['quantitative_analysis']['confidence_threshold']
        self.enabled = self.config['analysis']['quantitative_analysis']['enabled']
        
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
    
    def identify_macro_events(self) -> List[Dict[str, Any]]:
        """
        从关键词匹配和情绪分析结果中识别宏观事件
        
        Returns:
            识别出的宏观事件列表
        """
        if not self.enabled:
            logger.info("量化分析已禁用，跳过事件识别")
            return []
        
        # 获取最近一段时间内的关键词匹配和情绪分析结果
        recent_days = 7  # 最近7天的数据
        keyword_matches = self._get_recent_keyword_matches(recent_days)
        
        if not keyword_matches:
            logger.info("没有找到最近的关键词匹配结果，无法识别宏观事件")
            return []
        
        # 按关键词类别分组
        category_matches = {}
        for match in keyword_matches:
            category = match['keyword_category']
            if category not in category_matches:
                category_matches[category] = []
            category_matches[category].append(match)
        
        # 识别每个类别中的潜在事件
        identified_events = []
        for category, matches in category_matches.items():
            # 如果某个类别的匹配数量超过阈值，可能存在宏观事件
            if len(matches) >= 5:  # 至少5篇相关文章
                # 计算该类别的平均情绪极性
                article_ids = [match['article_id'] for match in matches]
                avg_sentiment = self._get_average_sentiment(article_ids)
                
                # 提取最常见的关键词
                keywords = {}
                for match in matches:
                    keyword = match['keyword']
                    if keyword not in keywords:
                        keywords[keyword] = 0
                    keywords[keyword] += match['match_count']
                
                top_keywords = sorted(keywords.items(), key=lambda x: x[1], reverse=True)[:5]
                
                # 构建事件名称
                event_name = f"{category.capitalize()} event: " + ", ".join([k for k, _ in top_keywords])
                
                # 计算事件重要性（1-5）
                importance = min(5, max(1, int(len(matches) / 5)))
                
                # 构建事件描述
                description = f"Identified from {len(matches)} articles with keywords: {', '.join([k for k, _ in top_keywords])}. "
                description += f"Average sentiment polarity: {avg_sentiment['avg_polarity']:.2f}"
                
                # 确定事件开始日期（最早的文章日期）
                start_date = min([match['published_date'] for match in matches])
                
                # 创建事件对象
                event = {
                    'event_name': event_name,
                    'event_category': category,
                    'start_date': start_date,
                    'end_date': None,  # 事件尚未结束
                    'description': description,
                    'importance': importance,
                    'article_ids': article_ids,
                    'sentiment': avg_sentiment
                }
                
                identified_events.append(event)
                logger.info(f"识别到潜在宏观事件: {event_name}")
        
        return identified_events
    
    def _get_recent_keyword_matches(self, days: int) -> List[Dict[str, Any]]:
        """
        获取最近一段时间内的关键词匹配结果
        
        Args:
            days: 天数
            
        Returns:
            关键词匹配结果列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
            SELECT km.*, na.published_date
            FROM keyword_matches km
            JOIN news_articles na ON km.article_id = na.id
            WHERE na.published_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY na.published_date DESC
            """
            
            cursor.execute(query, (days,))
            matches = cursor.fetchall()
            
            return matches
            
        except Error as e:
            logger.error(f"获取最近关键词匹配结果时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
    
    def _get_average_sentiment(self, article_ids: List[int]) -> Dict[str, float]:
        """
        获取一组文章的平均情绪极性
        
        Args:
            article_ids: 文章ID列表
            
        Returns:
            平均情绪极性
        """
        if not article_ids:
            return {'avg_polarity': 0.0, 'avg_subjectivity': 0.0}
        
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 构建IN子句
            placeholders = ', '.join(['%s'] * len(article_ids))
            
            query = f"""
            SELECT AVG(polarity) as avg_polarity, AVG(subjectivity) as avg_subjectivity
            FROM sentiment_analysis
            WHERE article_id IN ({placeholders})
            """
            
            cursor.execute(query, article_ids)
            result = cursor.fetchone()
            
            if result and result['avg_polarity'] is not None:
                return result
            else:
                return {'avg_polarity': 0.0, 'avg_subjectivity': 0.0}
            
        except Error as e:
            logger.error(f"获取平均情绪极性时出错: {e}")
            return {'avg_polarity': 0.0, 'avg_subjectivity': 0.0}
        finally:
            if connection.is_connected():
                cursor.close()
    
    def save_macro_event(self, event: Dict[str, Any]) -> Optional[int]:
        """
        保存宏观事件到数据库
        
        Args:
            event: 宏观事件字典
            
        Returns:
            事件ID，如果保存失败则返回None
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            # 插入宏观事件
            insert_query = """
            INSERT INTO macro_events 
            (event_name, event_category, start_date, end_date, description, importance)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                event['event_name'],
                event['event_category'],
                event['start_date'],
                event['end_date'],
                event['description'],
                event['importance']
            ))
            
            # 获取插入的事件ID
            event_id = cursor.lastrowid
            
            # 插入事件与文章的关联
            if 'article_ids' in event and event_id:
                self._save_event_articles(event_id, event['article_ids'])
            
            connection.commit()
            logger.info(f"成功保存宏观事件: {event['event_name']} (ID: {event_id})")
            
            return event_id
            
        except Error as e:
            logger.error(f"保存宏观事件时出错: {e}")
            connection.rollback()
            return None
        finally:
            if connection.is_connected():
                cursor.close()
    
    def _save_event_articles(self, event_id: int, article_ids: List[int]) -> bool:
        """
        保存事件与文章的关联
        
        Args:
            event_id: 事件ID
            article_ids: 文章ID列表
            
        Returns:
            是否成功保存
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            # 插入事件与文章的关联
            insert_query = """
            INSERT INTO event_articles 
            (event_id, article_id, relevance_score)
            VALUES (%s, %s, %s)
            """
            
            values = [(event_id, article_id, 1.0) for article_id in article_ids]
            
            cursor.executemany(insert_query, values)
            connection.commit()
            
            logger.debug(f"成功保存事件ID {event_id} 与 {len(article_ids)} 篇文章的关联")
            return True
            
        except Error as e:
            logger.error(f"保存事件与文章关联时出错: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                cursor.close()
    
    def analyze_event_impact(self, event_id: int) -> List[Dict[str, Any]]:
        """
        分析宏观事件对各经济指标的影响
        
        Args:
            event_id: 事件ID
            
        Returns:
            影响分析结果列表
        """
        if not self.enabled:
            logger.info("量化分析已禁用，跳过影响分析")
            return []
        
        # 获取事件信息
        event = self._get_event_by_id(event_id)
        if not event:
            logger.error(f"未找到事件ID {event_id}")
            return []
        
        # 获取历史类似事件
        similar_events = self._find_similar_events(event)
        if not similar_events:
            logger.warning(f"未找到与事件ID {event_id} 类似的历史事件，无法进行影响分析")
            return []
        
        # 分析对各指标的影响
        impacts = []
        
        # 分析对GDP的影响
        gdp_impact = self._analyze_indicator_impact(event, similar_events, 'gdp')
        if gdp_impact:
            impacts.append(gdp_impact)
        
        # 分析对利率的影响
        interest_rate_impact = self._analyze_indicator_impact(event, similar_events, 'interest_rate')
        if interest_rate_impact:
            impacts.append(interest_rate_impact)
        
        # 分析对通货膨胀的影响
        inflation_impact = self._analyze_indicator_impact(event, similar_events, 'inflation')
        if inflation_impact:
            impacts.append(inflation_impact)
        
        # 分析对商品价格的影响
        commodity_impact = self._analyze_indicator_impact(event, similar_events, 'commodity:general')
        if commodity_impact:
            impacts.append(commodity_impact)
        
        # 保存影响分析结果
        for impact in impacts:
            self._save_event_impact(event_id, impact)
        
        logger.info(f"完成事件ID {event_id} 的影响分析，共 {len(impacts)} 项影响")
        return impacts
    
    def _get_event_by_id(self, event_id: int) -> Optional[Dict[str, Any]]:
        """
        根据ID获取事件信息
        
        Args:
            event_id: 事件ID
            
        Returns:
            事件信息字典，如果未找到则返回None
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
            SELECT *
            FROM macro_events
            WHERE id = %s
            """
            
            cursor.execute(query, (event_id,))
            event = cursor.fetchone()
            
            return event
            
        except Error as e:
            logger.error(f"获取事件信息时出错: {e}")
            return None
        finally:
            if connection.is_connected():
                cursor.close()
    
    def _find_similar_events(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        查找与给定事件类似的历史事件
        
        Args:
            event: 事件信息字典
            
        Returns:
            类似事件列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查找同类别的历史事件
            query = """
            SELECT *
            FROM macro_events
            WHERE event_category = %s
              AND id != %s
              AND start_date < %s
            ORDER BY start_date DESC
            LIMIT 10
            """
            
            cursor.execute(query, (
                event['event_category'],
                event['id'],
                event['start_date']
            ))
            
            similar_events = cursor.fetchall()
            
            return similar_events
            
        except Error as e:
            logger.error(f"查找类似事件时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
    
    def _analyze_indicator_impact(self, event: Dict[str, Any], similar_events: List[Dict[str, Any]], 
                                 indicator: str) -> Optional[Dict[str, Any]]:
        """
        分析事件对特定指标的影响
        
        Args:
            event: 当前事件
            similar_events: 类似历史事件
            indicator: 指标名称
            
        Returns:
            影响分析结果，如果无法分析则返回None
        """
        # 获取类似事件发生前后的指标数据
        indicator_changes = []
        
        for similar_event in similar_events:
            # 获取事件前后的指标数据
            before_value = self._get_indicator_value(indicator, similar_event['start_date'], -30)  # 事件前30天
            after_value = self._get_indicator_value(indicator, similar_event['start_date'], 30)    # 事件后30天
            
            if before_value is not None and after_value is not None:
                # 计算变化率
                change_rate = (after_value - before_value) / before_value if before_value != 0 else 0
                indicator_changes.append(change_rate)
        
        if not indicator_changes:
            logger.warning(f"无法获取指标 {indicator} 的历史数据，跳过影响分析")
            return None
        
        # 计算平均变化率
        avg_change = np.mean(indicator_changes)
        
        # 计算置信度（简单方法：样本数量越多，置信度越高）
        confidence = min(0.5 + len(indicator_changes) / 20, 0.95)
        
        # 确定影响类型
        impact_type = 'direct' if abs(avg_change) > 0.01 else 'indirect'
        
        # 确定时间范围
        time_horizon = 'short_term'  # 默认短期影响
        
        # 构建影响分析结果
        impact = {
            'impact_target': indicator,
            'impact_type': impact_type,
            'impact_value': avg_change,
            'confidence': confidence,
            'time_horizon': time_horizon
        }
        
        return impact
    
    def _get_indicator_value(self, indicator: str, date: datetime.date, offset_days: int) -> Optional[float]:
        """
        获取特定日期的指标值
        
        Args:
            indicator: 指标名称
            date: 基准日期
            offset_days: 日期偏移量
            
        Returns:
            指标值，如果未找到则返回None
        """
        # 计算目标日期
        target_date = date + datetime.timedelta(days=offset_days)
        
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 查询最接近目标日期的指标值
            query = """
            SELECT value
            FROM economic_indicators
            WHERE indicator_name = %s
              AND ABS(DATEDIFF(date, %s)) <= 7
            ORDER BY ABS(DATEDIFF(date, %s))
            LIMIT 1
            """
            
            cursor.execute
(Content truncated due to size limit. Use line ranges to read in chunks)