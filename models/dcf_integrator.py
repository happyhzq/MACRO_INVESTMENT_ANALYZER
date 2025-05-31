#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DCF模型整合模块
负责将宏观分析结果整合到DCF模型中
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
import json

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from database.db_connector import DatabaseConnector
from analysis.historical_analyzer import HistoricalAnalyzer

logger = logging.getLogger(__name__)

class DCFIntegrator:
    """DCF模型整合类，负责将宏观分析结果整合到DCF模型中"""
    
    def __init__(self, config_path: str):
        """
        初始化DCF模型整合器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.historical_analyzer = HistoricalAnalyzer(config_path)
        self.impact_factors = self.config['model_integration']['dcf']['impact_factors']
        self.enabled = self.config['model_integration']['dcf']['enabled']
        
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
    
    def adjust_dcf_parameters(self, stock_symbol: str, event_ids: List[int] = None) -> Dict[str, Any]:
        """
        根据宏观事件调整DCF模型参数
        
        Args:
            stock_symbol: 股票代码
            event_ids: 要考虑的宏观事件ID列表，如果为None则考虑所有最近事件
            
        Returns:
            调整后的DCF参数
        """
        if not self.enabled:
            logger.info("DCF模型整合已禁用，跳过参数调整")
            return {}
        
        # 获取原始DCF参数
        original_params = self._get_original_dcf_parameters(stock_symbol)
        
        if not original_params:
            logger.warning(f"未找到股票 {stock_symbol} 的原始DCF参数")
            return {}
        
        # 获取要考虑的宏观事件
        if event_ids is None:
            events = self._get_recent_events(30)  # 最近30天的事件
        else:
            events = self._get_events_by_ids(event_ids)
        
        if not events:
            logger.info("没有找到需要考虑的宏观事件，使用原始DCF参数")
            return original_params
        
        # 调整DCF参数
        adjusted_params = original_params.copy()
        adjustments = {}
        
        # 遍历所有影响因子
        for factor in self.impact_factors:
            factor_name = factor['name']
            factor_weight = factor['weight']
            factor_source = factor['source']
            
            # 获取该因子的调整值
            adjustment = self._calculate_factor_adjustment(factor_name, factor_source, events)
            
            if adjustment:
                # 应用调整
                original_value = adjusted_params.get(factor_name, 0)
                adjusted_value = original_value * (1 + adjustment * factor_weight)
                
                adjusted_params[factor_name] = adjusted_value
                
                # 记录调整信息
                adjustments[factor_name] = {
                    'original_value': original_value,
                    'adjusted_value': adjusted_value,
                    'adjustment_factor': adjustment,
                    'weight': factor_weight
                }
                
                logger.info(f"调整股票 {stock_symbol} 的 {factor_name} 参数: {original_value} -> {adjusted_value}")
        
        # 保存调整记录
        self._save_dcf_adjustments(stock_symbol, adjustments, events)
        
        return adjusted_params
    
    def _get_original_dcf_parameters(self, stock_symbol: str) -> Dict[str, float]:
        """
        获取股票的原始DCF参数
        
        Args:
            stock_symbol: 股票代码
            
        Returns:
            原始DCF参数
        """
        # 在实际应用中，这些参数应该从数据库或外部系统获取
        # 这里我们使用一些默认值作为示例
        
        # 默认参数
        default_params = {
            'discount_rate': 0.10,  # 折现率
            'growth_rate': 0.05,    # 增长率
            'terminal_growth_rate': 0.02,  # 永续增长率
            'tax_rate': 0.25,       # 税率
            'risk_free_rate': 0.03, # 无风险利率
            'market_risk_premium': 0.05,  # 市场风险溢价
            'beta': 1.0             # 贝塔系数
        }
        
        # 根据股票代码调整一些参数
        if stock_symbol == 'AAPL':
            default_params['growth_rate'] = 0.08
            default_params['beta'] = 1.2
        elif stock_symbol == 'MSFT':
            default_params['growth_rate'] = 0.07
            default_params['beta'] = 1.1
        elif stock_symbol == 'AMZN':
            default_params['growth_rate'] = 0.10
            default_params['beta'] = 1.3
        
        return default_params
    
    def _get_recent_events(self, days: int) -> List[Dict[str, Any]]:
        """
        获取最近的宏观事件
        
        Args:
            days: 天数
            
        Returns:
            最近的宏观事件列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
            SELECT *
            FROM macro_events
            WHERE start_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY importance DESC, start_date DESC
            """
            
            cursor.execute(query, (days,))
            events = cursor.fetchall()
            
            return events
            
        except Error as e:
            logger.error(f"获取最近事件时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def _get_events_by_ids(self, event_ids: List[int]) -> List[Dict[str, Any]]:
        """
        根据ID获取宏观事件
        
        Args:
            event_ids: 事件ID列表
            
        Returns:
            宏观事件列表
        """
        if not event_ids:
            return []
        
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 构建IN子句
            placeholders = ', '.join(['%s'] * len(event_ids))
            
            query = f"""
            SELECT *
            FROM macro_events
            WHERE id IN ({placeholders})
            ORDER BY importance DESC, start_date DESC
            """
            
            cursor.execute(query, event_ids)
            events = cursor.fetchall()
            
            return events
            
        except Error as e:
            logger.error(f"获取事件时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def _calculate_factor_adjustment(self, factor_name: str, factor_source: str, 
                                   events: List[Dict[str, Any]]) -> float:
        """
        计算因子的调整值
        
        Args:
            factor_name: 因子名称
            factor_source: 因子数据源
            events: 宏观事件列表
            
        Returns:
            调整值
        """
        # 根据因子源获取调整值
        if factor_source == 'fred':
            # 从经济指标获取调整值
            return self._get_adjustment_from_economic_indicators(factor_name, events)
        elif factor_source == 'analysis':
            # 从分析结果获取调整值
            return self._get_adjustment_from_analysis(factor_name, events)
        else:
            logger.warning(f"不支持的因子源: {factor_source}")
            return 0.0
    
    def _get_adjustment_from_economic_indicators(self, factor_name: str, 
                                               events: List[Dict[str, Any]]) -> float:
        """
        从经济指标获取调整值
        
        Args:
            factor_name: 因子名称
            events: 宏观事件列表
            
        Returns:
            调整值
        """
        # 映射因子名称到经济指标
        factor_to_indicator = {
            'interest_rate': 'interest_rates',
            'gdp_growth': 'gdp',
            'inflation': 'cpi'
        }
        
        indicator = factor_to_indicator.get(factor_name)
        if not indicator:
            logger.warning(f"无法将因子 {factor_name} 映射到经济指标")
            return 0.0
        
        # 获取事件对该指标的影响
        total_impact = 0.0
        total_weight = 0.0
        
        for event in events:
            # 获取事件对该指标的影响
            impacts = self._get_event_impacts(event['id'], indicator)
            
            for impact in impacts:
                # 根据置信度加权
                weight = impact['confidence']
                total_impact += impact['impact_value'] * weight
                total_weight += weight
        
        # 计算加权平均影响
        if total_weight > 0:
            avg_impact = total_impact / total_weight
        else:
            avg_impact = 0.0
        
        return avg_impact
    
    def _get_adjustment_from_analysis(self, factor_name: str, 
                                    events: List[Dict[str, Any]]) -> float:
        """
        从分析结果获取调整值
        
        Args:
            factor_name: 因子名称
            events: 宏观事件列表
            
        Returns:
            调整值
        """
        if factor_name == 'sentiment':
            # 计算事件的平均情绪极性
            total_polarity = 0.0
            count = 0
            
            for event in events:
                # 获取事件的情绪分析结果
                sentiment = self._get_event_sentiment(event['id'])
                
                if sentiment:
                    total_polarity += sentiment['avg_polarity']
                    count += 1
            
            # 计算平均情绪极性
            if count > 0:
                avg_polarity = total_polarity / count
            else:
                avg_polarity = 0.0
            
            # 将情绪极性转换为调整值
            # 情绪极性范围为[-1, 1]，我们将其映射到合理的调整范围
            adjustment = avg_polarity * 0.05  # 最大±5%的调整
            
            return adjustment
        else:
            logger.warning(f"不支持的分析因子: {factor_name}")
            return 0.0
    
    def _get_event_impacts(self, event_id: int, indicator: str) -> List[Dict[str, Any]]:
        """
        获取事件对特定指标的影响
        
        Args:
            event_id: 事件ID
            indicator: 指标名称
            
        Returns:
            影响列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
            SELECT *
            FROM event_impacts
            WHERE event_id = %s AND impact_target = %s
            """
            
            cursor.execute(query, (event_id, indicator))
            impacts = cursor.fetchall()
            
            return impacts
            
        except Error as e:
            logger.error(f"获取事件影响时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def _get_event_sentiment(self, event_id: int) -> Optional[Dict[str, float]]:
        """
        获取事件的情绪分析结果
        
        Args:
            event_id: 事件ID
            
        Returns:
            情绪分析结果
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 获取与事件相关的文章
            query = """
            SELECT article_id
            FROM event_articles
            WHERE event_id = %s
            """
            
            cursor.execute(query, (event_id,))
            article_rows = cursor.fetchall()
            
            if not article_rows:
                return None
            
            article_ids = [row['article_id'] for row in article_rows]
            
            # 获取这些文章的情绪分析结果
            placeholders = ', '.join(['%s'] * len(article_ids))
            
            query = f"""
            SELECT AVG(polarity) as avg_polarity, AVG(subjectivity) as avg_subjectivity
            FROM sentiment_analysis
            WHERE article_id IN ({placeholders})
            """
            
            cursor.execute(query, article_ids)
            result = cursor.fetchone()
            
            return result
            
        except Error as e:
            logger.error(f"获取事件情绪分析结果时出错: {e}")
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def _save_dcf_adjustments(self, stock_symbol: str, adjustments: Dict[str, Dict[str, Any]], 
                            events: List[Dict[str, Any]]) -> bool:
        """
        保存DCF参数调整记录
        
        Args:
            stock_symbol: 股票代码
            adjustments: 调整信息
            events: 相关事件
            
        Returns:
            是否成功保存
        """
        if not adjustments:
            return True
        
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            # 当前日期
            adjustment_date = datetime.date.today()
            
            # 保存每个因子的调整
            for factor_name, adjustment in adjustments.items():
                # 找出影响最大的事件
                max_event_id = None
                if events:
                    max_event_id = events[0]['id']
                
                # 插入调整记录
                insert_query = """
                INSERT INTO dcf_adjustments 
                (stock_symbol, adjustment_date, factor_name, original_value, adjusted_value, 
                 adjustment_reason, confidence, event_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                reason = f"Adjusted based on {len(events)} macro events"
                confidence = min(0.5 + len(events) / 20, 0.95)  # 简单的置信度计算
                
                cursor.execute(insert_query, (
                    stock_symbol,
                    adjustment_date,
                    factor_name,
                    adjustment['original_value'],
                    adjustment['adjusted_value'],
                    reason,
                    confidence,
                    max_event_id
                ))
            
            connection.commit()
            
            logger.info(f"成功保存股票 {stock_symbol} 的DCF参数调整记录")
            return True
            
        except Error as e:
            logger.error(f"保存DCF参数调整记录时出错: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def calculate_dcf_valuation(self, stock_symbol: str, event_ids: List[int] = None) -> Dict[str, Any]:
        """
        计算考虑宏观事件的DCF估值
        
        Args:
            stock_symbol: 股票代码
            event_ids: 要考虑的宏观事件ID列表
            
        Returns:
            DCF估值结果
        """
        if not self.enabled:
            logger.info("DCF模型整合已禁用，跳过估值计算")
            return {}
        
        # 获取调整后的DCF参数
        adjusted_params = self.adjust_dcf_parameters(stock_symbol, event_ids)
        
        if not adjusted_params:
            logger.warning(f"无法获取股票 {stock_symbol} 的DCF参数")
            return {}
        
        # 获取股票的财务数据
        financials = self._get_stock_financials(stock_symbol)
        
        if not financials:
            logger.warning(f"无法获取股票 {stock_symbol} 的财务数据")
            return {}
        
        # 计算DCF估值
        try:
            # 提取参数
            discount_rate = adjusted_params.get('discount_rate', 0.10)
            growth_rate = adjusted_params.get('growth_rate', 0.05)
            terminal_growth_rate = adjusted_params.get('terminal_growth_rate', 0.02)
            
            # 提取财务数据
  
(Content truncated due to size limit. Use line ranges to read in chunks)