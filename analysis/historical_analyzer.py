#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
历史数据分析模块
负责获取历史数据并分析宏观事件与市场数据的关系
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
import requests
import json
from scipy import stats

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)

class HistoricalAnalyzer:
    """历史数据分析类，负责获取历史数据并分析宏观事件与市场数据的关系"""
    
    def __init__(self, config_path: str):
        """
        初始化历史数据分析器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.historical_sources = self.config['historical_data']['sources']
        self.cache_days = self.config['historical_data']['local_cache_days']
        
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
    
    def get_historical_data(self, data_type: str, start_date: datetime.date, 
                           end_date: datetime.date = None, source: str = None) -> pd.DataFrame:
        """
        获取历史数据
        
        Args:
            data_type: 数据类型，如'gdp', 'interest_rates', 'stock_prices'等
            start_date: 开始日期
            end_date: 结束日期，默认为当前日期
            source: 数据源，如'fred', 'yahoo_finance'等，默认根据数据类型自动选择
            
        Returns:
            包含历史数据的DataFrame
        """
        if end_date is None:
            end_date = datetime.date.today()
        
        # 首先尝试从本地数据库获取
        local_data = self._get_data_from_db(data_type, start_date, end_date)
        
        if not local_data.empty:
            logger.info(f"从本地数据库获取 {data_type} 数据成功，共 {len(local_data)} 条记录")
            return local_data
        
        # 如果本地没有数据，从外部源获取
        if source is None:
            source = self._get_default_source_for_type(data_type)
        
        if source == 'fred':
            data = self._get_data_from_fred(data_type, start_date, end_date)
        elif source == 'yahoo_finance':
            data = self._get_data_from_yahoo(data_type, start_date, end_date)
        elif source == 'world_bank':
            data = self._get_data_from_world_bank(data_type, start_date, end_date)
        else:
            logger.error(f"不支持的数据源: {source}")
            return pd.DataFrame()
        
        # 保存到本地数据库
        if not data.empty:
            self._save_data_to_db(data_type, data, source)
        
        return data
    
    def _get_default_source_for_type(self, data_type: str) -> str:
        """
        根据数据类型获取默认数据源
        
        Args:
            data_type: 数据类型
            
        Returns:
            默认数据源名称
        """
        type_to_source = {
            'gdp': 'fred',
            'interest_rates': 'fred',
            'unemployment': 'fred',
            'cpi': 'fred',
            'ppi': 'fred',
            'stock_prices': 'yahoo_finance',
            'indices': 'yahoo_finance',
            'global_trade': 'world_bank',
            'commodity_prices': 'world_bank'
        }
        
        return type_to_source.get(data_type, 'fred')
    
    def _get_data_from_db(self, data_type: str, start_date: datetime.date, 
                         end_date: datetime.date) -> pd.DataFrame:
        """
        从本地数据库获取历史数据
        
        Args:
            data_type: 数据类型
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            包含历史数据的DataFrame
        """
        connection = self.db_connector.get_connection()
        
        try:
            query = """
            SELECT date, value, unit, country
            FROM economic_indicators
            WHERE indicator_name = %s
              AND date BETWEEN %s AND %s
            ORDER BY date
            """
            
            df = pd.read_sql(query, connection, params=(data_type, start_date, end_date))
            
            return df
            
        except Error as e:
            logger.error(f"从数据库获取数据时出错: {e}")
            return pd.DataFrame()
        finally:
            if connection.is_connected():
                connection.close()
    
    def _save_data_to_db(self, data_type: str, data: pd.DataFrame, source: str) -> bool:
        """
        保存历史数据到本地数据库
        
        Args:
            data_type: 数据类型
            data: 数据DataFrame
            source: 数据源
            
        Returns:
            是否成功保存
        """
        if data.empty:
            return False
        
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor()
            
            # 准备插入数据
            insert_query = """
            INSERT INTO economic_indicators 
            (indicator_name, indicator_category, value, unit, country, date, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            value = VALUES(value), source = VALUES(source)
            """
            
            # 确定指标类别
            category = data_type.split(':')[0] if ':' in data_type else data_type
            
            # 准备数据
            values = []
            for _, row in data.iterrows():
                values.append((
                    data_type,
                    category,
                    float(row['value']),
                    row.get('unit', ''),
                    row.get('country', 'global'),
                    row['date'],
                    source
                ))
            
            # 批量插入
            cursor.executemany(insert_query, values)
            connection.commit()
            
            logger.info(f"成功保存 {len(values)} 条 {data_type} 数据到数据库")
            return True
            
        except Error as e:
            logger.error(f"保存数据到数据库时出错: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def _get_data_from_fred(self, data_type: str, start_date: datetime.date, 
                           end_date: datetime.date) -> pd.DataFrame:
        """
        从FRED获取历史数据
        
        Args:
            data_type: 数据类型
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            包含历史数据的DataFrame
        """
        # 将数据类型映射到FRED系列ID
        type_to_series = {
            'gdp': 'GDP',
            'interest_rates': 'FEDFUNDS',
            'unemployment': 'UNRATE',
            'cpi': 'CPIAUCSL',
            'ppi': 'PPIACO'
        }
        
        series_id = type_to_series.get(data_type)
        if not series_id:
            logger.error(f"FRED不支持的数据类型: {data_type}")
            return pd.DataFrame()
        
        # 在实际应用中，这里应该使用FRED API获取数据
        # 由于当前环境限制，我们模拟一些数据
        logger.warning(f"使用模拟数据代替FRED API获取 {data_type} 数据")
        
        # 生成日期范围
        date_range = pd.date_range(start=start_date, end=end_date, freq='M')
        
        # 根据数据类型生成不同的模拟数据
        if data_type == 'gdp':
            # 模拟GDP数据，按季度增长
            values = np.linspace(20000, 25000, len(date_range))
            values = values * (1 + np.random.normal(0, 0.01, len(date_range)))
            unit = 'Billions of Dollars'
        elif data_type == 'interest_rates':
            # 模拟利率数据，在2%到5%之间波动
            values = np.random.uniform(2, 5, len(date_range))
            unit = 'Percent'
        elif data_type == 'unemployment':
            # 模拟失业率数据，在3%到8%之间波动
            values = np.random.uniform(3, 8, len(date_range))
            unit = 'Percent'
        elif data_type == 'cpi':
            # 模拟CPI数据，逐渐上升
            values = np.linspace(250, 280, len(date_range))
            values = values * (1 + np.random.normal(0, 0.005, len(date_range)))
            unit = 'Index 1982-1984=100'
        elif data_type == 'ppi':
            # 模拟PPI数据，逐渐上升
            values = np.linspace(200, 220, len(date_range))
            values = values * (1 + np.random.normal(0, 0.008, len(date_range)))
            unit = 'Index 1982=100'
        else:
            values = np.zeros(len(date_range))
            unit = ''
        
        # 创建DataFrame
        df = pd.DataFrame({
            'date': date_range.date,
            'value': values,
            'unit': unit,
            'country': 'US'
        })
        
        return df
    
    def _get_data_from_yahoo(self, data_type: str, start_date: datetime.date, 
                            end_date: datetime.date) -> pd.DataFrame:
        """
        从Yahoo Finance获取历史数据
        
        Args:
            data_type: 数据类型
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            包含历史数据的DataFrame
        """
        # 确定股票代码或指数
        symbol = None
        if data_type == 'stock_prices':
            symbol = 'AAPL'  # 默认使用苹果股票
        elif data_type == 'indices':
            symbol = '^GSPC'  # S&P 500指数
        elif ':' in data_type:
            # 支持如'stock_prices:AAPL'这样的格式
            symbol = data_type.split(':')[1]
        
        if not symbol:
            logger.error(f"Yahoo Finance不支持的数据类型: {data_type}")
            return pd.DataFrame()
        
        # 在实际应用中，这里应该使用Yahoo Finance API获取数据
        # 由于当前环境限制，我们模拟一些数据
        logger.warning(f"使用模拟数据代替Yahoo Finance API获取 {symbol} 数据")
        
        # 生成日期范围（交易日）
        date_range = pd.date_range(start=start_date, end=end_date, freq='B')
        
        # 生成模拟股价数据
        if symbol == 'AAPL':
            # 模拟苹果股价，从150到200之间波动
            start_price = 150
            volatility = 0.015
        elif symbol == '^GSPC':
            # 模拟S&P 500指数，从4000到4500之间波动
            start_price = 4000
            volatility = 0.01
        else:
            # 其他股票，从50到100之间波动
            start_price = 50
            volatility = 0.02
        
        # 生成随机游走价格
        prices = [start_price]
        for i in range(1, len(date_range)):
            change = prices[-1] * np.random.normal(0, volatility)
            prices.append(prices[-1] + change)
        
        # 创建DataFrame
        df = pd.DataFrame({
            'date': date_range.date,
            'value': prices,
            'unit': 'USD',
            'country': 'US'
        })
        
        return df
    
    def _get_data_from_world_bank(self, data_type: str, start_date: datetime.date, 
                                 end_date: datetime.date) -> pd.DataFrame:
        """
        从World Bank获取历史数据
        
        Args:
            data_type: 数据类型
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            包含历史数据的DataFrame
        """
        # 将数据类型映射到World Bank指标代码
        type_to_indicator = {
            'global_trade': 'NE.TRD.GNFS.ZS',  # 贸易占GDP的百分比
            'commodity_prices': 'PCOMM'  # 商品价格指数
        }
        
        indicator = type_to_indicator.get(data_type)
        if not indicator:
            logger.error(f"World Bank不支持的数据类型: {data_type}")
            return pd.DataFrame()
        
        # 在实际应用中，这里应该使用World Bank API获取数据
        # 由于当前环境限制，我们模拟一些数据
        logger.warning(f"使用模拟数据代替World Bank API获取 {data_type} 数据")
        
        # 生成日期范围（按年）
        years = range(start_date.year, end_date.year + 1)
        dates = [datetime.date(year, 1, 1) for year in years]
        
        # 根据数据类型生成不同的模拟数据
        if data_type == 'global_trade':
            # 模拟全球贸易占GDP的百分比，在50%到60%之间波动
            values = np.random.uniform(50, 60, len(dates))
            unit = 'Percent of GDP'
        elif data_type == 'commodity_prices':
            # 模拟商品价格指数，逐渐上升
            values = np.linspace(80, 120, len(dates))
            values = values * (1 + np.random.normal(0, 0.05, len(dates)))
            unit = 'Index 2010=100'
        else:
            values = np.zeros(len(dates))
            unit = ''
        
        # 创建DataFrame
        df = pd.DataFrame({
            'date': dates,
            'value': values,
            'unit': unit,
            'country': 'global'
        })
        
        return df
    
    def analyze_event_market_correlation(self, event_category: str, market_data_type: str, 
                                        window_days: int = 30) -> Dict[str, Any]:
        """
        分析特定类型宏观事件与市场数据的相关性
        
        Args:
            event_category: 事件类别
            market_data_type: 市场数据类型
            window_days: 事件前后的窗口天数
            
        Returns:
            相关性分析结果
        """
        # 获取该类别的历史事件
        events = self._get_events_by_category(event_category)
        
        if not events:
            logger.warning(f"未找到类别为 {event_category} 的历史事件")
            return {}
        
        # 收集事件前后的市场数据变化
        pre_event_values = []
        post_event_values = []
        
        for event in events:
            event_date = event['start_date']
            
            # 获取事件前的市场数据
            pre_start = event_date - datetime.timedelta(days=window_days)
            pre_end = event_date
            pre_data = self.get_historical_data(market_data_type, pre_start, pre_end)
            
            # 获取事件后的市场数据
            post_start = event_date
            post_end = event_date + datetime.timedelta(days=window_days)
            post_data = self.get_historical_data(market_data_type, post_start, post_end)
            
            if not pre_data.empty and not post_data.empty:
                # 计算事件前后的平均值
                pre_avg = pre_data['value'].mean()
                post_avg = post_data['value'].mean()
                
                pre_event_values.append(pre_avg)
                post_event_values.append(post_avg)
        
        if not pre_event_values or not post_event_values:
            logger.warning(f"没有足够的数据分析 {event_category} 事件与 {market_data_type} 的相关性")
            return {}
        
        # 计算事件前后的变化率
        changes = [(post - pre) / pre for pre, post in zip(pre_event_values, post_event_values)]
        
        # 计算统计指标
        avg_change = np.mean(changes)
        median_change = np.median(changes)
        std_dev = np.std(changes)
        
        # 进行t检验，判断变化是否显著
        t_stat, p_value = stats.ttest_1samp(changes, 0)
        
        # 判断相关性方向和强度
        if p_value < 0.05:
            if avg_change > 0:
                direction = 'positive'
            else:
                direction = 'negative'
                
            if abs(avg_change) > 0.1:
                strength = 'strong'
            elif abs(avg_change) > 0.05:
                strength = 'moderate'
            else:
                strength = 'weak'
        else:
            direction = 'neutral'
            strength = 'insignificant'
        
        # 构建结果
        result = {
            'event_category': event_category,
            'market_data_type': market_data_type,
            'sample_size': len(changes),
            'average_change': avg_change,
            'median_change': median_change,
            'standard_deviation': std_dev,
            't_statistic': t_stat,
            'p_value': p_value,
            'correlation_direction': direction,
            'correlation_strength': strength,
            'window_days': window_days
        }
        
        return result
    
    def _get_events_by_category(self, category: str) -> List[Dict[str, Any]]:
        """
        获取特定类别的历史事件
        
        Args:
            category: 事件类别
            
        Returns:
            事件列表
        """
      
(Content truncated due to size limit. Use line ranges to read in chunks)