#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
投资归因分析模块
负责评估投资结果并进行归因分析
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
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import setup_logger
from database.db_connector import DatabaseConnector

logger = logging.getLogger(__name__)

class AttributionModel:
    """投资归因分析类，负责评估投资结果并进行归因分析"""
    
    def __init__(self, config_path: str):
        """
        初始化投资归因分析器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.lookback_period = self.config['attribution']['lookback_period']
        self.factors = self.config['attribution']['factors']
        self.report_format = self.config['attribution']['report_format']
        self.enabled = self.config['attribution']['enabled']
        
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
    
    def analyze_investment_decision(self, decision_id: int) -> Dict[str, Any]:
        """
        分析单个投资决策的结果
        
        Args:
            decision_id: 投资决策ID
            
        Returns:
            归因分析结果
        """
        if not self.enabled:
            logger.info("投资归因分析已禁用，跳过分析")
            return {}
        
        # 获取投资决策信息
        decision = self._get_investment_decision(decision_id)
        
        if not decision:
            logger.warning(f"未找到投资决策ID {decision_id}")
            return {}
        
        # 获取当前资产价格
        current_price = self._get_current_asset_price(decision['asset_type'], decision['asset_symbol'])
        
        if current_price is None:
            logger.warning(f"无法获取资产 {decision['asset_symbol']} 的当前价格")
            return {}
        
        # 计算投资回报
        initial_value = decision['price'] * decision['quantity']
        current_value = current_price * decision['quantity']
        absolute_return = current_value - initial_value
        percent_return = (current_value / initial_value - 1) * 100 if initial_value > 0 else 0
        
        # 计算持有期间
        holding_period = (datetime.datetime.now() - decision['decision_date']).days
        annualized_return = ((1 + percent_return / 100) ** (365 / holding_period) - 1) * 100 if holding_period > 0 else 0
        
        # 获取基准回报
        benchmark_return = self._get_benchmark_return(decision['asset_type'], decision['decision_date'])
        
        # 计算超额回报
        excess_return = percent_return - benchmark_return
        
        # 进行归因分析
        attribution = self._perform_attribution_analysis(decision, percent_return, benchmark_return)
        
        # 构建结果
        result = {
            'decision_id': decision_id,
            'asset_type': decision['asset_type'],
            'asset_symbol': decision['asset_symbol'],
            'decision_type': decision['decision_type'],
            'decision_date': decision['decision_date'],
            'evaluation_date': datetime.datetime.now(),
            'initial_price': decision['price'],
            'current_price': current_price,
            'quantity': decision['quantity'],
            'initial_value': initial_value,
            'current_value': current_value,
            'absolute_return': absolute_return,
            'percent_return': percent_return,
            'holding_period_days': holding_period,
            'annualized_return': annualized_return,
            'benchmark_return': benchmark_return,
            'excess_return': excess_return,
            'attribution': attribution
        }
        
        # 保存归因分析结果
        self._save_attribution_result(result)
        
        return result
    
    def _get_investment_decision(self, decision_id: int) -> Optional[Dict[str, Any]]:
        """
        获取投资决策信息
        
        Args:
            decision_id: 投资决策ID
            
        Returns:
            投资决策信息
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
            SELECT *
            FROM investment_decisions
            WHERE id = %s
            """
            
            cursor.execute(query, (decision_id,))
            decision = cursor.fetchone()
            
            return decision
            
        except Error as e:
            logger.error(f"获取投资决策信息时出错: {e}")
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def _get_current_asset_price(self, asset_type: str, asset_symbol: str) -> Optional[float]:
        """
        获取资产的当前价格
        
        Args:
            asset_type: 资产类型
            asset_symbol: 资产代码
            
        Returns:
            当前价格
        """
        # 在实际应用中，应该从市场数据API获取最新价格
        # 这里我们使用一些示例数据
        
        # 股票价格示例
        stock_prices = {
            'AAPL': 175.0,
            'MSFT': 350.0,
            'AMZN': 130.0,
            'GOOGL': 140.0,
            'META': 300.0
        }
        
        # 商品价格示例
        commodity_prices = {
            'oil': 75.0,
            'gold': 1800.0,
            'copper': 4.0,
            'wheat': 7.0,
            'corn': 5.0
        }
        
        if asset_type == 'stock':
            return stock_prices.get(asset_symbol)
        elif asset_type == 'commodity':
            return commodity_prices.get(asset_symbol)
        else:
            return None
    
    def _get_benchmark_return(self, asset_type: str, start_date: datetime.datetime) -> float:
        """
        获取基准回报率
        
        Args:
            asset_type: 资产类型
            start_date: 开始日期
            
        Returns:
            基准回报率（百分比）
        """
        # 在实际应用中，应该从市场数据API获取基准指数的回报率
        # 这里我们使用一些示例数据
        
        # 计算持有天数
        days = (datetime.datetime.now() - start_date).days
        
        # 根据资产类型选择不同的基准回报率
        if asset_type == 'stock':
            # 假设股票基准是S&P 500，年化回报率约10%
            annual_return = 0.10
        elif asset_type == 'commodity':
            # 假设商品基准是Bloomberg Commodity Index，年化回报率约5%
            annual_return = 0.05
        else:
            annual_return = 0.07  # 默认年化回报率
        
        # 计算期间回报率
        period_return = ((1 + annual_return) ** (days / 365) - 1) * 100
        
        return period_return
    
    def _perform_attribution_analysis(self, decision: Dict[str, Any], actual_return: float, 
                                    benchmark_return: float) -> Dict[str, float]:
        """
        进行归因分析
        
        Args:
            decision: 投资决策信息
            actual_return: 实际回报率
            benchmark_return: 基准回报率
            
        Returns:
            归因分析结果
        """
        # 计算超额回报
        excess_return = actual_return - benchmark_return
        
        # 初始化归因结果
        attribution = {}
        
        # 如果没有超额回报，则无需进一步归因
        if abs(excess_return) < 0.01:
            for factor in self.factors:
                attribution[factor] = 0.0
            return attribution
        
        # 获取与决策相关的宏观事件
        events = self._get_related_events(decision)
        
        # 计算宏观事件贡献
        macro_contribution = 0.0
        if events:
            # 简单方法：根据宏观调整比例估算贡献
            macro_adjustment = decision.get('macro_adjustment', 0)
            if macro_adjustment != 0:
                # 假设宏观调整与实际回报有50%的相关性
                expected_impact = macro_adjustment * 0.5
                if (expected_impact > 0 and excess_return > 0) or (expected_impact < 0 and excess_return < 0):
                    # 方向一致，贡献为正
                    macro_contribution = min(abs(excess_return) * 0.4, abs(expected_impact))
                    if excess_return < 0:
                        macro_contribution = -macro_contribution
                else:
                    # 方向相反，贡献为负
                    macro_contribution = -min(abs(excess_return) * 0.2, abs(expected_impact))
        
        # 计算行业因素贡献
        sector_contribution = 0.0
        if decision['asset_type'] == 'stock':
            # 获取行业表现
            sector_performance = self._get_sector_performance(decision['asset_symbol'], decision['decision_date'])
            
            # 计算行业贡献
            if sector_performance:
                sector_excess = sector_performance - benchmark_return
                # 假设行业因素占超额回报的30%
                sector_contribution = sector_excess * 0.3
        
        # 计算公司/商品特定因素贡献
        specific_contribution = 0.0
        if decision['asset_type'] == 'stock':
            # 获取公司特定事件
            company_events = self._get_company_events(decision['asset_symbol'], decision['decision_date'])
            
            # 根据公司事件数量和重要性估算贡献
            if company_events:
                importance_sum = sum(event.get('importance', 1) for event in company_events)
                # 假设公司特定因素占超额回报的40%
                specific_contribution = excess_return * 0.4 * min(importance_sum / 10, 1.0)
        elif decision['asset_type'] == 'commodity':
            # 获取商品特定因素
            commodity_factors = self._get_commodity_specific_factors(decision['asset_symbol'], decision['decision_date'])
            
            # 根据商品特定因素估算贡献
            if commodity_factors:
                # 假设商品特定因素占超额回报的50%
                specific_contribution = excess_return * 0.5 * commodity_factors.get('impact', 0.5)
        
        # 计算市场情绪贡献
        sentiment_contribution = 0.0
        # 获取市场情绪数据
        sentiment_data = self._get_market_sentiment(decision['asset_type'], decision['decision_date'])
        
        if sentiment_data:
            # 假设市场情绪占超额回报的20%
            sentiment_impact = sentiment_data.get('impact', 0)
            sentiment_contribution = excess_return * 0.2 * sentiment_impact
        
        # 分配剩余的未解释部分
        explained_return = macro_contribution + sector_contribution + specific_contribution + sentiment_contribution
        unexplained = excess_return - explained_return
        
        # 构建归因结果
        attribution = {
            'macro_events': macro_contribution,
            'sector_performance': sector_contribution,
            'company_specific': specific_contribution if decision['asset_type'] == 'stock' else 0,
            'commodity_specific': specific_contribution if decision['asset_type'] == 'commodity' else 0,
            'market_sentiment': sentiment_contribution,
            'unexplained': unexplained
        }
        
        # 确保所有因素的贡献总和等于超额回报
        total_attribution = sum(attribution.values())
        if abs(total_attribution - excess_return) > 0.01:
            # 调整未解释部分
            attribution['unexplained'] += (excess_return - total_attribution)
        
        return attribution
    
    def _get_related_events(self, decision: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        获取与投资决策相关的宏观事件
        
        Args:
            decision: 投资决策信息
            
        Returns:
            相关宏观事件列表
        """
        connection = self.db_connector.get_connection()
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 获取决策前后的宏观事件
            start_date = decision['decision_date'] - datetime.timedelta(days=30)
            end_date = datetime.datetime.now()
            
            query = """
            SELECT *
            FROM macro_events
            WHERE start_date BETWEEN %s AND %s
            ORDER BY importance DESC, start_date
            """
            
            cursor.execute(query, (start_date, end_date))
            events = cursor.fetchall()
            
            return events
            
        except Error as e:
            logger.error(f"获取相关宏观事件时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def _get_sector_performance(self, stock_symbol: str, start_date: datetime.datetime) -> float:
        """
        获取股票所属行业的表现
        
        Args:
            stock_symbol: 股票代码
            start_date: 开始日期
            
        Returns:
            行业回报率（百分比）
        """
        # 在实际应用中，应该从市场数据API获取行业指数的回报率
        # 这里我们使用一些示例数据
        
        # 股票行业映射
        stock_sectors = {
            'AAPL': 'Technology',
            'MSFT': 'Technology',
            'AMZN': 'Consumer Cyclical',
            'GOOGL': 'Communication Services',
            'META': 'Communication Services'
        }
        
        # 行业回报率映射
        sector_returns = {
            'Technology': 15.0,
            'Consumer Cyclical': 10.0,
            'Communication Services': 8.0,
            'Healthcare': 7.0,
            'Financials': 9.0
        }
        
        sector = stock_sectors.get(stock_symbol, 'Unknown')
        return sector_returns.get(sector, 0.0)
    
    def _get_company_events(self, stock_symbol: str, start_date: datetime.datetime) -> List[Dict[str, Any]]:
        """
        获取公司特定事件
        
        Args:
            stock_symbol: 股票代码
            start_date: 开始日期
            
        Returns:
            公司事件列表
        """
        # 在实际应用中，应该从新闻API或公司公告获取公司事件
        # 这里我们返回一些示例数据
        
        # 示例公司事件
        company_events = []
        
        if stock_symbol == 'AAPL':
            company_events = [
                {'event': 'New Product Launch', 'date': datetime.datetime.now() - datetime.timedelta(days=20), 'importance': 3},
                {'event': 'Quarterly Earnings', 'date': datetime.datetime.now() - datetime.timedelta(days=15), 'importance': 4}
            ]
        elif stock_symbol == 'MSFT':
            company_events = [
                {'event': 'Cloud Service Expansion', 'date': datetime.datetime.now() - datetime.timedelta(days=25), 'importance': 2},
                {'event': 'Acquisition Announcement', 'date': datetime.datetime.now() - datetime.timedelta(days=10), 'importance': 3}
            ]
        
        return company_events
    
    def _get_commodity_specific_factors(self, commodity_name: str, start_date: datetime.datetime) -> Dict[str, Any]:
        """
        获取商品特定因素
        
        Args:
            commodity_name: 商品名称
            start_date: 开始日期
            
        Returns:
            商品特定因素
        """
        # 在实际应用中，应该从商品市场数据获取特定因素
        # 这里我们返回一些示例数据
        
        # 示例商品特定因素
        commodity_factors = {
            'oil': {'supply_shock': True, 'demand_change': -0.05, 'impact': 0.7},
            'gold': {'safe_haven_demand': True, 'inflation_hedge': True, 'impact': 0.6},
            'copper': {'industrial_demand': 0.03, 'supply_constraints': False, 'impact': 0.4},
            'wheat': {'harvest_conditions': 'good', 'export_restrictions': False, 'impact': 0.3},
            'corn': {'weather_impact': -0.02, 'ethanol_demand': 0.01, 'impact': 0.5}
        }
        
        return commodity_factors.get(commodity_name, {'impact': 0.5})
    
    def _get_market_sentiment(self, asset_type: str, start_date: datetime.datetime) -> Dict[str, Any]:
        """
        获取市场情绪数据
        
        Args:
            asset_type: 资产类型
            start_date: 开始日期
            
        Returns:
            市场情绪数据
        """
        # 在实际应用中，应该从情绪分析API或社交媒体数据获取市场情绪
        # 这里我们返回一些示例数据
        
        # 计算持有天数
        days = (dateti
(Content truncated due to size limit. Use line ranges to read in chunks)