#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
商品供需预测整合模块
负责将宏观分析结果整合到商品供需预测模型中
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

class CommodityIntegrator:
    """商品供需预测整合类，负责将宏观分析结果整合到商品供需预测模型中"""
    
    def __init__(self, config_path: str):
        """
        初始化商品供需预测整合器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.historical_analyzer = HistoricalAnalyzer(config_path)
        self.impact_factors = self.config['model_integration']['commodity']['impact_factors']
        self.enabled = self.config['model_integration']['commodity']['enabled']
        
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
    
    def adjust_commodity_parameters(self, commodity_name: str, event_ids: List[int] = None) -> Dict[str, Any]:
        """
        根据宏观事件调整商品供需预测参数
        
        Args:
            commodity_name: 商品名称
            event_ids: 要考虑的宏观事件ID列表，如果为None则考虑所有最近事件
            
        Returns:
            调整后的商品供需参数
        """
        if not self.enabled:
            logger.info("商品供需预测整合已禁用，跳过参数调整")
            return {}
        
        # 获取原始商品供需参数
        original_params = self._get_original_commodity_parameters(commodity_name)
        
        if not original_params:
            logger.warning(f"未找到商品 {commodity_name} 的原始供需参数")
            return {}
        
        # 获取要考虑的宏观事件
        if event_ids is None:
            events = self._get_recent_events(30)  # 最近30天的事件
        else:
            events = self._get_events_by_ids(event_ids)
        
        if not events:
            logger.info("没有找到需要考虑的宏观事件，使用原始商品供需参数")
            return original_params
        
        # 调整商品供需参数
        adjusted_params = original_params.copy()
        adjustments = {}
        
        # 遍历所有影响因子
        for factor in self.impact_factors:
            factor_name = factor['name']
            factor_weight = factor['weight']
            factor_source = factor['source']
            
            # 获取该因子的调整值
            adjustment = self._calculate_factor_adjustment(factor_name, factor_source, events, commodity_name)
            
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
                
                logger.info(f"调整商品 {commodity_name} 的 {factor_name} 参数: {original_value} -> {adjusted_value}")
        
        # 保存调整记录
        self._save_commodity_adjustments(commodity_name, adjustments, events)
        
        return adjusted_params
    
    def predict_commodity_price(self, commodity_name: str, event_ids: List[int] = None) -> Dict[str, Any]:
        """
        预测商品价格
        
        Args:
            commodity_name: 商品名称
            event_ids: 要考虑的宏观事件ID列表，如果为None则考虑所有最近事件
            
        Returns:
            价格预测结果
        """
        # 调整商品供需参数
        adjusted_params = self.adjust_commodity_parameters(commodity_name, event_ids)
        
        if not adjusted_params:
            logger.warning(f"无法获取商品 {commodity_name} 的调整参数，无法预测价格")
            return {}
        
        # 获取当前价格
        current_price = self._get_current_commodity_price(commodity_name)
        
        if current_price <= 0:
            logger.warning(f"无法获取商品 {commodity_name} 的当前价格，无法预测价格")
            return {}
        
        # 计算供需平衡
        supply = adjusted_params.get('supply', 0)
        demand = adjusted_params.get('demand', 0)
        inventory = adjusted_params.get('inventory', 0)
        
        if supply <= 0 or demand <= 0:
            logger.warning(f"商品 {commodity_name} 的供应量或需求量无效，无法预测价格")
            return {}
        
        # 计算供需比
        supply_demand_ratio = supply / demand
        
        # 计算库存覆盖天数
        inventory_days = inventory / (demand / 365) if demand > 0 else 0
        
        # 计算价格弹性
        price_elasticity = adjusted_params.get('price_elasticity', -0.5)
        
        # 计算季节性因子
        seasonal_factor = adjusted_params.get('seasonal_factor', 1.0)
        
        # 计算预测价格变化率
        # 供需比<1表示供不应求，价格上涨；供需比>1表示供过于求，价格下跌
        price_change_pct = ((1 / supply_demand_ratio) - 1) * (-1 / price_elasticity) * seasonal_factor
        
        # 考虑库存因素
        # 库存天数越多，价格变化越小
        inventory_factor = max(0.5, min(1.5, 30 / inventory_days)) if inventory_days > 0 else 1.5
        price_change_pct *= inventory_factor
        
        # 限制价格变化在合理范围内
        price_change_pct = max(min(price_change_pct, 0.5), -0.3)  # 最大涨50%，最大跌30%
        
        # 计算预测价格
        predicted_price = current_price * (1 + price_change_pct)
        
        # 构建预测结果
        prediction = {
            'commodity_name': commodity_name,
            'current_price': current_price,
            'predicted_price': predicted_price,
            'price_change_pct': price_change_pct * 100,  # 转换为百分比
            'supply': supply,
            'demand': demand,
            'supply_demand_ratio': supply_demand_ratio,
            'inventory_days': inventory_days,
            'prediction_date': datetime.datetime.now().strftime('%Y-%m-%d'),
            'confidence': self._calculate_prediction_confidence(adjusted_params, event_ids)
        }
        
        # 保存预测结果
        self._save_price_prediction(prediction)
        
        return prediction
    
    def _get_original_commodity_parameters(self, commodity_name: str) -> Dict[str, float]:
        """
        获取商品的原始供需参数
        
        Args:
            commodity_name: 商品名称
            
        Returns:
            原始供需参数
        """
        # 在实际应用中，这些参数应该从数据库或外部系统获取
        # 这里我们使用一些默认值作为示例
        
        # 默认参数
        default_params = {
            'supply': 1000.0,        # 供应量（万吨）
            'demand': 950.0,         # 需求量（万吨）
            'inventory': 200.0,      # 库存量（万吨）
            'production_growth': 0.02,  # 产量增长率
            'consumption_growth': 0.03,  # 消费增长率
            'price_elasticity': -0.5,   # 价格弹性
            'seasonal_factor': 1.0      # 季节性因子
        }
        
        # 根据商品名称调整一些参数
        if commodity_name == 'oil':
            default_params['supply'] = 10000.0
            default_params['demand'] = 9800.0
            default_params['inventory'] = 1500.0
            default_params['price_elasticity'] = -0.3
        elif commodity_name == 'gold':
            default_params['supply'] = 3.0
            default_params['demand'] = 3.1
            default_params['inventory'] = 30.0
            default_params['price_elasticity'] = -0.1
        elif commodity_name == 'copper':
            default_params['supply'] = 2000.0
            default_params['demand'] = 1950.0
            default_params['inventory'] = 300.0
            default_params['price_elasticity'] = -0.4
        
        return default_params
    
    def _get_current_commodity_price(self, commodity_name: str) -> float:
        """
        获取商品的当前价格
        
        Args:
            commodity_name: 商品名称
            
        Returns:
            当前价格
        """
        # 在实际应用中，应该从市场数据API获取最新价格
        # 这里我们使用一些默认值作为示例
        
        if commodity_name == 'oil':
            return 75.0  # 美元/桶
        elif commodity_name == 'gold':
            return 1800.0  # 美元/盎司
        elif commodity_name == 'copper':
            return 9000.0  # 美元/吨
        else:
            return 100.0  # 默认价格
    
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
                                   events: List[Dict[str, Any]], commodity_name: str) -> float:
        """
        计算因子的调整值
        
        Args:
            factor_name: 因子名称
            factor_source: 因子数据源
            events: 宏观事件列表
            commodity_name: 商品名称
            
        Returns:
            调整值
        """
        # 根据因子源获取调整值
        if factor_source == 'keyword_filter':
            # 从关键词过滤结果获取调整值
            return self._get_adjustment_from_keywords(factor_name, events, commodity_name)
        elif factor_source == 'world_bank':
            # 从世界银行数据获取调整值
            return self._get_adjustment_from_world_bank(factor_name, events, commodity_name)
        elif factor_source == 'fred':
            # 从经济指标获取调整值
            return self._get_adjustment_from_economic_indicators(factor_name, events)
        elif factor_source == 'analysis':
            # 从分析结果获取调整值
            return self._get_adjustment_from_analysis(factor_name, events)
        else:
            logger.warning(f"不支持的因子源: {factor_source}")
            return 0.0
    
    def _get_adjustment_from_keywords(self, factor_name: str, events: List[Dict[str, Any]], 
                                    commodity_name: str) -> float:
        """
        从关键词过滤结果获取调整值
        
        Args:
            factor_name: 因子名称
            events: 宏观事件列表
            commodity_name: 商品名称
            
        Returns:
            调整值
        """
        if factor_name != 'trade_policy':
            logger.warning(f"关键词过滤不支持的因子: {factor_name}")
            return 0.0
        
        # 筛选与贸易政策相关的事件
        trade_events = [e for e in events if e['event_category'] == 'trade_policy']
        
        if not trade_events:
            return 0.0
        
        # 计算贸易政策事件的影响
        total_impact = 0.0
        
        for event in trade_events:
            # 获取事件的关键词匹配情况
            keywords = self._get_event_keywords(event['id'])
            
            # 检查是否包含与该商品相关的关键词
            commodity_keywords = self._get_commodity_keywords(commodity_name)
            
            relevance = 0.0
            for keyword in keywords:
                if keyword in commodity_keywords:
                    relevance += 1.0
            
            # 如果有相关性，计算影响
            if relevance > 0:
                # 获取事件的情绪极性
                sentiment = self._get_event_sentiment(event['id'])
                
                if sentiment:
                    # 根据情绪极性和事件重要性计算影响
                    impact = sentiment['avg_polarity'] * event['importance'] / 5.0 * relevance / len(commodity_keywords)
                    total_impact += impact
        
        # 限制总影响在合理范围内
        return max(min(total_impact, 0.2), -0.2)  # 最大±20%的调整
    
    def _get_adjustment_from_world_bank(self, factor_name: str, events: List[Dict[str, Any]], 
                                      commodity_name: str) -> float:
        """
        从世界银行数据获取调整值
        
        Args:
            factor_name: 因子名称
            events: 宏观事件列表
            commodity_name: 商品名称
            
        Returns:
            调整值
        """
        if factor_name != 'global_supply':
            logger.warning(f"世界银行数据不支持的因子: {factor_name}")
            return 0.0
        
        # 获取全球贸易数据
        try:
            start_date = datetime.date.today() - datetime.timedelta(days=365)
            end_date = datetime.date.today()
            
            trade_data = self.historical_analyzer.get_historical_data('global_trade', start_date, end_date)
            
            if trade_data.empty:
                logger.warning("无法获取全球贸易数据")
                return 0.0
            
            # 计算最近的贸易变化率
            recent_values = trade_data.sort_values('date', ascending=False)['value'].head(2).values
            
            if len(recent_values) < 2:
                return 0.0
            
            change_rate = (recent_values[0] - recent_values[1]) / recent_values[1]
            
            # 根据商品类型调整影响系数
            commodity_sensitivity = self._get_commodity_trade_sensitivity(commodity_name)
            
            # 计算调整值
            adjustment = change_rate * commodity_sensitivity
            
            return adjustment
            
        except Exception as e:
            logger.error(f"计算全球供应调整值时出错: {e}")
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
        if factor_name != 'demand_forecast':
            logger.warning(f"经济指标不支持的因子: {factor_name}")
            return 0.0
        
        # 获取GDP和通胀数据
        try:
            start_date = datetime.date.today() - datetime.timedelta(days=365)
            end_date = datetime.date.today()
            
            gdp_data = self.historical_analyzer.get_historical_data('gdp', start_date, end_date)
            cpi_data = self.historical_analyzer.get_historical_data('cpi', start_date, end_date)
            
            if gdp_data.empty or cpi_data.empty:
                logger.warning("无法获取GDP或CPI数据")
                return 0.0
            
            # 计算GDP增长率
            gdp_values = gdp_data.sort_values('date')['value'].values
            gdp_growth = (gdp_values[-1] - gdp_values[0]) / gdp_values[0] if len(gdp_values) > 1 else 0
            
            # 计算通胀率
            cpi_values = cpi_data.sort_values('date')['value'].values
            inflation = (cpi_values[-1] - cpi_values[0]) / cpi_values[0] if len(cpi_values) > 1 else 0
            
            # 计算需求预测调整值
            # GDP增长正向影响需求，通胀负向影响需求
            adjustment = gdp_growth * 0.7 - inflation * 0.3
            
            return adjustment
            
        except Exception as e:
            logger.error(f"计算需求预测调整值时出错: {e}")
            return 0.0
    
    def _get_adjustment_from_analysis(self, factor_name: str, events: List[Dict[str, Any]]) -> float:
        """
        从分析结果获取调整值
        
        Args:
            factor_name: 因子名称
            events: 宏观事件列表
            
        Returns:
            调整值
        """
        if factor_name != 'sentiment':
            logger.warning(f"分析结果不支持的因子: {factor_name}")
            return 0.0
        
        # 计算事件的平均情绪极性
        total_polarity = 0.0
        total_weight = 0.0
        
        for event in events:
            # 获取事件的情绪分析结果
            sentiment = self._get_event_sentiment(event['id'])
            
            if sentiment:
                # 根据事件重要性加权
                weight = event['importance'] / 5.0
                total_polarity += sentiment['avg_polarity'] * weight
                total_weight += weight
        
        # 计算加权平均情绪极性
        if total_weight > 0:
            avg_polarity = total_polarity / total_weight
        else:
            avg_polarity = 0.0
        
        # 将情绪极性转换为调整值
        # 情绪极性范围为[-1, 1]，我们将其映射到合理的调整范围
        adjustment = avg_polarity * 0.1  # 最大±10%的调整
        
        return adjustment
    
    def _get_event_keywords(self, event_id: int) -> List[str]:
        """
        获取事件的关键词
        
        Args:
            event_id: 事件ID
            
        Returns:
            关键词列表
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
                return []
            
            article_ids = [row['article_id'] for row in article_rows]
            
            # 获取这些文章的关键词
            placeholders = ', '.join(['%s'] * len(article_ids))
            
            query = f"""
            SELECT DISTINCT keyword
            FROM keyword_matches
            WHERE article_id IN ({placeholders})
            """
            
            cursor.execute(query, article_ids)
            keyword_rows = cursor.fetchall()
            
            return [row['keyword'] for row in keyword_rows]
            
        except Error as e:
            logger.error(f"获取事件关键词时出错: {e}")
            return []
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def _get_commodity_keywords(self, commodity_name: str) -> List[str]:
        """
        获取与商品相关的关键词
        """
        mapping = {
            'oil': ['oil', 'crude', 'petroleum', 'OPEC', 'refinery', 'energy'],
            'gold': ['gold', 'bullion', 'precious metal', 'safe haven'],
            'copper': ['copper', 'metal', 'industrial metal', 'mining'],
            'wheat': ['wheat', 'grain', 'agriculture', 'food'],
            'corn': ['corn', 'maize', 'grain', 'feed']
        }
        return mapping.get(commodity_name, [commodity_name])

    def _get_commodity_trade_sensitivity(self, commodity_name: str) -> float:
        """
        获取商品对全球贸易变化的敏感度
        """
        sensitivities = {
            'oil': 1.2,
            'copper': 1.0,
            'gold': 0.4,
            'wheat': 0.7,
            'corn': 0.7
        }
        return sensitivities.get(commodity_name, 0.5)

    def _get_event_sentiment(self, event_id: int) -> Optional[Dict[str, float]]:
        """
        获取事件的情绪分析结果
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
            if not article_ids:
                return None
            # 获取这些文章的情绪分析结果
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
                return None
        except Error as e:
            logger.error(f"获取事件情绪分析结果时出错: {e}")
            return None
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def _save_commodity_adjustments(self, commodity_name: str, adjustments: Dict[str, Dict[str, Any]], events: List[Dict[str, Any]]) -> bool:
        """
        保存商品参数调整记录
        """
        if not adjustments:
            return True
        connection = self.db_connector.get_connection()
        try:
            cursor = connection.cursor()
            adjustment_date = datetime.date.today()
            for factor_name, adjustment in adjustments.items():
                max_event_id = None
                if events:
                    max_event_id = events[0].get('id')
                insert_query = """
                INSERT INTO commodity_adjustments
                (commodity_name, adjustment_date, factor_name, original_value, adjusted_value, adjustment_reason, confidence, event_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                reason = f"Adjusted based on {len(events)} macro events"
                confidence = min(0.5 + len(events) / 20, 0.95)
                cursor.execute(insert_query, (
                    commodity_name,
                    adjustment_date,
                    factor_name,
                    adjustment['original_value'],
                    adjustment['adjusted_value'],
                    reason,
                    confidence,
                    max_event_id
                ))
            connection.commit()
            logger.info(f"成功保存商品 {commodity_name} 的参数调整记录")
            return True
        except Error as e:
            logger.error(f"保存商品参数调整记录时出错: {e}")
            connection.rollback()
            return False
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def forecast(self, commodity_name: str, event_ids: List[int] = None) -> Dict[str, Any]:
        """
        结合宏观事件进行商品供需预测（示例）
        """
        adjusted_params = self.adjust_commodity_parameters(commodity_name, event_ids)
        if not adjusted_params:
            logger.warning(f"无法获取商品 {commodity_name} 的调整后参数，预测失败")
            return {}
        try:
            # 简单的供需平衡模型：价格变动幅度与供需缺口成正比
            supply = adjusted_params.get('supply', 0)
            demand = adjusted_params.get('demand', 0)
            inventory = adjusted_params.get('inventory', 0)
            price_elasticity = adjusted_params.get('price_elasticity', -0.5)
            seasonal_factor = adjusted_params.get('seasonal_factor', 1.0)

            gap = (demand - supply) / (supply + 1e-6)
            price_change = gap * price_elasticity * seasonal_factor
            result = {
                'commodity': commodity_name,
                'adjusted_params': adjusted_params,
                'predicted_price_change_pct': round(price_change * 100, 4),
                'inventory': inventory
            }
            logger.info(f"商品 {commodity_name} 供需预测结果: {json.dumps(result)}")
            return result
        except Exception as e:
            logger.error(f"商品供需预测计算出错: {e}")
            return {}

# 可选：为单元测试/演示提供main入口
if __name__ == "__main__":
    integrator = CommodityIntegrator('config.yaml')
    result = integrator.forecast('oil')
    print(result)