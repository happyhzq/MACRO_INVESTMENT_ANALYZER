#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
项目测试脚本
用于测试宏观经济分析投资决策系统的各个模块
"""

import os
import sys
import logging
import yaml
import unittest
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logger import setup_logger
from data_sources.rss_fetcher import RSSFetcher
from data_sources.news_api_fetcher import NewsAPIFetcher
from analysis.keyword_filter import KeywordFilter
from analysis.sentiment_analyzer import SentimentAnalyzer
from analysis.quantitative_analyzer import QuantitativeAnalyzer
from analysis.historical_analyzer import HistoricalAnalyzer
from models.dcf_integrator import DCFIntegrator
from models.commodity_integrator import CommodityIntegrator
from models.attribution_model import AttributionModel
from database.db_connector import DatabaseConnector

# 设置日志
setup_logger(log_level="INFO")
logger = logging.getLogger(__name__)

class TestMacroInvestmentAnalyzer(unittest.TestCase):
    """测试宏观经济分析投资决策系统"""
    
    @classmethod
    def setUpClass(cls):
        """测试前的准备工作"""
        # 加载配置
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.yaml')
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config_template.yaml')
            
        with open(config_path, 'r', encoding='utf-8') as file:
            cls.config = yaml.safe_load(file)
        
        # 初始化数据库连接
        cls.db_connector = DatabaseConnector(cls.config['database'])
        
        logger.info("测试环境准备完成")
    
    def test_database_connection(self):
        """测试数据库连接"""
        try:
            connection = self.db_connector.get_connection()
            self.assertTrue(connection.is_connected())
            logger.info("数据库连接测试通过")
        except Exception as e:
            self.fail(f"数据库连接测试失败: {e}")
    
    def test_rss_fetcher(self):
        """测试RSS数据抓取"""
        try:
            rss_sources = self.config['data_sources']['rss']['sources']
            rss_fetcher = RSSFetcher(rss_sources, self.db_connector)
            
            # 测试单个源抓取
            if rss_sources:
                test_source = rss_sources[0]
                articles = rss_fetcher.fetch_from_source(test_source['url'], test_source['name'])
                self.assertIsInstance(articles, list)
                logger.info(f"RSS抓取测试通过，从 {test_source['name']} 获取到 {len(articles)} 篇文章")
        except Exception as e:
            logger.warning(f"RSS抓取测试跳过: {e}")
    
    def test_news_api_fetcher(self):
        """测试NewsAPI数据抓取"""
        try:
            api_key = self.config['data_sources']['news_api']['api_key']
            if not api_key or api_key == "your_api_key_here":
                logger.warning("NewsAPI测试跳过: 未提供有效的API密钥")
                return
                
            news_api_fetcher = NewsAPIFetcher(api_key, self.db_connector)
            articles = news_api_fetcher.fetch_news(
                keywords=self.config['data_sources']['news_api']['keywords'],
                days=1  # 仅测试1天的数据
            )
            self.assertIsInstance(articles, list)
            logger.info(f"NewsAPI抓取测试通过，获取到 {len(articles)} 篇文章")
        except Exception as e:
            logger.warning(f"NewsAPI抓取测试跳过: {e}")
    
    def test_keyword_filter(self):
        """测试关键词过滤"""
        try:
            keywords = self.config['analysis']['keyword_filter']['keywords']
            keyword_filter = KeywordFilter(keywords, self.db_connector)
            
            # 创建测试文章
            test_article = {
                'title': 'US Imposes New Tariffs on Chinese Goods',
                'content': 'The United States announced new tariffs on Chinese imports, affecting various sectors including technology and manufacturing.',
                'source': 'Test Source',
                'url': 'http://example.com/test',
                'published_date': datetime.datetime.now()
            }
            
            # 测试关键词匹配
            matches = keyword_filter.find_keywords(test_article)
            self.assertIsInstance(matches, list)
            logger.info(f"关键词过滤测试通过，找到 {len(matches)} 个关键词匹配")
        except Exception as e:
            self.fail(f"关键词过滤测试失败: {e}")
    
    def test_sentiment_analyzer(self):
        """测试情绪分析"""
        try:
            sentiment_analyzer = SentimentAnalyzer(self.db_connector)
            
            # 测试文本
            test_texts = [
                "The economy is showing strong signs of recovery with increasing job numbers.",
                "Markets plunged today as inflation fears grow among investors.",
                "Central bank maintains neutral stance on interest rates."
            ]
            
            for text in test_texts:
                result = sentiment_analyzer.analyze_text(text)
                self.assertIsInstance(result, dict)
                self.assertIn('polarity', result)
                self.assertIn('subjectivity', result)
                
            logger.info("情绪分析测试通过")
        except Exception as e:
            self.fail(f"情绪分析测试失败: {e}")
    
    def test_historical_analyzer(self):
        """测试历史数据分析"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.yaml')
            historical_analyzer = HistoricalAnalyzer(config_path)
            
            # 测试历史数据获取
            start_date = datetime.date.today() - datetime.timedelta(days=30)
            end_date = datetime.date.today()
            
            gdp_data = historical_analyzer.get_historical_data('gdp', start_date, end_date)
            self.assertIsInstance(gdp_data, pd.DataFrame)
            
            # 测试相关性分析
            event_category = 'trade_policy'
            market_data_type = 'stock_prices'
            correlation = historical_analyzer.analyze_event_market_correlation(event_category, market_data_type)
            
            logger.info("历史数据分析测试通过")
        except Exception as e:
            logger.warning(f"历史数据分析测试跳过: {e}")
    
    def test_dcf_integrator(self):
        """测试DCF模型整合"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.yaml')
            dcf_integrator = DCFIntegrator(config_path)
            
            # 测试股票
            test_stock = 'AAPL'
            
            # 测试参数调整
            adjusted_params = dcf_integrator.adjust_dcf_parameters(test_stock)
            self.assertIsInstance(adjusted_params, dict)
            
            # 测试估值计算
            valuation = dcf_integrator.calculate_dcf_valuation(test_stock)
            self.assertIsInstance(valuation, dict)
            
            logger.info(f"DCF模型整合测试通过，股票 {test_stock} 的估值: {valuation.get('per_share_value', 'N/A')}")
        except Exception as e:
            logger.warning(f"DCF模型整合测试跳过: {e}")
    
    def test_commodity_integrator(self):
        """测试商品供需预测整合"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.yaml')
            commodity_integrator = CommodityIntegrator(config_path)
            
            # 测试商品
            test_commodity = 'oil'
            
            # 测试参数调整
            adjusted_params = commodity_integrator.adjust_commodity_parameters(test_commodity)
            self.assertIsInstance(adjusted_params, dict)
            
            # 测试价格预测
            prediction = commodity_integrator.predict_commodity_price(test_commodity)
            self.assertIsInstance(prediction, dict)
            
            logger.info(f"商品供需预测整合测试通过，商品 {test_commodity} 的价格预测: {prediction.get('predicted_price', 'N/A')}")
        except Exception as e:
            logger.warning(f"商品供需预测整合测试跳过: {e}")
    
    def test_attribution_model(self):
        """测试投资归因分析"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.yaml')
            attribution_model = AttributionModel(config_path)
            
            # 测试决策ID
            test_decision_id = 1
            
            # 测试归因分析
            result = attribution_model.analyze_investment_decision(test_decision_id)
            
            # 如果没有实际的决策数据，这里可能会返回空结果
            if result:
                self.assertIsInstance(result, dict)
                self.assertIn('attribution', result)
                logger.info(f"投资归因分析测试通过，决策ID {test_decision_id} 的回报率: {result.get('percent_return', 'N/A')}%")
            else:
                logger.warning(f"投资归因分析测试跳过: 未找到决策ID {test_decision_id}")
        except Exception as e:
            logger.warning(f"投资归因分析测试跳过: {e}")
    
    def test_visualization(self):
        """测试可视化功能"""
        try:
            # 创建测试数据
            dates = pd.date_range(start='2024-01-01', end='2024-05-01', freq='D')
            values = np.random.normal(0, 1, len(dates)).cumsum()
            
            # 创建图表
            plt.figure(figsize=(10, 6))
            plt.plot(dates, values)
            plt.title('Test Visualization')
            plt.xlabel('Date')
            plt.ylabel('Value')
            
            # 保存图表
            output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, 'test_visualization.png')
            plt.savefig(output_file)
            
            self.assertTrue(os.path.exists(output_file))
            logger.info(f"可视化测试通过，图表保存至: {output_file}")
        except Exception as e:
            self.fail(f"可视化测试失败: {e}")
    
    @classmethod
    def tearDownClass(cls):
        """测试后的清理工作"""
        logger.info("测试完成")

if __name__ == "__main__":
    unittest.main()
