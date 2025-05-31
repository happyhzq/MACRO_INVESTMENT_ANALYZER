#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
主程序入口
负责启动宏观经济分析投资决策系统
"""

import os
import sys
import argparse
import logging
import datetime
import yaml
import time

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

logger = logging.getLogger(__name__)

def load_config(config_path):
    """加载配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        sys.exit(1)

def initialize_database(config):
    """初始化数据库"""
    try:
        db_connector = DatabaseConnector(config['database'])
        connection = db_connector.get_connection()
        
        # 检查连接
        if connection.is_connected():
            logger.info("数据库连接成功")
            
            # 初始化数据库结构
            schema_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database', 'schema.sql')
            if os.path.exists(schema_file):
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema_sql = f.read()
                
                cursor = connection.cursor()
                
                # 分割SQL语句并执行
                for statement in schema_sql.split(';'):
                    if statement.strip():
                        cursor.execute(statement)
                
                connection.commit()
                logger.info("数据库结构初始化完成")
            else:
                logger.warning(f"数据库结构文件不存在: {schema_file}")
        
        return db_connector
    except Exception as e:
        logger.error(f"初始化数据库失败: {e}")
        sys.exit(1)

def fetch_data(config, db_connector):
    """获取数据"""
    try:
        # 获取RSS数据
        if config['data_sources']['rss']['enabled']:
            logger.info("开始获取RSS数据...")
            rss_fetcher = RSSFetcher(config['data_sources']['rss']['sources'], db_connector)
            rss_articles = rss_fetcher.fetch_all()
            logger.info(f"获取到 {len(rss_articles)} 篇RSS文章")
        
        # 获取NewsAPI数据
        if config['data_sources']['news_api']['enabled']:
            logger.info("开始获取NewsAPI数据...")
            news_api_fetcher = NewsAPIFetcher(config['data_sources']['news_api']['api_key'], db_connector)
            news_articles = news_api_fetcher.fetch_news(
                keywords=config['data_sources']['news_api']['keywords'],
                days=config['data_sources']['news_api']['days_back']
            )
            logger.info(f"获取到 {len(news_articles)} 篇NewsAPI文章")
        
        return True
    except Exception as e:
        logger.error(f"获取数据失败: {e}")
        return False

def analyze_data(config, db_connector):
    """分析数据"""
    try:
        # 关键词过滤
        if config['analysis']['keyword_filter']['enabled']:
            logger.info("开始进行关键词过滤分析...")
            keyword_filter = KeywordFilter(config['analysis']['keyword_filter']['keywords'], db_connector)
            keyword_results = keyword_filter.analyze_recent_articles(
                days=config['analysis']['keyword_filter']['days_back']
            )
            logger.info(f"完成 {len(keyword_results)} 篇文章的关键词过滤分析")
        
        # 情绪分析
        if config['analysis']['sentiment']['enabled']:
            logger.info("开始进行情绪分析...")
            sentiment_analyzer = SentimentAnalyzer(db_connector)
            sentiment_results = sentiment_analyzer.analyze_recent_articles(
                days=config['analysis']['sentiment']['days_back']
            )
            logger.info(f"完成 {len(sentiment_results)} 篇文章的情绪分析")
        
        # 量化分析
        if config['analysis']['quantitative']['enabled']:
            logger.info("开始进行量化分析...")
            quantitative_analyzer = QuantitativeAnalyzer(db_connector)
            quant_results = quantitative_analyzer.analyze_recent_events(
                days=config['analysis']['quantitative']['days_back']
            )
            logger.info(f"完成 {len(quant_results)} 个事件的量化分析")
        
        # 历史数据分析
        if config['analysis']['historical']['enabled']:
            logger.info("开始进行历史数据分析...")
            historical_analyzer = HistoricalAnalyzer(os.path.abspath(args.config))
            correlations = historical_analyzer.analyze_all_correlations()
            historical_analyzer.save_correlation_results(correlations)
            logger.info(f"完成 {len(correlations)} 个历史数据相关性分析")
        
        return True
    except Exception as e:
        logger.error(f"分析数据失败: {e}")
        return False

def integrate_models(config, db_connector):
    """整合模型"""
    try:
        # DCF模型整合
        if config['model_integration']['dcf']['enabled']:
            logger.info("开始进行DCF模型整合...")
            dcf_integrator = DCFIntegrator(os.path.abspath(args.config))
            
            # 测试股票列表
            test_stocks = ['AAPL', 'MSFT', 'AMZN']
            
            for stock in test_stocks:
                valuation = dcf_integrator.calculate_dcf_valuation(stock)
                if valuation:
                    logger.info(f"股票 {stock} 的DCF估值: {valuation['per_share_value']:.2f}")
        
        # 商品供需预测整合
        if config['model_integration']['commodity']['enabled']:
            logger.info("开始进行商品供需预测整合...")
            commodity_integrator = CommodityIntegrator(os.path.abspath(args.config))
            
            # 测试商品列表
            test_commodities = ['oil', 'gold', 'copper']
            
            for commodity in test_commodities:
                prediction = commodity_integrator.predict_commodity_price(commodity)
                if prediction:
                    logger.info(f"商品 {commodity} 的价格预测: {prediction['predicted_price']:.2f}")
        
        return True
    except Exception as e:
        logger.error(f"整合模型失败: {e}")
        return False

def perform_attribution(config, db_connector):
    """执行投资归因分析"""
    try:
        if config['attribution']['enabled']:
            logger.info("开始进行投资归因分析...")
            attribution_model = AttributionModel(os.path.abspath(args.config))
            
            # 测试决策ID列表（在实际应用中应该从数据库获取）
            test_decision_ids = [1, 2, 3]
            
            results = []
            for decision_id in test_decision_ids:
                result = attribution_model.analyze_investment_decision(decision_id)
                if result:
                    results.append(result)
                    logger.info(f"投资决策ID {decision_id} 的回报率: {result['percent_return']:.2f}%")
            
            if results:
                # 生成报告
                report_file = attribution_model.generate_attribution_report(decision_ids=test_decision_ids)
                
                # 可视化归因分析
                chart_file = attribution_model.visualize_attribution(results)
                
                logger.info(f"投资归因分析完成，报告: {report_file}, 图表: {chart_file}")
        
        return True
    except Exception as e:
        logger.error(f"执行投资归因分析失败: {e}")
        return False

def main(args):
    """主函数"""
    # 设置日志
    setup_logger(log_level=args.log_level)
    
    logger.info("宏观经济分析投资决策系统启动")
    
    # 加载配置
    config = load_config(args.config)
    
    # 初始化数据库
    db_connector = initialize_database(config)
    
    # 执行流程
    if args.fetch_only:
        fetch_data(config, db_connector)
    elif args.analyze_only:
        analyze_data(config, db_connector)
    elif args.integrate_only:
        integrate_models(config, db_connector)
    elif args.attribution_only:
        perform_attribution(config, db_connector)
    else:
        # 完整流程
        logger.info("开始执行完整分析流程")
        
        # 获取数据
        if fetch_data(config, db_connector):
            # 分析数据
            if analyze_data(config, db_connector):
                # 整合模型
                if integrate_models(config, db_connector):
                    # 执行归因分析
                    perform_attribution(config, db_connector)
    
    logger.info("宏观经济分析投资决策系统执行完毕")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="宏观经济分析投资决策系统")
    parser.add_argument("--config", default="config/config.yaml", help="配置文件路径")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="日志级别")
    parser.add_argument("--fetch-only", action="store_true", help="仅获取数据")
    parser.add_argument("--analyze-only", action="store_true", help="仅分析数据")
    parser.add_argument("--integrate-only", action="store_true", help="仅整合模型")
    parser.add_argument("--attribution-only", action="store_true", help="仅执行归因分析")
    
    args = parser.parse_args()
    
    main(args)
