#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库初始化脚本
用于创建和初始化宏观分析模块所需的数据库和表结构
"""

import os
import sys
import logging
import yaml
import mysql.connector
from mysql.connector import Error

# 添加项目根目录到路径，以便导入其他模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.logger import setup_logger

logger = logging.getLogger(__name__)

def load_config(config_path):
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

def create_database(db_config):
    """
    创建数据库
    
    Args:
        db_config: 数据库配置
        
    Returns:
        是否成功创建
    """
    try:
        # 连接MySQL服务器（不指定数据库）
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config.get('port', 3306)
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # 创建数据库
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']}")
            logger.info(f"数据库 {db_config['database']} 创建成功或已存在")
            
            return True
            
    except Error as e:
        logger.error(f"创建数据库时出错: {e}")
        return False
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def initialize_tables(db_config):
    """
    初始化数据库表结构
    
    Args:
        db_config: 数据库配置
        
    Returns:
        是否成功初始化
    """
    try:
        # 连接到指定数据库
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            port=db_config.get('port', 3306)
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # 读取SQL脚本
            schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database', 'schema.sql')
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # 执行SQL脚本（按语句分割）
            for statement in sql_script.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            
            connection.commit()
            logger.info("数据库表结构初始化成功")
            
            return True
            
    except Error as e:
        logger.error(f"初始化数据库表结构时出错: {e}")
        return False
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def insert_test_data(db_config):
    """
    插入测试数据
    
    Args:
        db_config: 数据库配置
        
    Returns:
        是否成功插入
    """
    try:
        # 连接到指定数据库
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            port=db_config.get('port', 3306)
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # 插入测试数据
            
            # 1. 插入新闻文章
            cursor.execute("""
            INSERT INTO news_articles 
            (title, content, source, url, published_date, category)
            VALUES 
            ('美国宣布新一轮关税政策', '美国政府今日宣布对中国进口商品加征25%关税...', 'Reuters', 'https://example.com/news1', 
             CURDATE() - INTERVAL 10 DAY, 'trade'),
            ('欧盟央行维持利率不变', '欧洲央行今日宣布维持当前利率水平不变...', 'Bloomberg', 'https://example.com/news2', 
             CURDATE() - INTERVAL 8 DAY, 'monetary'),
            ('OPEC+同意减产', 'OPEC+成员国同意每日减产100万桶...', 'Financial Times', 'https://example.com/news3', 
             CURDATE() - INTERVAL 5 DAY, 'commodity')
            """)
            
            # 获取插入的文章ID
            cursor.execute("SELECT id FROM news_articles ORDER BY id DESC LIMIT 3")
            article_ids = [row[0] for row in cursor.fetchall()]
            
            # 2. 插入关键词匹配
            if article_ids:
                cursor.execute(f"""
                INSERT INTO keyword_matches 
                (article_id, keyword, keyword_category, match_count)
                VALUES 
                ({article_ids[0]}, '关税', 'trade_policy', 3),
                ({article_ids[0]}, '贸易战', 'trade_policy', 2),
                ({article_ids[1]}, '利率', 'monetary_policy', 5),
                ({article_ids[1]}, '央行', 'monetary_policy', 4),
                ({article_ids[2]}, '石油', 'commodity', 3),
                ({article_ids[2]}, '减产', 'commodity', 2)
                """)
            
            # 3. 插入情绪分析
            if article_ids:
                cursor.execute(f"""
                INSERT INTO sentiment_analysis 
                (article_id, polarity, subjectivity)
                VALUES 
                ({article_ids[0]}, -0.2, 0.6),
                ({article_ids[1]}, 0.1, 0.4),
                ({article_ids[2]}, -0.1, 0.5)
                """)
            
            # 4. 插入宏观事件
            cursor.execute("""
            INSERT INTO macro_events 
            (event_name, event_category, start_date, end_date, description, importance)
            VALUES 
            ('美国加征关税', 'trade_policy', CURDATE() - INTERVAL 10 DAY, NULL, '美国对中国商品加征25%关税', 4),
            ('欧盟维持利率', 'monetary_policy', CURDATE() - INTERVAL 8 DAY, NULL, '欧洲央行维持当前利率水平', 3),
            ('OPEC+减产', 'commodity', CURDATE() - INTERVAL 5 DAY, NULL, 'OPEC+成员国减产石油', 4)
            """)
            
            # 获取插入的事件ID
            cursor.execute("SELECT id FROM macro_events ORDER BY id DESC LIMIT 3")
            event_ids = [row[0] for row in cursor.fetchall()]
            
            # 5. 插入事件影响
            if event_ids:
                cursor.execute(f"""
                INSERT INTO event_impacts 
                (event_id, impact_target, impact_type, impact_value, confidence, time_horizon)
                VALUES 
                ({event_ids[0]}, 'gdp', 'direct', -0.02, 0.7, 'medium_term'),
                ({event_ids[0]}, 'stock_prices', 'direct', -0.03, 0.8, 'short_term'),
                ({event_ids[1]}, 'interest_rate', 'direct', 0.0, 0.9, 'short_term'),
                ({event_ids[1]}, 'stock_prices', 'indirect', 0.01, 0.6, 'short_term'),
                ({event_ids[2]}, 'commodity:oil', 'direct', 0.05, 0.8, 'short_term'),
                ({event_ids[2]}, 'inflation', 'indirect', 0.01, 0.6, 'medium_term')
                """)
            
            # 6. 插入经济指标数据
            cursor.execute("""
            INSERT INTO economic_indicators 
            (indicator_name, indicator_category, value, unit, country, date, source)
            VALUES 
            ('gdp', 'gdp', 22000, 'Billions of Dollars', 'US', CURDATE() - INTERVAL 90 DAY, 'fred'),
            ('gdp', 'gdp', 22100, 'Billions of Dollars', 'US', CURDATE() - INTERVAL 60 DAY, 'fred'),
            ('gdp', 'gdp', 22200, 'Billions of Dollars', 'US', CURDATE() - INTERVAL 30 DAY, 'fred'),
            ('interest_rates', 'interest_rates', 4.5, 'Percent', 'US', CURDATE() - INTERVAL 30 DAY, 'fred'),
            ('interest_rates', 'interest_rates', 4.5, 'Percent', 'US', CURDATE() - INTERVAL 15 DAY, 'fred'),
            ('interest_rates', 'interest_rates', 4.5, 'Percent', 'US', CURDATE(), 'fred'),
            ('stock_prices:AAPL', 'stock_prices', 170, 'USD', 'US', CURDATE() - INTERVAL 10 DAY, 'yahoo_finance'),
            ('stock_prices:AAPL', 'stock_prices', 175, 'USD', 'US', CURDATE() - INTERVAL 5 DAY, 'yahoo_finance'),
            ('stock_prices:AAPL', 'stock_prices', 180, 'USD', 'US', CURDATE(), 'yahoo_finance'),
            ('commodity:oil', 'commodity_prices', 70, 'USD per Barrel', 'global', CURDATE() - INTERVAL 10 DAY, 'world_bank'),
            ('commodity:oil', 'commodity_prices', 75, 'USD per Barrel', 'global', CURDATE() - INTERVAL 5 DAY, 'world_bank'),
            ('commodity:oil', 'commodity_prices', 80, 'USD per Barrel', 'global', CURDATE(), 'world_bank')
            """)
            
            # 7. 插入投资决策
            cursor.execute("""
            INSERT INTO investment_decisions 
            (asset_type, asset_symbol, decision_type, price, quantity, decision_date, decision_reason, macro_adjustment)
            VALUES 
            ('stock', 'AAPL', 'buy', 170, 100, CURDATE() - INTERVAL 10 DAY, 'DCF估值显示低估', -0.05),
            ('commodity', 'oil', 'sell', 70, 10, CURDATE() - INTERVAL 10 DAY, '供需分析显示供应过剩', 0.1)
            """)
            
            connection.commit()
            logger.info("测试数据插入成功")
            
            return True
            
    except Error as e:
        logger.error(f"插入测试数据时出错: {e}")
        connection.rollback()
        return False
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def main():
    """主函数"""
    # 设置日志
    setup_logger()
    
    # 获取配置文件路径
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.yaml')
    
    if not os.path.exists(config_path):
        config_template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config_template.yaml')
        if os.path.exists(config_template_path):
            logger.info(f"配置文件不存在，正在从模板创建: {config_path}")
            import shutil
            shutil.copy(config_template_path, config_path)
        else:
            logger.error(f"配置文件和模板都不存在: {config_path}")
            sys.exit(1)
    
    try:
        # 加载配置
        config = load_config(config_path)
        
        # 打印配置信息以便调试
        logger.info(f"加载的配置信息: {config.keys()}")
        if 'database' in config:
            logger.info(f"数据库配置: {config['database']}")
        else:
            logger.error("配置中缺少database字段")
            sys.exit(1)
            
        db_config = config['database']
        
        # 创建数据库
        if not create_database(db_config):
            logger.error("创建数据库失败，退出")
            sys.exit(1)
        
        # 初始化表结构
        if not initialize_tables(db_config):
            logger.error("初始化数据库表结构失败，退出")
            sys.exit(1)
        
        # 插入测试数据
        if not insert_test_data(db_config):
            logger.warning("插入测试数据失败，但数据库结构已创建")
        
        logger.info(f"数据库 {db_config['database']} 初始化完成")
        
    except Exception as e:
        logger.error(f"数据库初始化过程中出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
