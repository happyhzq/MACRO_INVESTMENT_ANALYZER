-- 宏观分析模块数据库设计

-- 创建数据库
CREATE DATABASE IF NOT EXISTS macro_investment;
USE macro_investment;

-- 新闻文章表
CREATE TABLE IF NOT EXISTS news_articles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    source VARCHAR(100) NOT NULL,
    url VARCHAR(512) NOT NULL UNIQUE,
    published_date DATETIME NOT NULL,
    fetched_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    language VARCHAR(10) DEFAULT 'zh',
    category VARCHAR(50),
    author VARCHAR(100),
    INDEX idx_published_date (published_date),
    INDEX idx_source (source),
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 关键词匹配表
CREATE TABLE IF NOT EXISTS keyword_matches (
    id INT AUTO_INCREMENT PRIMARY KEY,
    article_id INT NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    keyword_category VARCHAR(50) NOT NULL,
    match_count INT NOT NULL DEFAULT 1,
    context TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (article_id) REFERENCES news_articles(id) ON DELETE CASCADE,
    INDEX idx_keyword (keyword),
    INDEX idx_keyword_category (keyword_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 情绪分析结果表
CREATE TABLE IF NOT EXISTS sentiment_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    article_id INT NOT NULL,
    polarity FLOAT NOT NULL, -- 情感极性 (-1.0 到 1.0)
    subjectivity FLOAT NOT NULL, -- 主观性 (0.0 到 1.0)
    confidence FLOAT NOT NULL, -- 置信度
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (article_id) REFERENCES news_articles(id) ON DELETE CASCADE,
    INDEX idx_polarity (polarity),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 宏观事件表
CREATE TABLE IF NOT EXISTS macro_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_name VARCHAR(255) NOT NULL,
    event_category VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    description TEXT,
    importance INT NOT NULL DEFAULT 3, -- 1-5 重要性评分
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_event_category (event_category),
    INDEX idx_start_date (start_date),
    INDEX idx_importance (importance)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 事件与文章关联表
CREATE TABLE IF NOT EXISTS event_articles (
    event_id INT NOT NULL,
    article_id INT NOT NULL,
    relevance_score FLOAT NOT NULL DEFAULT 1.0, -- 相关性评分
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, article_id),
    FOREIGN KEY (event_id) REFERENCES macro_events(id) ON DELETE CASCADE,
    FOREIGN KEY (article_id) REFERENCES news_articles(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 经济指标数据表
CREATE TABLE IF NOT EXISTS economic_indicators (
    id INT AUTO_INCREMENT PRIMARY KEY,
    indicator_name VARCHAR(100) NOT NULL,
    indicator_category VARCHAR(50) NOT NULL,
    value FLOAT NOT NULL,
    unit VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL DEFAULT 'global',
    date DATE NOT NULL,
    source VARCHAR(100) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_indicator (indicator_name, country, date),
    INDEX idx_indicator_name (indicator_name),
    INDEX idx_country (country),
    INDEX idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 事件影响分析表
CREATE TABLE IF NOT EXISTS event_impacts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_id INT NOT NULL,
    impact_target VARCHAR(100) NOT NULL, -- 如 'gdp', 'interest_rate', 'commodity:oil' 等
    impact_type ENUM('direct', 'indirect') NOT NULL DEFAULT 'direct',
    impact_value FLOAT NOT NULL, -- 正负值表示正面或负面影响
    confidence FLOAT NOT NULL, -- 置信度
    time_horizon VARCHAR(20) NOT NULL, -- 'short_term', 'medium_term', 'long_term'
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES macro_events(id) ON DELETE CASCADE,
    INDEX idx_impact_target (impact_target),
    INDEX idx_time_horizon (time_horizon)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- DCF模型输入调整表
CREATE TABLE IF NOT EXISTS dcf_adjustments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_symbol VARCHAR(20) NOT NULL,
    adjustment_date DATE NOT NULL,
    factor_name VARCHAR(50) NOT NULL, -- 如 'discount_rate', 'growth_rate' 等
    original_value FLOAT NOT NULL,
    adjusted_value FLOAT NOT NULL,
    adjustment_reason TEXT,
    confidence FLOAT NOT NULL,
    event_id INT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES macro_events(id) ON DELETE SET NULL,
    INDEX idx_stock_symbol (stock_symbol),
    INDEX idx_adjustment_date (adjustment_date),
    INDEX idx_factor_name (factor_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 商品供需调整表
CREATE TABLE IF NOT EXISTS commodity_adjustments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    commodity_name VARCHAR(50) NOT NULL,
    adjustment_date DATE NOT NULL,
    factor_name VARCHAR(50) NOT NULL, -- 如 'supply', 'demand', 'inventory' 等
    original_value FLOAT NOT NULL,
    adjusted_value FLOAT NOT NULL,
    adjustment_reason TEXT,
    confidence FLOAT NOT NULL,
    event_id INT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES macro_events(id) ON DELETE SET NULL,
    INDEX idx_commodity_name (commodity_name),
    INDEX idx_adjustment_date (adjustment_date),
    INDEX idx_factor_name (factor_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 投资决策记录表
CREATE TABLE IF NOT EXISTS investment_decisions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    asset_type ENUM('stock', 'commodity', 'other') NOT NULL,
    asset_symbol VARCHAR(20) NOT NULL,
    decision_type ENUM('buy', 'sell', 'hold') NOT NULL,
    decision_date DATETIME NOT NULL,
    price FLOAT NOT NULL,
    quantity FLOAT NOT NULL,
    dcf_valuation FLOAT,
    macro_adjustment FLOAT, -- 宏观因素调整值
    confidence FLOAT NOT NULL,
    reasoning TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_asset_symbol (asset_symbol),
    INDEX idx_decision_date (decision_date),
    INDEX idx_decision_type (decision_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 投资结果归因表
CREATE TABLE IF NOT EXISTS investment_attribution (
    id INT AUTO_INCREMENT PRIMARY KEY,
    decision_id INT NOT NULL,
    evaluation_date DATETIME NOT NULL,
    actual_return FLOAT NOT NULL,
    expected_return FLOAT NOT NULL,
    attribution_json JSON NOT NULL, -- 包含各因素贡献的JSON
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (decision_id) REFERENCES investment_decisions(id) ON DELETE CASCADE,
    INDEX idx_evaluation_date (evaluation_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 数据源状态表
CREATE TABLE IF NOT EXISTS data_source_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    last_update DATETIME NOT NULL,
    status ENUM('active', 'error', 'disabled') NOT NULL DEFAULT 'active',
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source (source_name, source_type),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
