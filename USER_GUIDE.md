# 宏观经济分析投资决策系统使用说明

## 项目概述

宏观经济分析投资决策系统是一个集成化工具，旨在通过抓取指定网站的RSS和新闻API数据，分析宏观事件对经济的影响，并将这些分析结果整合到投资决策框架中。系统主要解决以下问题：

1. 自动抓取和分析金融新闻和宏观事件
2. 评估宏观事件对GDP、利率、汇率等经济指标的影响
3. 将宏观分析结果整合到DCF估值模型中
4. 将宏观分析结果整合到商品供需预测中
5. 提供投资结果评价与归因分析

## 系统架构

系统由以下主要模块组成：

1. **数据源模块**：负责从RSS和NewsAPI获取新闻数据
2. **分析模块**：包括关键词过滤、情绪分析、量化分析和历史数据分析
3. **模型整合模块**：将分析结果整合到DCF模型和商品供需预测中
4. **归因分析模块**：评估投资结果并进行归因分析
5. **数据库模块**：存储和管理所有数据
6. **工具模块**：提供日志、配置等通用功能

## 安装指南

### 系统要求

- Python 3.8+
- MySQL 5.7+
- 足够的磁盘空间用于存储新闻和分析数据

### 安装步骤

1. 克隆或下载项目代码到本地

2. 创建并激活虚拟环境（推荐）
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate  # Windows
   ```

3. 安装依赖
   ```bash
   pip install -r requirements.txt
   ```

4. 配置数据库
   - 创建MySQL数据库
   - 复制`config/config_template.yaml`为`config/config.yaml`
   - 编辑`config.yaml`，填入数据库连接信息

5. 初始化数据库结构
   ```bash
   python main.py --init-db
   ```

## 配置说明

系统通过`config/config.yaml`文件进行配置，主要配置项包括：

### 数据库配置
```yaml
database:
  host: localhost
  port: 3306
  user: root
  password: your_password
  database: macro_investment
```

### 数据源配置
```yaml
data_sources:
  rss:
    enabled: true
    sources:
      - name: Bloomberg
        url: https://www.bloomberg.com/feed/podcast/etf-report
      - name: Reuters
        url: https://www.reutersagency.com/feed/
  news_api:
    enabled: true
    api_key: your_api_key_here
    keywords:
      - tariff
      - trade policy
      - interest rate
    days_back: 7
```

### 分析配置
```yaml
analysis:
  keyword_filter:
    enabled: true
    keywords:
      - tariff:
          weight: 0.8
          category: trade_policy
      - interest rate:
          weight: 0.9
          category: monetary_policy
    days_back: 30
  sentiment:
    enabled: true
    days_back: 30
  quantitative:
    enabled: true
    days_back: 90
  historical:
    enabled: true
    local_cache_days: 365
```

### 模型整合配置
```yaml
model_integration:
  dcf:
    enabled: true
    impact_factors:
      - name: discount_rate
        weight: 0.5
        source: fred
      - name: growth_rate
        weight: 0.3
        source: analysis
  commodity:
    enabled: true
    impact_factors:
      - name: supply
        weight: 0.6
        source: world_bank
      - name: demand_forecast
        weight: 0.4
        source: fred
```

### 归因分析配置
```yaml
attribution:
  enabled: true
  lookback_period: 90
  factors:
    - macro_events
    - sector_performance
    - company_specific
    - market_sentiment
  report_format: html
```

## 使用指南

### 基本用法

1. 运行完整分析流程
   ```bash
   python main.py
   ```

2. 仅获取数据
   ```bash
   python main.py --fetch-only
   ```

3. 仅分析数据
   ```bash
   python main.py --analyze-only
   ```

4. 仅整合模型
   ```bash
   python main.py --integrate-only
   ```

5. 仅执行归因分析
   ```bash
   python main.py --attribution-only
   ```

6. 设置日志级别
   ```bash
   python main.py --log-level DEBUG
   ```

### 运行测试

运行系统测试以验证各模块功能：
```bash
python test_system.py
```

## 功能详解

### 数据抓取

系统支持从RSS源和NewsAPI获取新闻数据：

- **RSS抓取**：通过`RSSFetcher`类实现，支持配置多个RSS源
- **NewsAPI抓取**：通过`NewsAPIFetcher`类实现，支持按关键词和时间范围获取新闻

### 数据分析

系统提供多种分析方法：

- **关键词过滤**：识别新闻中的关键词，并按权重和类别进行分类
- **情绪分析**：使用TextBlob分析新闻的情绪极性和主观性
- **量化分析**：对新闻和事件进行量化评分
- **历史数据分析**：分析历史宏观事件与市场数据的相关性

### 模型整合

系统将分析结果整合到投资模型中：

- **DCF模型整合**：调整折现率、增长率等参数，生成考虑宏观因素的股票估值
- **商品供需预测整合**：调整供应量、需求量等参数，预测考虑宏观因素的商品价格

### 归因分析

系统提供投资结果的归因分析：

- **投资决策分析**：分析单个投资决策的回报和表现
- **归因分解**：将投资回报分解为宏观事件、行业表现、公司/商品特定因素等
- **报告生成**：生成HTML、PDF或CSV格式的归因分析报告
- **可视化**：生成归因分析图表

## 项目结构

```
macro_investment_analyzer/
├── config/                  # 配置文件
│   ├── config_template.yaml # 配置模板
│   └── config.yaml          # 实际配置（需自行创建）
├── data_sources/            # 数据源模块
│   ├── rss_fetcher.py       # RSS数据抓取
│   └── news_api_fetcher.py  # NewsAPI数据抓取
├── analysis/                # 分析模块
│   ├── keyword_filter.py    # 关键词过滤
│   ├── sentiment_analyzer.py # 情绪分析
│   ├── quantitative_analyzer.py # 量化分析
│   └── historical_analyzer.py # 历史数据分析
├── models/                  # 模型整合模块
│   ├── dcf_integrator.py    # DCF模型整合
│   ├── commodity_integrator.py # 商品供需预测整合
│   └── attribution_model.py # 归因分析模型
├── database/                # 数据库模块
│   ├── db_connector.py      # 数据库连接器
│   └── schema.sql           # 数据库结构
├── utils/                   # 工具模块
│   └── logger.py            # 日志工具
├── reports/                 # 报告输出目录
├── main.py                  # 主程序入口
├── test_system.py           # 系统测试
├── requirements.txt         # 依赖列表
└── README.md                # 项目说明
```

## 扩展与定制

### 添加新的数据源

1. 在`data_sources`目录下创建新的数据源类
2. 实现`fetch_data`方法
3. 在`config.yaml`中添加相应配置
4. 在`main.py`中集成新数据源

### 添加新的分析方法

1. 在`analysis`目录下创建新的分析类
2. 实现`analyze_data`方法
3. 在`config.yaml`中添加相应配置
4. 在`main.py`中集成新分析方法

### 自定义模型整合

1. 在`models`目录下创建新的模型整合类
2. 实现参数调整和预测方法
3. 在`config.yaml`中添加相应配置
4. 在`main.py`中集成新模型

## 常见问题

1. **数据库连接失败**
   - 检查数据库服务是否运行
   - 验证配置文件中的连接信息是否正确
   - 确认用户具有足够的数据库权限

2. **API请求失败**
   - 检查API密钥是否有效
   - 验证网络连接是否正常
   - 查看API调用限制是否已达上限

3. **分析结果不准确**
   - 增加训练数据量
   - 调整关键词权重和类别
   - 优化情绪分析算法参数

4. **系统运行缓慢**
   - 减少数据获取的时间范围
   - 优化数据库查询
   - 增加服务器资源

## 技术支持

如有问题或需要技术支持，请联系项目维护者。

---

© 2025 宏观经济分析投资决策系统
