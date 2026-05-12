# E-commerce Customer Behavior Analysis — Real-Time Processing Module

基于 Spark Structured Streaming 的电商客户行为实时处理与流式分析模块。

## 模块概述

本模块构建了一条从模拟交易事件生成 → 流式数据接入 → 微批次处理 → 多维聚合与风险识别 → 可视化仪表盘输出的完整实时数据链路，使系统从"批处理"扩展为"批处理 + 实时处理"双链路架构。

## 目录结构

```
real-time-processing-module/
├── src/                          # 核心脚本
│   ├── realtime_producer.py      # 事件生成器（种子数据受控变异）
│   ├── realtime_processor.py     # Spark Structured Streaming 流处理引擎
│   ├── realtime_reporting.py     # 报表生成器（CSV + HTML + MD）
│   └── render_realtime_dashboard.py  # 仪表盘独立渲染入口
├── kafka-reference/              # Kafka 模式原始参考实现
│   ├── kafka_producer_ecommerce.py
│   └── kafka_consumer_ecommerce_to_pg.py
├── data/                         # 种子数据
│   └── E-commerceCustomerBehavior-Sheet1.csv
├── docker/                       # Docker 部署配置
│   ├── docker-compose.yml
│   └── Dockerfile
├── figures/                      # 架构图与运行截图
│   ├── pic1.png ~ pic4.png       # 流程图（架构/事件生成/风险识别/微批次处理）
│   ├── dashboard_full.png        # HTML 仪表盘全貌
│   ├── dashboard_results1~4.png  # 仪表盘各区域展示
│   ├── realtime_output_dir.png   # 输出目录结构
│   └── terminal_processing.png   # 终端运行过程
└── report/                       # 报告文档
    ├── main.tex                  # LaTeX 完整报告
    └── 成员3-程觉晓-实时处理与流式分析.md
```

## 运行方式

### 文件流模式（默认，教学演示）

```bash
# 1. 启动 Docker 环境
docker-compose -f docker/docker-compose.yml up -d

# 2. 进入 spark-master 容器
docker exec -it spark-master bash

# 3. 运行事件生成器
python /opt/spark-apps/realtime_producer.py \
    --mode files --iterations 5 --batch-size 20 \
    --sleep-seconds 5 --reset-output --seed 42

# 4. 启动流处理引擎（另开终端）
spark-submit /opt/spark-apps/realtime_processor.py \
    --source-type files --trigger-seconds 5 \
    --max-batches 5 --idle-timeout-seconds 12
```

### Kafka 模式（生产级，按需启用）

```bash
# 启动时指定 kafka profile
docker-compose -f docker/docker-compose.yml --profile kafka up -d

# Producer
python /opt/spark-apps/realtime_producer.py \
    --mode kafka --bootstrap-servers kafka-broker:29092 \
    --topic ecommerce-transactions

# Processor
spark-submit /opt/spark-apps/realtime_processor.py \
    --source-type kafka --bootstrap-servers kafka-broker:29092 \
    --topic ecommerce-transactions
```

## 核心技术点

- **种子数据受控变异**：以原始客户记录为基础，对消费金额、评分、购买间隔等字段施加合理扰动，兼顾统计真实性与业务逻辑自洽性
- **增强型风险客户识别**：在传统单条件规则基础上引入评分阈值作为替代判定条件（`days > 30 ∧ (satisfaction = Unsatisfied ∨ rating < 3.8)`），提升检测灵敏度
- **统一双模式架构**：文件流 / Kafka 通过统一 Schema 共用同一套处理逻辑
- **快照与历史分离写入**：快照型 CSV 覆盖写入供仪表盘读取，历史型 CSV 追加写入保留完整轨迹
- **无外部依赖 HTML 仪表盘**：纯内联 CSS + 原生 HTML，浏览器直接打开即可查看
- **Checkpoint 容错 + 安全路径校验**：支持流任务重启恢复，防止误删非目标目录

## 输出文件体系

| 文件 | 写入策略 | 用途 |
|------|----------|------|
| `kpi_history.csv` | 追加 | 批次级 KPI 时间序列（11 个指标） |
| `latest_batch_metrics.csv` | 覆盖 | 最新批次 KPI 快照 |
| `latest_events.csv` | 覆盖 | 最新批次事件明细 |
| `stream_events.csv` | 追加 | 全量事件历史 |
| `city_summary_latest.csv` | 覆盖 | 城市维度聚合 |
| `membership_summary_latest.csv` | 覆盖 | 会员维度聚合 |
| `at_risk_customers_latest.csv` | 覆盖 | 风险客户明细 |
| `events/batch_XXX.csv` | 按批写入 | 单批次事件归档 |
| `dashboard.html` | 覆盖 | HTML 可视化仪表盘 |
| `summary.md` | 覆盖 | Markdown 文本摘要 |
