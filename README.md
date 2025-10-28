# 中央数据厨房（Data Layer, *data* 分支）

**这是什么**  
本仓库只负责“数据侧”的工作：把原始数据（ATAS、Binance）统一清洗、对齐成**数据预制菜**（只读快照），供模型仓（V6 / V7 / 未来 V8）消费。  
**不包含**任何训练/决策/回测代码；那些在各自模型仓维护。

## 为什么要拆分
- **职责单一**：数据厨房专注采集→对齐→特征→快照发布**数据**；**模型仓**专注训练→验证→发布。  
- **口径一致**：对齐与特征派生在一个地方实现，避免多处口径漂移。  
- **可追溯**：每个快照带 manifest（行数/时间窗/MD5/列清单/生成器版本）与 QC 报告。  
- **易维护**：厨房 7×24 跑在独立机器；模型端可一台或多台并行训练。

## 概念设计图
```mermaid
flowchart TB
  %% ===== 数据源 =====
  subgraph S["数据源"]
    S1["ATAS 回放 JSONL<br/>(data/raw/atas/bar/*)"]
    S2["ATAS 实时 Tick<br/>(data/raw/atas/tick/*)"]
    S3["Binance 1m K 线<br/>(data/exchange/*)"]
  end

  %% ===== 对齐与校准 =====
  subgraph A["对齐与校准 · kitchen/align"]
    A1["标准分钟索引<br/>preprocessing/align/index.parquet"]
    A2["多源合并声明<br/>preprocessing/configs/merge.yaml"]
    A3["minute↔tick 分层校准<br/>calibration_profile.json"]
    A4["不可合并段白名单<br/>validation/configs/priority_downgrade.yaml"]
  end

  %% ===== 特征工程 =====
  subgraph FEA["特征工程 · kitchen/features"]
    F1["微观特征（订单流/AMT）<br/>kitchen/features/make_features_micro.py"]
    F2["宏观慢变量（MA200 等）<br/>features/macro_factor/*"]
  end

  %% ===== 质控与签名 =====
  subgraph Q["质控与签名 · kitchen/publish"]
    Q1["QC 报告<br/>output/results/qc_summary.md"]
    Q2["Manifest（initial/update）<br/>manifests/*.json"]
    Q3["签名：data_manifest_hash / calibration_hash<br/>output/results/manifest_hash.txt"]
  end

  %% ===== 发布与快照 =====
  subgraph P["原子发布 · kitchen/publish"]
    P1["publish_snapshot.py"]
    P2[(snapshots/DATE)]
    P3[(snapshots/LATEST)]
  end

  %% ===== 数据流连线 =====
  S1 --> A1
  S2 --> A1
  S3 --> A1
  A1 --> A2 --> A3 --> A4
  A3 --> F1
  S3 --> F2
  F1 --> Q
  F2 --> Q
  Q --> P1 --> P2
  P1 --> P3

  %% ===== 消费方 =====
  P2 --> C1[(V6 只读快照)]
  P3 --> C2[(V7 只读快照)]
```

> 预览图片可通过 mermaid-cli 生成：`npx -y @mermaid-js/mermaid-cli -i README.md -o docs/diagram.png --scale 2`
