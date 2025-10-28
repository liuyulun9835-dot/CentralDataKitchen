# 模型预制菜说明书（合并版）

> 本文合并 OrderFlow V6 / V7 两份项目说明书，保留原始章节结构，便于统一查看模型侧对数据厨房的需求。原始单独说明书已归档于 docs/_archive/ 目录。

---

## OrderFlow V6 项目说明书（修订 2.0）

> 版本：2.0  
> 日期：2025-10-18  
> 适用范围：OrderFlow V6 分层架构（governance / data / features / model / decision / execution / validation / output）  
> 本次修订目的：落实《策略诊断与改进报告》的建议，在**不改变目录框架**的前提下，修正模型与决策的核心假设，补齐治理契约，强化追溯与发布门控。

---

## 目录
1. [修订总览与变更图谱](#修订总览与变更图谱)  
2. [项目目标与范围](#项目目标与范围)  
3. [架构总览与目录约定](#架构总览与目录约定)  
4. [治理/控制/契约层（governance/*）](#治理控制契约层governance)  
5. [数据与特征层（data/*, features/*）](#数据与特征层data-features)  
6. [模型层（model/*）](#模型层model)  
7. [决策层（decision/*）](#决策层decision)  
8. [执行层（execution/*）](#执行层execution)  
9. [验证与发布（validation/*, output/*）](#验证与发布validation-output)  
10. [迁移、兼容与风险](#迁移兼容与风险)  
11. [附录 A：Schema/RULES 补全示例](#附录-a-schemarules-补全示例)  
12. [附录 B：目录映射与制品产物](#附录-b-目录映射与制品产物)  
13. [附录 C：卡片映射（与 todo 对应）](#附录-c卡片映射与-todo-对应)

---

## 修订总览与变更图谱

**核心修订（带标注处以「【修订 2.0】」呈现）：**
- **状态模型从“三态（平衡/趋势/过渡）改为两态（balance/trend）**；“过渡”不再作为稳态，而由**时变转移概率（TVTP）**显式表达。  
  **原因：**简化可辨识性、降低稀疏态估计偏差、提升样本外稳定性。
- **引入宏观慢变量作为 TVTP 的驱动因子（可选）**，如 `price/MA200`、长周期波动或风险因子。  
  **原因：**为转移概率提供稳定先验，减少短窗过拟合。
- **新增“状态转移方向性判断器（Directional Classifier）”**，在 `P(Si→Sj)` 过阈值时融合订单流因子（例：MFI、CVD、InformedFlow 代理）给出**方向 + 置信度**。  
  **原因：**把“进入趋势”与“做多/做空”分离建模，提升决策可解释与可控性。
- **治理契约补齐**：在 `SCHEMA_model.json`、`SCHEMA_decision.json`、`RULES_library.yaml` 新增字段与触发语法 `transition(Si->Sj, prob>τ)`；在 `CONTROL_switch_policy.yaml` 与发布清单绑定。
- **验证与发布强化**：Validator 输出新增**方向性分层统计**与**含/不含宏观因子对比**；四键签名不变（`schema_version/build_id/data_manifest_hash/calibration_hash`）。

---

## 项目目标与范围

- **目标**：以 **A/B 双源数据**（轨 A：长窗 OHLCV 基线；轨 B：ATAS/Tick 近窗增强）驱动的**2-State TVTP-HMM** + **方向性判断器**，在风险可控与可追溯的框架内，生成稳定的市场状态与方向建议，供执行层消化。
- **不做**：本版本不引入执行层的新型交易所适配与账户路由；不修改四键签名与现有发布准入链路。

---

## 架构总览与目录约定

```
/                                # 根目录：治理 / 控制 / 契约 层
├── governance/
│   ├── CONTROL_naming.md
│   ├── CONTROL_costs.yaml
│   ├── CONTROL_switch_policy.yaml          # AB 热切换门控（与发布绑定）
│   ├── RULES_library.yaml                  # 规则库（新增 transition 触发）【修订 2.0】
│   ├── SCHEMA_data.json
│   ├── SCHEMA_features.json
│   ├── SCHEMA_model.json                   # 模型制品 schema（两态 + 宏观字段）【修订 2.0】
│   ├── SCHEMA_decision.json                # 决策制品 schema（触发/置信字段）【修订 2.0】
│   └── SCHEMA_execution.json
├── data/
│   ├── ingestion/*                         # 轨 A/B 数据引入
│   ├── preprocessing/*                     # 清洗、对齐、窗口
│   └── manifests/*                         # 水位与哈希
├── features/
│   ├── microflow/*                         # MFI/CVD/imbalance 等
│   └── macro_factor/*                      # 宏观慢变量构建【修订 2.0】
├── model/
│   └── hmm_tvtp_hsmm/
│       ├── train_tvtp.py                   # 2-State TVTP 训练【修订 2.0】
│       ├── state_inference.py              # 状态/转移推断【修订 2.0】
│       └── artifacts/*                     # 模型制品（含宏观字段）【修订 2.0】
├── decision/
│   ├── directional_classifier.py           # 方向判断器（Si→Sj 触发）【修订 2.0】
│   ├── rules/transition_examples.yaml      # 规则示例：transition(...)【修订 2.0】
│   └── engine/*
├── execution/
│   ├── risk/*
│   └── switch/*                            # AB 切换与回滚
├── validation/
│   ├── configs/validator_v2.yaml           # 输出方向性分层与宏观对比【修订 2.0】
│   ├── src/univariate.py
│   ├── src/multivariate.py
│   └── reports/*
└── output/
    ├── publish_docs/{ARCHITECTURE.md,VALIDATION.md,CHANGELOG.md}
    ├── qa/*
    ├── results/*
    └── report/*
```

---

## 治理/控制/契约层（governance/*）

### 4.1 RULES 库扩展【修订 2.0】
- 新增触发语法：`transition(Si->Sj, prob>τ[, min_dur=k])`。
- 目的：把“进入某状态”与“方向性”解耦，允许规则引擎独立评估。
- 影响：规则评测与回放需读取 `decision.meta.trigger` 与 `decision.meta.classifier_confidence`。

### 4.2 SCHEMA_model.json 扩展【修订 2.0】
新增/调整字段：
```json
{
  "states": ["balance", "trend"],
  "tvtp": {
    "enabled": true,
    "drivers": ["microflow_MFI", "microflow_CVD", "macro_factor_MA200"],
    "link": "logit"
  },
  "macro_factor_used": true,
  "signatures": {
    "schema_version": "v2.0",
    "build_id": "...",
    "data_manifest_hash": "...",
    "calibration_hash": "..."
  }
}
```
**原因**：固定两态定义；显式记录 TVTP 驱动因子与宏观因子使用情况，便于追溯。

### 4.3 SCHEMA_decision.json 扩展【修订 2.0】
新增/调整字段：
```json
{
  "trigger": {
    "type": "transition",
    "from": "balance",
    "to": "trend",
    "prob": 0.86,
    "threshold": 0.80
  },
  "directional_classifier": {
    "label": "bullish",
    "confidence": 0.91,
    "inputs": ["MFI", "CVD", "informed_flow_proxy"]
  },
  "macro_factor_used": "MA200"
}
```
**原因**：把触发依据与方向置信写入契约，支撑回放与审计。

### 4.4 CONTROL_switch_policy.yaml 与发布绑定（说明强化）【修订 2.0】
- 在发布流水（`output/report/release.yml`）中加入对 `switch_policy.yaml` 的**必检**：阈值签名一致、近 7/30 天 `switch_audit` 日志无异常。
- **原因**：将 AB 热切换治理化，降低源数据漂移风险。

---

## 数据与特征层（data/*, features/*）

### 5.1 A/B 双源数据（不改框架，补全说明）
- **轨 A（Baseline）**：Binance OHLCV 长历史（2017-至今），用于宏观慢变量与稳态回归基线。
- **轨 B（Enhance）**：ATAS/Tick 近窗订单流；通过对齐/校准融入 TVTP 的微观驱动端。
- **准入与回滚**：以 PSI/KS/ECE 等稳定性门控，结果写入 `output/qa/*`，并触发 `CONTROL_switch_policy.yaml`。

### 5.2 宏观慢变量构建（features/macro_factor/*）【修订 2.0】
- **新增产物**：`macro_factor.parquet` + `configs/macro_factor.yaml`。
- **内容**：长周期趋势/风险代理（例：`price/MA200`、低波动 regime 指示）。
- **原因**：作为 TVTP 的稳定驱动，减少微观特征噪声放大。

---

## 模型层（model/*）

### 6.1 状态定义从三态到两态【修订 2.0】
- **旧**：`balance / trend / transition`。
- **新**：`balance / trend`，其中“transition”由**转移概率的动态**表示。
- **原因**：提高可辨识性与样本外稳健性，避免稀疏态导致的参数漂移。

### 6.2 2-State TVTP-HMM 训练与制品【修订 2.0】
- **代码位置**：`model/hmm_tvtp_hsmm/train_tvtp.py`（新增）。
- **要点**：
  1) 多起点 EM / BFGS 稳健化；
  2) TVTP 驱动：`X = [microflow, macro_factor]`；
  3) 产出 `artifacts/`：
     - `states=[balance, trend]`；
     - `tvtp/drivers` 列表；
     - `macro_factor_used` 布尔；
     - 四键签名；
     - 交叉验证摘要（AIC/BIC、解的多样性）。

### 6.3 推断接口（state_inference.py）【修订 2.0】
- `predict_proba(snapshot) → {state, confidence, P(Si→Sj)}`
- **原因**：为决策层提供统一的“触发 + 方向”上游输入。

---

## 决策层（decision/*）

### 7.1 状态转移方向性判断器【修订 2.0】
- **位置**：`decision/directional_classifier.py`（新增）。
- **功能**：当 `P(Si→Sj)` 超阈值时，融合 `MFI/CVD/InformedFlow` 推断 **bull/bear + 置信度**。
- **日志**：`logs/decision_log.jsonl` 写入 `trigger`、`classifier_confidence`、输入因子快照。
- **原因**：把“进入趋势”与“做多/做空”分离，提升解释与风险开合。

### 7.2 规则引擎扩展（transition 触发）【修订 2.0】
- 示例见附录 A。与原有规则并行，不破坏向后兼容。

---

## 执行层（execution/*）

- **不改目录与接口**，仅在策略编排中消费新增的 `trigger` 与 `classifier_confidence` 字段；
- **风控**：保留现有成本鲁棒性与滑点预算；新增对“方向误判率”的运行监控面板（命中率、翻转率、滞后分布），作为软门控信号写入 QA。
- **原因**：把新判断器纳入“先观测、后放权”的渐进控制。

---

## 验证与发布（validation/*, output/*）

### 9.1 Validator 输出增强【修订 2.0】
- **新增**：方向性分层统计（bull/bear 子样本收益与稳定性）、含/不含宏观因子的 TVTP 对比。
- **产出**：写入 `output/publish_docs/VALIDATION.md` 与 `output/results/OF_V6_stats.xlsx`。
- **原因**：让改动的有效性可量化、可回放。

### 9.2 发布门控与追溯（不改机制，强化绑定）【修订 2.0】
- **绑定**：`release.yml` 校验 `switch_policy.yaml`、Validator 摘要、四键签名一致性。
- **原因**：治理闭环，减少“带病上线”。

---

## 迁移、兼容与风险

- **迁移**：
  - 删除“第三态”相关配置/文案；
  - 新增宏观因子配置文件，不启用时默认 `macro_factor_used=false`；
  - 规则库可并存旧触发与新触发，逐步迁移；
- **兼容**：旧结果读取不受影响（新增字段向后可选）；
- **风险**：宏观因子选择失当可能引入偏置；方向判断器早期置信不稳，需观察期与回滚预案。

---

## 附录 A：Schema/RULES 补全示例

### A.1 RULES：transition 触发
```yaml
- id: enter_trend_bull
  when: transition(balance->trend, prob>0.80, min_dur: 3)
  then:
    directional: use_classifier   # 读取 directional_classifier 输出
    risk_cap: mid
    note: "entering trend with directional confirmation"
```

### A.2 SCHEMA_model.json 关键段
```json
{
  "states": ["balance", "trend"],
  "tvtp": {"enabled": true, "drivers": ["MFI","CVD","MA200"], "link": "logit"},
  "macro_factor_used": true
}
```

### A.3 SCHEMA_decision.json 关键段
```json
{
  "trigger": {"type":"transition","from":"balance","to":"trend","prob":0.86,"threshold":0.80},
  "directional_classifier": {"label":"bullish","confidence":0.91}
}
```

---

## 附录 B：目录映射与制品产物

| 层级 | 目录 | 关键制品 | 本次修订 |
|---|---|---|---|
| 治理 | governance/* | SCHEMA_model.json / SCHEMA_decision.json / RULES_library.yaml | 字段与触发语法扩展 |
| 数据 | data/* | manifests/*、对齐样表 | 无结构变更 |
| 特征 | features/microflow/* / macro_factor/* | `macro_factor.parquet` | **新增宏观因子** |
| 模型 | model/hmm_tvtp_hsmm/* | artifacts/* | **两态 TVTP + 制品字段** |
| 决策 | decision/* | directional_classifier.py、rules/* | **新增判断器 + 触发** |
| 执行 | execution/* | switch_policy.yaml | 绑定发布校验 |
| 验证 | validation/* | OF_V6_stats.xlsx, VALIDATION.md | **新增分层与对比** |

---

## 附录 C：卡片映射（与 todo 对应）

- **207** 宏观状态因子构建（features/macro_factor/*）【修订 2.0】
- **303** TVTP-HMM/分组转移矩阵（强化宏观驱动）【修订 2.0】
- **305** 状态推断接口统一（state_inference）【修订 2.0】
- **607-B** 状态转移方向性判断器（decision/directional_classifier.py）【修订 2.0】
- **865** RULES/SCHEMA 扩展与 CI 校验（governance/*）【修订 2.0】

> 注：卡片编号与原 todo 体系保持一致；本次仅**新增/强化**，不拆不并。


---

## OrderFlow V7 项目说明书（前瞻版，含工程对比）

> 版本：7.0 – 前瞻工程草案\
> 日期：2025-10-26\
> 作者：OrderFlow 研究组\
> 适用标的：BTC/USDT、ETH/USDT（主实验）\
> 依托版本：OrderFlow-V6 主分支（2025-10-18）

---

## 0. 为什么需要 V7（思维哲学 / 策略逻辑 / 思想本质）

### 0.1 V7 的世界观（哲学）

- **非稳态假设**：我们不再认为“市场状态”——即 V6 中定义的**平衡状态（市场买卖力量大致对称、价格波动围绕公允价震荡）**************与**************失衡状态（主动买卖单压倒另一方、价格趋势单边扩散）**——拥有跨周期的固定统计特征。市场是一个**连续漂移的信号源**。\
  用更直白的比喻：市场就像一条不断流动、形状随时在变的河流——你无法两次踏入同一条河。

- **动态可分性**：动态可分性指模型在任意滚动窗口内，都能找到一条合理的“分界线”，将当期市场特征划分为两类（例如相对平衡与相对失衡）。模型的任务从“定义状态是什么”切换为“**维持数据在当前窗口内可被稳定二元区分（A/B）**”。这种划分不追求永恒语义，而追求短期的可操作性与稳定性。

- **元认知谦逊**：承认模型对**长期语义**的无知。这里的“长期语义”指市场状态在多年或跨周期中具体代表的经济含义（例如“趋势=机构增仓”、“平衡=资金观望”），这些语义随市场结构演化而失效。模型的**长期记忆**——即对过往窗口特征分布和状态含义的经验记忆——在非稳态市场中会成为偏见，因此 V7 采用“滚动再编码”持续更新认知。拒绝/旁路机制则用于避免模型对当期无法解释的行为（如极端行情或结构突变）**进行强行解释**，而是选择“暂停判断”。

- **双层约束**：双层约束体系由“慢约束”和“快约束”组成，目的是**在市场连续漂移中保持系统稳定与反应灵敏之间的平衡**。慢约束是“河床”，由宏观流动性、波动分层、长期趋势锚（如 MA200）构成，决定模型的宏观边界；快约束是“水流”，由订单流、成交密度、CVD、POC、VAH/VAL 等快变特征的动态聚类形成，确保模型对短期微观结构快速响应。

- **信任即风险**：在 V6 中，模型的核心输出是**状态标签**（balance 或 trend）——交易系统依据标签来选择策略模式。而在 V7 中，交易的核心输入不再是这种标签，而是“**可分性的置信度**”。“可分度”定义为模型对当前市场结构是否仍可被区分的信心，它由状态后验概率（A/B 的区分度）与样本到边界的距离共同决定。换言之，它是关于“当前划分仍然可信”这一命题的置信度。置信度高 → 表明结构稳定、可持仓；置信度低 → 市场模糊、应降仓或观望。

> 比喻：在 V6，我们问的是“这条河现在是左岸还是右岸”；在 V7，我们问的是“这条河的两岸是否还清晰可辨、够不够安全去渡河”。

---
## 0.1：策略哲学下的收益与风险解释（精简版）

### D.1 核心逻辑

V7 不再以预测市场为目标，而以维持结构可分性为核心。收益与风险均源自模型对结构可辨性的感知质量。

> 只有当市场结构变化可被分辨时，系统才具备收益可能；否则盈亏仅是系统性风险 × 风险敞口。

### D.2 收益的本质

收益源于结构清晰期的正确行动，可表示为：

\[ E[R_t] = \alpha + \beta_1 Clarity_t + \beta_2 TransitionProb_{t-1,t} + \beta_3 MacroAnchor_t + \epsilon_t \]

> 收益是结构可辨时系统的反应速度，而非价格预测精度。

### D.3 风险的本质

风险是结构失效的概率，可表示为：

\[ Risk_t = \gamma_0 + \gamma_1 PrototypeDrift_t - \gamma_2 AbstainRate_t + \eta_t \]

> 风险来自系统在失真时不自知，而非市场波动。

### D.4 一行总结方程

\[ R_t = \beta_1 Clarity_t - \beta_2 Drift_t + \epsilon_t \]

当结构清晰（高 Clarity，低 Drift）时有正期望；结构模糊时收益坍缩为系统暴露。

> V7 的核心收益机制：**在市场可分时赚，在结构漂移时停。**


## 1. 版本沿革与“生态位迁移史”（V4 → V7）

| 版本     | 核心假设             | 方法论                            | 研究目标       | 市场生态位  |
| ------ | ---------------- | ------------------------------ | ---------- | ------ |
| **V4** | 市场具可观测的拍卖结构      | 指标/规则树 + 基础回测                  | 证明订单流可用    | 原型验证者  |
| **V5** | 结构可标准化与验证        | 因子模块化 + Validator              | 科学化显著性与鲁棒性 | 规则工程师  |
| **V6** | 两态（平衡/失衡）可被建模并预测 | 2-state TVTP-HMM + 方向判断器 + 宏观锚 | 可预测转移并执行   | 结构建模者  |
| **V7** | 市场无稳态，状态仅为临时聚类   | 滚动聚类 + 自适应 TVTP + 置信度映射        | 维持可分性与自校准  | 自适应感知者 |

> **生态位迁移**：从“捕猎信号”（V4/5），到“构建结构”（V6），到“维护可信度”（V7）。

---

## **2. 策略哲学与研究逻辑**

### **2.1 策略哲学**

1. **非稳态假设**：\
   市场状态的特征分布随流动性与参与者结构漂移。任何长期固定描述皆不可持续。
2. **动态可分性原则**：\
   模型在滚动窗口上维持对市场数据的最优二元划分（A/B），\
   不执着语义，仅保证“当下仍可被区分”。
3. **元认知谦逊**：\
   模型持续承认自身的“未知”，\
   当置信度衰减即自动降低参与度——\
   交易信号不是预测，而是**对信任程度的表达**。
4. **双层约束结构**：
   - **慢约束（河床）**：宏观流动性、波动率、趋势周期（MA200、Vol Regime）。
   - **快约束（水流）**：滚动窗口内的订单流动态聚类（A/B 二元分类）。
5. **风险-置信映射**：\
   置信度 → 仓位；\
   模糊度 → 降仓或观望；\
   漂移 → 重新学习。

---

### **2.2 策略逻辑（V6 → V7 演化）**

| 层次V6 逻辑V7 逻辑 |                                  |                                                  |
| ------------ | -------------------------------- | ------------------------------------------------ |
| **观测层**      | 采样 ATAS 订单流 + Binance OHLCV 双轨合并 | 保留双轨并加入窗口聚类标签与漂移度量                               |
| **特征层**      | AMT 因子（POC/VAL/VAH/CVD） + 宏观锚    | 同上 + 动态聚类生成的 A/B 标签与置信区                          |
| **模型层**      | 2-state TVTP HMM 预测状态转移          | 动态聚类（online GMM） → A/B 标签序列 → Adaptive TVTP 预测切换 |
| **方向层**      | 独立方向判断器长期运行                      | 仅在切换事件触发时启用 （低频高信噪）                              |
| **决策层**      | 状态=交易模式                          | 可分性=风险许可度，方向=触发信号                                |
| **执行层**      | 信号→仓位                            | 置信度→仓位 ； 漂移→停手                                   |
| **验证层**      | 收益/显著性                           | 可分性稳定度/漂移健康/校准度                                  |

---

## **3. 架构总览（与 V6 保持同构）**

```
/
├── governance/
├── data/
├── features/
├── model/
│   ├── clusterer_dynamic/        ← 新增
│   └── hmm_tvtp_adaptive/        ← 改造
├── decision/
│   ├── rules/                    ← 更新
│   └── engine/
├── execution/
├── validation/
└── output/

```

---

## **4. 治理层（governance）**

- **SCHEMA\_model.json (v7)**\
  新增字段：`clusterer_config`、`label_map`、`prototype_drift`、`drift_health`。\
  作用：记录聚类窗口配置与标签对齐状态。
- **SCHEMA\_decision.json (v7)**\
  新增字段：`clarity`（可分性指数）、`abstain`、`reason`。
- **RULES\_library.yaml (v7)**\
  增加规则触发类型：`on: transition(A->B, prob>τ)`、`on: low_confidence`。
- **CONTROL\_switch\_policy.yaml (v7)**\
  引入 `drift_metrics` 与 `abstain_rate` 门控，用于发布审计。

---

## **5. 数据与特征层（data/, features/）**

**继承 V6 AB 双轨**（ATAS 短期高频 + Binance 长历史低频），并引入：

- `cluster_artifacts.json`：保存每个窗口的聚类结果、原型向量、漂移指标；
- `macro_factor_slowanchor.parquet`：记录宏观锚定变量；
- 在预处理阶段插入 `rolling_cluster_labeler` 节点。

**修改原因**：支持“滚动再编码”与语义漂移监控。

---

## **6. 模型层（model/）**

### **6.1 新增：clusterer\_dynamic/**

- **算法**：在线 GMM / Online-EM （K=2，带遗忘因子 λ ≈ 0.97）。
- **输出**：A/B 标签、原型向量、标签映射、漂移统计。
- **设计理由**：解决 V6 中发射分布长期固定导致的语义老化。

### **6.2 改造：hmm\_tvtp\_adaptive/**

- **输入**：聚类标签序列 + 宏观慢变量。
- **目标**：学习“切换概率”而非“状态定义”；保留时序记忆。
- **特性**：
  - 滚动再训练（窗口滑动）；
  - label-switch 对齐；
  - 漂移触发软重置。

### **6.3 方向判断器（direction\_classifier）**

- 启动条件：`P(A->B) > τ_tr`。
- 特征：MFI、CVD、聪明钱 proxy、价量趋势。
- 输出：方向 {bull,bear,none} + 置信度。

---

## **7. 决策层（decision/）**

### **7.1 决策哲学**

- **信号即信任**：模型不是预测价格，而是输出“我有多确定”。
- **交易是置信度管理**：置信高 → 放权；置信低 → 缩手。

### **7.2 简化逻辑**

1. **门控阶段**：\
   漂移 warn/fail → 停止； 置信度 < 阈 → 观望。
2. **触发阶段**：\
   当 `transition_prob > 0.75` 且方向置信 > 0.7 → 发出 long/short。
3. **仓位阶段**：\
   仓位 ≈ clarity¹·⁵ × macro\_anchor cap；\
   clarity 由 state\_posterior 与 boundary\_distance 联合计算。
4. **退出阶段**：\
   状态模糊 → 减仓； 方向翻转 → 平仓。
5. **拒绝机制**：\
   任何阶段 abstain 都计入 `abstain_rate` 并在 validation 记录。

### **7.3 修改原因**

- 减少模型与决策间的语义错位；
- 以置信度为风险杠杆，更好适应非稳态市场。

---

## **8. 执行层（execution/）**

- 支持动作：`open_long`, `open_short`, `reduce`, `close_all`, `abstain`。
- 冷却控制：触发后 900 秒内不反向开仓。
- 资金分配：按 clarity 映射风险预算，结合 ATR 做止盈止损。
- 日志：记录 clarity、slow\_anchor、abstain 原因，供 Validator 复盘。

---

## **9. 验证与发布（validation/, output/）**

| 类别指标说明 |                                               |               |
| ------ | --------------------------------------------- | ------------- |
| 漂移监控   | `prototype_drift`, `PSI/KS`                   | 聚类语义变化率       |
| 校准性    | `ECE`, `Brier`                                | 模型可信度与真实切换一致性 |
| 触发健康   | `transition_hit_ratio`, `direction_flip_rate` | 事件有效性         |
| 风控效率   | `abstain_rate`, `reduce_events`               | 风险响应          |
| 结果度量   | `PnL_per_risk`, `drawdown`                    | 收益仅作观察        |

**发布门控条件**：

- 漂移 < 阈； 校准 > 阈； 拒绝率 ≤ 目标。
- 通过后生成 manifest 签名并部署。

---

## **10. 风险与迁移策略**

### **10.1 迁移步骤**

1. 建立 V6.5 实验分支：加入动态聚类与拒绝机制。
2. 回放两周 + paper trading 一周。
3. 校准阈值后合并为 V7 主线，冻结 V6。

### **10.2 潜在风险**

| 风险防御    |                 |
| ------- | --------------- |
| 短窗污染    | 慢锚限制 + AB 轨热切换  |
| 标签漂移    | 原型对齐 + 门控发布     |
| 工程复杂度膨胀 | 模块独立、冻结旧逻辑      |
| 数据不连续   | 通过 Binance 轨补历史 |

---

## **11. V6 → V7 关键机制对照**

| 模块V6V7变更目的 |                          |                          |            |
| ---------- | ------------------------ | ------------------------ | ---------- |
| 状态建模       | 固定语义 (balance/trend) HMM | 滚动聚类 A/B + Adaptive TVTP | 消除稳态假设     |
| 转移学习       | TVTP 预测切换                | 同上，但驱动变量扩展为宏观+漂移         | 增强适应性      |
| 方向判断       | 持续运行                     | 仅在切换触发时启用                | 降噪降滞后      |
| 风控逻辑       | 状态置信 → 交易模式              | clarity → 仓位权重           | 以可信度替代方向置信 |
| 宏观约束       | 参考指标                     | 河床锚（slow anchor）         | 双层约束成型     |
| 验证标准       | 显著性与收益                   | 漂移、校准、健康度                | 从预测到可解释稳定  |

---

## **12. 结论与前瞻**

V7 是 OrderFlow 体系的**分水岭版本**：

- 从“解释市场”到“感知市场”；
- 从“预测结果”到“维持可信度”；
- 从“模型自信”到“模型自律”。

它让系统在结构漂移的世界中仍能自校准，\
让策略不再依赖固定语义，而依赖动态信任。

> **V7 的目标不是赢得所有行情，**\
> **而是永远知道自己何时不该出手。**

### **下一步（V7 → V8 蓝图）**

- 加入 Meta-Learning 层自动调整窗口长度与阈值；
- 引入多资产横向共振（跨品种状态一致性检验）；
- 探索自适应学习率与动态遗忘机制，使系统具备“自愈”能力。

---

## 附录 C：Clusterer Dynamic 输入与门控参数（对齐 ATAS 导出字段）

> 本附录以你提供的 `bar_YYYYMMDD.jsonl`（schema_version=v6.3，exporter_version=6.3.0.0）为准，对 **Clusterer Dynamic** 的输入字段、特征工程与配置进行明确约定，便于直接落地到 `model/clusterer_dynamic/`。

### C.1 ATAS 导出字段总表（按样例）
| 字段 | 类型 | 示例 | 含义/备注 |
|---|---|---|---|
| `timestamp`, `timestamp_utc` | ISO8601 | `2025-10-24T00:01:00...` | bar 时间（UTC 优先） |
| `tz` | str | `UTC` | 时区记号 |
| `open, high, low, close` | float | `110500.0` | OHLC |
| `volume` | float | `17.179` | 当根成交量 |
| `poc` | float/null | `null` | 当根 POC（部分分钟无） |
| `vah`, `val` | float/null | `null` | 当根 VAH/VAL（部分分钟无） |
| `cvd` | float | `-240.082` | 累积成交差（样例为 `cvd_mode: rolling:60`） |
| `absorption_detected` | bool | `false` | 当根是否检测到吸收单 |
| `absorption_strength` | float/null | `null` | 吸收强度（可能缺失） |
| `absorption_side` | str | `buy/sell` | 吸收方向（与 *detected* 联合解读） |
| `bar_vpo_price` | float | `110540.0` | 本 bar 的 Volume Point of Bar（体积峰值价） |
| `bar_vpo_vol` | float | `6.568` | 该价位对应体积 |
| `bar_vpo_loc` | float | `0.0~1.0` | 体积峰位置（0=low 侧，1=high 侧） |
| `bar_vpo_side` | str | `bull/bear` | 体积峰偏向（多/空） |
| `window_id` | ISO8601 | bar 开始时间 | 由导出器标记的窗口 ID（信息性） |
| `flush_seq` | int | `122` | 刷新序列号（信息性） |
| `window_convention` | str | `[minute_open, minute_close] right-closed` | 窗口闭合规则（信息性） |
| `fingerprint` | str | `V6-PARTITIONED` | 数据指纹（信息性） |
| `filename_pattern` | str | `bar_YYYYMMDD.jsonl` | 文件模式（信息性） |
| `exporter_version` | str | `6.3.0.0` | 导出器版本（信息性） |
| `schema_version` | str | `v6.3` | schema 版本（信息性） |
| `backfill` | bool | `true` | 回补标记（信息性） |
| `cvd_mode` | str | `rolling:60` | CVD 的计算窗口说明 |

> 说明：信息性字段不进入聚类，仅用于审计与溯源。

### C.2 聚类输入特征（从 ATAS 字段映射）
> 聚类输入为**连续向量**，缺失采用“业务合理填补 + 指示变量”策略，所有连续特征将进行**稳健标准化**（winsorize→z-score）。

| 特征名（聚类用） | 来源/构造 | 标准化/填补 | 说明 |
|---|---|---|---|
| `ret_1m` | `(close/open)-1` | z-score | 单根收益率（形态归一） |
| `hl_range` | `(high-low)/mid` | z-score | 相对振幅，`mid=(high+low)/2` |
| `vol_1m` | `volume` | log(1+x)+z | 成交量尺度归一 |
| `cvd_norm` | `cvd` / ATR 标尺 | z-score | 以波动标尺标准化的 CVD |
| `vpo_loc` | `bar_vpo_loc` | 已在 [0,1] | 体积峰在 K 线内的位置 |
| `vpo_bias` | `1{bar_vpo_side==bull}-1{==bear}` | {-1,1} | 峰值多空偏向 |
| `vpo_intensity` | `bar_vpo_vol / volume` | z-score | 峰值体积分数（强弱） |
| `spread_proxy` | `(high-low)/close` | z-score | 价差近似（无 L2 时） |
| `absorb_flag` | `1{absorption_detected}` | {0,1} | 吸收事件指示 |
| `absorb_side` | `1{buy}-1{sell}` | {-1,1} | 吸收方向（缺失→0） |
| `absorb_strength` | `absorption_strength` | 填 0 + z | 缺失用 0，保留强度信息 |
| `poc_gap` | `poc - close` | 填 0 + z | POC 缺失→0（并结合 flag） |
| `vah_gap` | `vah - close` | 填 0 + z | VAH 缺失→0 |
| `val_gap` | `val - close` | 填 0 + z | VAL 缺失→0 |
| `trend_proxy` | `EMA(close, n=20)-EMA(close, n=50)` / close | z-score | 轻量趋势锚（快约束参考） |
| `rv_5m` | `realized_vol(5m)` 近似 | z-score | 短期实现波动（可由 hl 构造） |

> 备注：对 `poc/vah/val` 的缺失，使用零填补 + 各自缺失 flag（不进入聚类，仅在审计中存留）可避免偏移。

### C.3 滚动窗口、滑动与遗忘
- **窗口长度**：`W = 30 天`（或 `43200` 根 `1m` bar）；
- **滑动步长**：`Δ = 1 天`（`1440` 根）；
- **遗忘因子**：`λ = 0.97`（在线 EM 权重系数，强调新数据）。

> 如需更快适应高波动周，可将 `W` 降至 14 天、`λ` 升至 0.985。

### C.4 发布门控阈值（建议）
| 指标 | 阈值 | 含义 |
|---|---|---|
| `prototype_drift`（KL/PSI 归一） | `< 0.15` | 原型漂移健康；超阈值触发再标定/软重置 |
| `ECE`（校准误差） | `< 0.08` | 概率输出与真实频率一致 |
| `Brier`（切换概率误差） | `< 0.18` | 转移预测误差可控 |
| `abstain_rate`（拒绝率） | `0.10–0.25` | 风险自控强度适中；过低或过高均需复盘 |
| `transition_hit_ratio` | `> 0.60` | 触发事件后的结构推进命中率 |

### C.5 `clusterer_dynamic/config.yaml` 示例
```yaml
version: 1
window:
  length_days: 30
  step_days: 1
  bar_interval: 1m
online_em:
  forgetting_lambda: 0.97
  components: 2
  covariance_type: full
  reg_covar: 1.0e-6
  init: kmeans++
features:
  continuous:
    - ret_1m
    - hl_range
    - vol_1m
    - cvd_norm
    - vpo_loc
    - vpo_bias
    - vpo_intensity
    - spread_proxy
    - absorb_flag
    - absorb_side
    - absorb_strength
    - poc_gap
    - vah_gap
    - val_gap
    - trend_proxy
    - rv_5m
  transforms:
    winsorize:
      lower_q: 0.01
      upper_q: 0.99
    standardize: zscore
    missing_policy:
      fill_zero: [absorb_strength, poc_gap, vah_gap, val_gap]
label_alignment:
  method: hungarian
  distance: kl_mahalanobis
  drift_metric:
    - psi
    - ks
  thresholds:
    prototype_drift: 0.15
artifacts:
  save:
    - labels_wt.parquet
    - cluster_artifacts.json
```

