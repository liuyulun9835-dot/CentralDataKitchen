# OrderFlow V6 项目说明书（修订 2.0）

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

