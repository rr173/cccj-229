# 指标目录变更分析器 (Metric Catalog Change Analyzer)

数据团队修改指标口径时，防止下游报表在**没有提示**的情况下继续得到含义不同的数字。
分析器读取版本化指标定义、字段血缘、报表查询和消费者兼容声明，产出：

1. **受影响对象**（指标 + 报表，精确定位到查询的哪个 select 项）
2. **不可兼容原因**（语义化变更分类 + 契约违反说明）
3. **可执行迁移顺序**（依赖图拓扑：先底层指标，再派生指标，最后报表）

分析器**不执行生产查询**，也**不做字符串比较**：所有判断都基于受限声明式 AST
的规范化形式与粒度/过滤契约。

## 目录结构

```
metric_analyzer/          产品代码
  astx.py                 受限 AST：校验、规范化、过滤条件蕴含、引用提取
  core.py                 语义 diff、影响分析、阻断规则、拓扑排序、计划应用（纯函数）
  loader.py               目录/注册表导入校验（引用必须可解析）
  store.py                SQLite：目录/注册表版本历史、计划及输入绑定摘要
  cli.py                  全部对外接口
reference/
  ref_analyzer.py         独立参考实现（零共享代码，按同一套 AST/契约规则重算）
demo/
  data/*.json             演示目录与注册表（收入/净收入/区域报表多版本）
  reconcile.py            产品 vs 参考 逐项对账
  run_demo.sh             端到端演示（含失败断言）
```

## 数据模型

**目录（catalog）**：`catalog_id`、`dimensions`（维度→字段映射）、`fields`（字段血缘：
名称/表/类型）、`metrics[]`。每个指标版本包含：`name`、`version`、`effective_from`、
`grain`（维度粒度）、`filter`（过滤条件 AST）、`agg`（聚合表达式 AST）。依赖指标从
`agg` AST 中的 `metric` 引用推导，**引用必须显式钉住版本**；同名新版本与旧版本
并存，绝不隐式替代。

**注册表（registry）**：`reports[]`（声明式查询：select 指标@版本、group_by、where）
和 `consumers[]`（兼容声明：`allowed_changes`、`migration_window`、`target_versions`）。

**受限 AST**（`astx.py` 头部有完整文法）：`lit/field/metric/add/sub/mul/div/neg/
sum/avg/min/max/count/count_distinct/and/or/not/cmp/in/case/coalesce`。
校验规则：字段引用必须存在于血缘、指标引用必须可解析到具体版本、过滤条件中
禁止聚合与指标引用、聚合不可嵌套、指标引用不可出现在聚合参数内、类型相容。

## 变更分类（语义化，非字符串比较）

| 类型 | 判定 |
|---|---|
| `DIMENSION_ADDED` | 新粒度是旧粒度的严格超集（仅增加可选维度） |
| `GRAIN_CHANGED` | 粒度收缩或重排（列出被移除的维度） |
| `FILTER_CHANGED` | 过滤条件规范化后不等价；子类型 `NARROWED`/`WIDENED`/`RESHAPED` 由合取式谓词蕴含判定（等值⊂IN、IN 集合包含、数值区间包含） |
| `DEPENDENCY_CHANGED` | AST 中指标引用 (name, version) 集合变化（依赖替换） |
| `AGG_CHANGED` | 抽象掉字段/指标引用后的计算骨架仍不同 |
| `FIELD_REMOVED` / `FIELD_ADDED` | 表达式引用的字段集合减少/增加（删除字段需消费者声明，新增为提示性） |

规范化：交换律（and/or/add/mul 排序）、结合律（同算符扁平化）、幂等（and/or 去重）、
`=`/`!=` 操作数排序、`IN` 值集合化。等价判断作用于规范化 AST。

## 阻断规则（计划状态 BLOCKED）

- `CYCLE`：迁移目标版本及其引用闭包内存在依赖环
- `MISSING_TARGET`：声明的目标版本在目录中不存在，或引用版本被移除且未声明迁移
- `UNDECLARED_COMPAT`：实际变更类型超出消费者 `allowed_changes`
- `VERSION_CONFLICT`：同一报表查询同时引用同一指标的两个冲突版本
- 另有 `VERSION_REMOVED` / `UNKNOWN_METRIC` / `INVALID_QUERY` / `UNKNOWN_REPORT`

## 计划绑定与失效

计划生成时绑定：源/目标目录内容哈希、注册表内容哈希、每个受影响报表的
**查询文本摘要**（规范化 JSON 的 SHA-256）、每个相关消费者声明的摘要。
`execute` 时逐一重算比对：任何输入变化 → 计划置为 `INVALIDATED` 并拒绝执行
（退出码 3），绝不静默沿用旧计划。执行只是注册表状态迁移（报表查询重定向到
目标版本并落库为新序列），**不触碰生产数据**。

## 接口

```bash
python3 -m metric_analyzer.cli --db app.db <cmd>

import-catalog  --file cat.json            # 目录导入（导入即 AST 校验，不合格拒绝）
import-registry --file reg.json            # 注册表导入（报表查询 + 消费者声明）
validate        --catalog cat-v2           # AST 校验 + 依赖关系报告
analyze         --from cat-v1 --to cat-v2 --registry reg-v2 [--json]
plan            --from cat-v1 --to cat-v2 --registry reg-v2 [--json]
blockers        --plan pln-xxx [--json]    # 阻断查询
explain         --plan pln-xxx [--step N]  # 逐项解释（每个迁移步骤/阻断项的原因）
execute         --plan pln-xxx             # 执行 READY 计划（过期计划被拒绝）
export-registry --registry reg-v2          # 查看迁移后的报表查询
plans                                    # 计划列表
```

退出码：0 成功；1 输入校验失败；3 计划已失效（输入变化）；4 计划状态不可执行。

## 演示与对账

```bash
bash demo/run_demo.sh
```

覆盖场景：过滤范围改变（`revenue` v1→v2，WIDENED）、粒度改变（`daily_revenue`
v1→v2 移除 date）、仅增加可选维度（`revenue` v1→v3 增加 channel）、依赖替换
（`net_revenue` v1→v2 改依赖 `revenue@v2`）、删除字段（`net_revenue` v2→v3 移除
`orders.cost`）、同名版本并存（v1/v2/v3 同时可解析）、消费者允许/不允许的变化、
依赖环（`metric_a@v2 ↔ metric_b@v2`）、同一报表引用冲突版本。

演示关键环节：

1. 声明不完整 → 计划 BLOCKED，三类阻断逐项解释；
2. 声明修正 → READY 计划，拓扑序 `revenue → net_revenue → 报表`；
3. **篡改目录后尝试旧计划 → `INVALIDATED` 拒绝（退出码 3），而非静默执行**；
4. 恢复目录 → 重新生成 → 执行成功，注册表落库为新版本；
5. `demo/reconcile.py`：独立参考程序按 AST 依赖与契约规则重算受影响集合与顺序，
   与产品逐项对账（migrations / violations / blockers / steps 四节，顺序精确一致）。
