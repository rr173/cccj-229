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
  store.py                SQLite：目录/注册表版本历史、计划、切片存储、镜像版本台账
  slices.py               离线指标切片：闭包编译、JSONL 更正解析、增量刷新、读取、
                          单笔撤回影响查询
  mirror.py               切片镜像交付：源端启用/流式全量与补发、接收端校验/原子入库/查询
  cli.py                  全部对外接口
reference/
  ref_analyzer.py         独立参考实现（零共享代码，按同一套 AST/契约规则重算）
demo/
  data/*.json             演示目录与注册表（收入/净收入/区域报表多版本）
  reconcile.py            产品 vs 参考 逐项对账
  run_demo.sh             端到端演示（含失败断言）
  run_mirror_demo.py/.sh  切片镜像交付端到端自校验演示
tests/
  test_slice.py           切片刷新链路测试（独立整算基准逐批核对）
  test_mirror.py          镜像交付测试（版本、补发净差、完整性、来源链、快照并发）
  test_slice_impact.py    单笔撤回影响测试（完整清单对独立整算预言机核对）
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
slice-init    --slice S --catalog cat-v1 --seq 1 --metric revenue@v1
                                         # 首次刷新：创建命名切片（钉住目录导入 + 根指标）
slice-refresh --slice S --file corr.jsonl  # 应用一批事实更正（增量）
slice-read    --slice S                    # 读取根指标分组结果（JSON）
slice-impact-prepare --slice S             # 一次性准备单笔撤回影响索引
slice-impact  --slice S --key '{"day":"2026-10-01","region":"east"}'
                                         # 单笔撤回影响：当前值 + 会改变它的订单（JSON）
slices                                   # 切片列表
mirror-enable  --slice S                  # 现有切片启用镜像交付，落第 0 版
mirror-export  --slice S --out f.jsonl [--from N]  # 全量或 N→当前版补发
mirror-receive --db rpt.db --file f.jsonl # 接收端校验并原子入库（独立库）
mirror-query   --db rpt.db --source-id ID --slice S [--key K]  # 查询镜像
mirror-status                             # 源端已启用切片与版本；mirrors 列接收端
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

## 离线指标切片（增量刷新）

月底对账会更正订单的归属区域、客户号等事实。切片（slice）是一张可反复读取的
离线指标视图：把更正写进去，下次读取就拿到当前事实对应的分组数值，不再反复
扫描整份订单。切片只消费本仓库已导入的指标定义，**不生成迁移计划，也不运行
生产查询**。

### 生命周期

1. **首次刷新（`slice-init`）**：为命名切片选定已导入目录的 `catalog_id`、导入
   序号 `seq` 和一个显式 `name@version` 的根指标，从空事实集开始。初始化时校验
   根指标的整个版本引用闭包：闭包内有依赖环、依赖粒度不是父粒度子集（无法投影）、
   或出现不支持的表达式，一律拒绝；闭包之外的指标（即使有问题）不影响本切片。
2. **之后只接受事实更正**（`slice-refresh`）：切片钉住 `(catalog_id, seq)`，
   不跟随该目录的后续导入，也不重绑指标。
3. **读取（`slice-read`）**：输出根指标的全部分组键和值。

### 输入：JSONL 更正批

每行一个 JSON 对象，表示一条更正：

```json
{"op": "put",    "id": "o-1", "row": {"orders.amount": 100, "orders.status": "completed", "orders.region": "east", "orders.day": "2026-10-01"}}
{"op": "delete", "id": "o-1"}
```

- `id`：非空字符串或整数；一批内每个 `id` 至多出现一次。
- `put` 给出**完整新行**（新增或替换）；`delete` 移除已存在的行。
- 行内字段名沿用目录字段名（一份已展平的事实表，无关联、无其他格式）；
  只需提供闭包引用到的字段，多余字段被忽略。
- 事实值只支持字符串、布尔、有符号 32 位整数；维度与 `count_distinct`
  字段必须是非空字符串；`date` 类型字段按字符串处理。
- **整批拒绝**：缺少被引用字段、类型不符、重复 `id`、删除不存在的 `id`。
  拒绝后切片保持上一批的完整结果（事实与物化值在同一事务提交）。

### 输出：`slice-read` 的 JSON

```json
{
  "slice": "month-end",
  "catalog": {"catalog_id": "cat-v1", "seq": 1},
  "root": "net_daily@v1",
  "grain": ["day", "region"],
  "facts": 4,
  "groups": [
    {"key": {"day": "2026-10-01", "region": "east"}, "value": -200},
    {"key": {"day": "2026-10-01", "region": "west"}, "value": null}
  ]
}
```

- `groups[].key` 按目录 `grain` 顺序给出维度值；空粒度根的键为 `{}`。
- `value` 为精确整数（任意精度）或 `null`。**分组只有在至少一条合格事实
  存在时才存在**：依赖组消失时父组记录保留、值为 `null`；根组消失则整条
  记录从结果中移除——`0`、`null`、缺失三者语义不同。

### 切片支持的定义子集与语义

- 过滤器：可缺省，或布尔常量、`字段 = 同类型字面量`、`and`、`or`。
- 分组表达式：`sum(field)`、`count_distinct(field)`、显式版本的指标引用、
  `add`、二元 `sub`。
- 闭包内每个指标对**更正后的完整事实集**使用自己的行过滤器和 `grain`；
  维度沿用目录的字段映射。
- 父指标只引用粒度为自身子集的依赖指标，按**同名维度投影**找到依赖组
  （不按前缀猜测，也不把依赖值复制到事实上再聚合）；依赖组不存在时引用
  取 `null`，算术传播 `null`。

### 刷新性质

- **增量**：按 `id` 查找更正行，只重算这些行直接波及的分组；分组值变化
  沿反向引用索引（`slice_refs`）波及引用它的上游分组——一个粗粒度组的
  变动会更新多个未被更正的父组，但绝不遍历全部事实或全部分组。
  `slice-refresh` 输出会报告 `rows examined` 与 `groups recomputed`。
- **原子**：事实与物化值同事务提交；一批失败（或进程中断）后重新打开
  数据库，只能看到上一批完整结果，刷新可继续。

### 单笔撤回影响查询（审计）

审计问的是“如果某一笔订单其实不该入账，单独撤掉它会怎样”。`slice-impact`
只读已提交状态：不删事实、不执行刷新再回滚，也不改变切片、刷新版本或镜像
历史。

```bash
# 新建切片（slice-init）时已准备好空索引，无需额外操作；
# 仅当切片缺少准备标记时才需要一次性准备：
python3 -m metric_analyzer.cli --db app.db slice-impact-prepare --slice month-end

python3 -m metric_analyzer.cli --db app.db slice-impact --slice month-end \
  --key '{"day": "2026-10-01", "region": "east"}'
# 也可用 grain 顺序的数组：--key '["2026-10-01", "east"]'
# 空粒度根的键为 {}；--key 必填且必须给出完整根分组键
```

输出（JSON）：

```json
{
  "slice": "month-end", "root": "net_daily@v1", "grain": ["day", "region"],
  "key": {"day": "2026-10-01", "region": "east"}, "current": -200,
  "changes": [
    {"id": "o-1", "state": "group_removed"},
    {"id": "o-2", "state": "null", "value": null},
    {"id": "o-3", "state": "value", "value": 0}
  ]
}
```

- `current` 是该组当前物化值（整数或 `null`）。
- 每笔 `changes[]` 都是**只撤这一笔**、回到同一份已提交事实的独立结果，
  彼此不累积；`state`：`group_removed`（组消失）、`null`（组仍在、值变
  `null`）、`value`（组仍在、精确整数，含 `0`）。只列出状态或值确实变化
  的订单，每个 ID 至多一条，ID 保留原 JSON 类型（字符串或整数）。
- 指定的根组不存在时命令失败（退出码 1，明确报告查无此组），不返回看似
  成功的空清单。
- 撤回按本切片已绑定的定义计算：过滤、维度投影、依赖与当前刷新规则一致。
  一笔事实可能同时属于目标组和它引用的粗/细组，甚至出现在两条计算路径上；
  查询一次性从所有这些位置移除它，再判断总体结果（相抵而值不变则不出现在
  清单里）。共享同一去重值的另一笔订单仍保留该值。不在目标组、但通过粗
  粒度依赖影响该格子的订单也会被列出。

**准备与开销**：`slice_fact_groups` 记录每条已提交事实在每个闭包指标下所属
的唯一分组；`slice-init` 以空集建立，之后每次 `slice-refresh` 在自己的事务
内只重写本批更正行对应的条目，失败批次不会留下半新半旧的关系，也无需每批
重建全部准备数据。单次查询只访问目标组、沿引用到达的实际分组、参与这些
分组的事实与物化叶子（以及一次性的组成员清单），不扫全部事实或全部根组，
也不对每个候选重新遍历分组。刷新本身仍只处理更正波及的范围。

### 测试

```bash
python3 -m unittest
```

`tests/test_slice.py` 用一个不调用刷新实现的整算基准（每批都全量扫描所有
事实重算）逐批核对增量结果，覆盖：两个细组共用一个粗组且同批发生换组与
过滤进出；删掉同一客户的部分订单再删其最后一条订单（去重计数）；依赖组
消失时仍存在的父组 vs 根组消失 vs 结果为零；含坏行批次回滚后继续刷新；
以及用不相关分组验证单个粗组更正不退化为全量计算。

`tests/test_slice_impact.py` 用“逐笔移除后整算重算”的独立预言机核对完整
撤回清单：粗组两个客户的出现次数重新分布而根值不变时，下一次查询换对的
事实 ID；同一笔事实沿两条路径贡献相抵；撤回后依赖缺失（组仍在、值
`null`）与根组消失的区别；共享去重值；整数 ID 类型；空粒度键；不存在
的根组报错；少量不相关事实保证查询不把整库当候选（并检查 SQL 不出现全表
扫描）；坏批次失败后重新打开库仍得到先前清单；查询不推进镜像版本；
以及 CLI 端到端。

## 切片镜像交付（离线版本化补发）

区域报表机只接收离线文件：它没有订单库，也不应看到订单明细。镜像交付只
搬运**一个已存在命名切片的根指标分组**，不传事实、依赖组、目录或执行计划，
也不改变刷新或指标求值规则。报表机拿到的是一个独立 SQLite 镜像库，不需要
源库即可直接查询某一版的分组键和值。

### 版本与来源身份

- `mirror-enable` 为现有切片启用同步：把当时**已提交**的根结果记作第 0 版
  （只顺序遍历一次根组，不补建旧历史），并生成一个 16 字节随机 `source_id`
  作为这条来源链的持久身份。
- 启用后，每次**成功**的 `slice-refresh` 都在刷新自己的事务内把版本前进一版，
  即使根结果完全没变（空批次也占一版）；被拒绝或回滚的刷新不占版本。版本推进
  与根组净变更、事实/物化值在同一事务提交，进程中断后重新打开库只能看到完整
  的上一版。
- 交付坐标是 `(source_id, slice_name, version)`，三者一起持久化，重开数据库
  不变。另一份独立源库即使有同名切片、相同版本号，也有不同的 `source_id`，
  无法接着往这个镜像里写。

### 命令

```bash
python3 -m metric_analyzer.cli --db app.db <cmd>

mirror-enable  --slice S                 # 现有切片启用同步，落第 0 版
mirror-export  --slice S --out f.jsonl [--from N]
                                         # 无 --from：全量；--from N：N→当前版补发
mirror-status                            # 源库：列出已启用切片与当前版本
# 报表机（独立库，无需源库）：
mirror-receive --db rpt.db --file f.jsonl   # 校验并原子接收
mirror-query   --db rpt.db --source-id ID --slice S [--key '{...}']
mirrors       --db rpt.db                  # 列出该接收库中的镜像
```

### JSONL 文件格式（带完整性校验）

每行一个 JSON 对象、UTF-8，三类行：

1. **首行 header**：`kind`（`mirror-full`/`mirror-delta`）、`format`、`slice`、
   `source_id`、`version`（delta 另有 `from_version`）、`root`、`grain`、
   `catalog` 坐标。
2. **中间记录**（流式写入，不把全部结果攒进内存）：
   - 全量：`{"kind":"group", ...coords, "gkey":[...], "value":"<十进制>"|null}`，
     列出该版所有根组；
   - 补发：`{"kind":"op", ...coords, "gkey":[...], "change":"upsert"|"delete",
     "value"?}`，每个键至多一条操作。新增或值不同给终点值；原来有而终点没有
     给 `delete`（不带 value）；中途改了又恢复的键不发送。`upsert` 的
     `value` 为 JSON `null` 表示"存在但值为 null 的组"，与删除严格区分。
3. **末行 manifest**：`records`、`header_digest`（首行字节的 SHA-256）、
   `digest`（除末行外全部字节的 SHA-256）。整数值以**十进制文本**传输，
   任意精度（含超过 64 位的整数）端到端不丢精度。

**如何确认文件结束、版本与来源身份**：文件的最后一行必须是 `manifest`，
其后不得再有字节；其 `digest` 必须等于对前面所有字节流式算出的 SHA-256，
`records` 必须与实际记录数一致，且每行的 `source_id/slice/version` 必须与
header 一致。任一不符即视为截断/损坏，整体拒收。

### 导出语义

- 导出用独立连接打开一个 `BEGIN DEFERRED` 快照事务（源库切到 WAL，导出不
  阻塞后续刷新）。一次导出只交付一个已提交时点：文件写到一半时中心再刷新，
  新值既不会混进文件，也不会改写文件标注的版本；刷新无需等待整个文件写完。
- 全量文件可在任一当前版本导出，但**只用于建立新镜像**。补发接受启用后的
  任一旧版坐标，以**开始导出时的当前版本**为终点。
- 补发不遍历全部事实或全部现存根组：每版刷新只把该版根组的净变更记入
  `slice_sync_log`（磁盘保留，本题不清理）；补发仅取出区间内变更过的键，
  逐键查两端状态求净差。初次全量允许顺序遍历根组；两类文件都通过服务端
  游标流式产出。

### 接收语义（独立 SQLite 库）

接收在单事务内把记录写入临时暂存表并完成全部校验，任何失败都整体回滚——
分组与接收坐标都不会前进；只有完整接收并提交后，重新打开库才能一起看到
新分组和新坐标。具体规则：

- 全量：仅当该 `(source_id, slice)` 镜像不存在时建立；已存在则拒收（过期
  全量不能覆盖新结果），除非是台账里已记录的同一文件的精确重投。
- 补发：镜像必须已存在，且其当前版本必须等于 `from_version`；从前版缺失、
  另一条来源链（`source_id` 不同）或错误坐标起步都报错。
- 同一完整文件可安全重投：台账按 `(source_id, slice, from_version, version)`
  记录已接收文件的 `digest`，字节完全一致则幂等无操作；同坐标但内容不同
  （digest 不同）一律拒收，不会被当成重投。

### 测试与演示

```bash
python3 -m unittest                 # 含 tests/test_mirror.py 的 22 个交付测试
bash demo/run_mirror_demo.sh        # 端到端自校验演示（无需服务/传输代理）
```

演示覆盖：从已有数据建 v0 镜像；跨多次刷新逐版补发，并把**实际补发项与
手算的两端差异逐条核对**（含一次只改粗粒度依赖、却改变另一日期根组值的
更正，改了又恢复的键不发送，null 组与删除的区分，超 64 位整数精度）；
导出快照与并发刷新的版本一致性；截断件与校验伪造件被拒、坐标不动，随后
完整件正常续收；以及独立来源链和同坐标异内容文件的拒收。

