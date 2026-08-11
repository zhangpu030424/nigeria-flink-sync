# ng01 Application 数据质量报告 (2026-08-10)

## 样本

- **来源**：SeaweedFS `udp-archive` bucket, `archive/ng01/2026-08-10/`
- **抓取工具**：`internal/validator/regression_archive_test.go` (`TestArchiveRegression`)
- **抓取范围**：前 100 个 `.pb.gz` 归档文件（按 key 字典序）
- **展开后**：1901 条 `Application` 记录（`application` + `apply_list` 合并）
- **验证器版本**：`internal/validator/` @ 2026-08-10
- **时区**：`Africa/Lagos`（用于 F 段 today 派生）

## 规则触发汇总

| 规则  | 级别   | 触发次数 | 命中率*      |
|-------|--------|---------|-------------|
| C7    | ERROR  | 701     | 36.9%       |
| A5    | ERROR  | 342     | 18.0%       |
| F17   | ERROR  | 168     | 8.8% loans  |
| F11   | ERROR  | 84      | 4.4% loans  |
| C3    | ERROR  | 83      | 4.4%        |
| A12   | ERROR  | 9       | 0.5%        |
| F4    | WARN   | 8       | 0.4% loans  |
| C4    | ERROR  | 2       | 0.1%        |

*Application 级规则按 1901 分母；Loan 级 (F*) 按实际 loans 总数近似（未单列，约 1900）。

## 逐规则解读

### C7 — `disbursed_time == 0 && disbursed_amount > 0`（701）

约 37% 的申请记录带非零 `disbursed_amount` 但 `disbursed_time == 0`。

- **可能成因**：
  - `disbursed_amount` 字段语义在 ng01 payload 生成侧 = "计划放款额"而非"实际放款额"（与文档定义冲突）
  - 或：`disbursed_time` 写入侧漏字段，仅 `_amount` 写了
- **动作**：与 ng01 payload 生产团队对齐字段口径。若确认为"计划放款"，本文档 C7 规则需要放宽/删除；若确认为"实际放款"，则是严重上游 bug。

### A5 — `status ∈ DISBURSED ∪ POST_REVIEW_NO_DISB 但 reviewed_time == 0`（342）

约 18% 的申请状态已越过审核（REJECTED/APPROVED/LOAN_FAIL/DISBURSING/OUTSTANDING/OVERDUE/PAID_OFF/WRITTEN_OFF）但 `reviewed_time` 字段为 0。

- **可能成因**：ng01 上游数据回填时 `reviewed_time` 字段被漏写；或历史订单未回补该字段。
- **动作**：抽样确认是否所有历史订单都缺，还是仅特定时间段/状态；如为历史遗留，补历史回填任务。

### F17 — Loan `paid_time` 时间字段越界（168）

168 条 Loan 记录的 `paid_time` 违反 B1（≥ 1e12 ms）/ B2（13 位）/ B7（≤ `DecisionPayload.time`）之一。

- **可能成因**：秒/毫秒混用，或未来时间（数据回放/时钟漂移）。
- **动作**：抽 5 条实际值确认是哪种失败模式，再决定是修数据还是调规则。

### F11 — `loan.paid_time < application.disbursed_time`（84）

84 条 Loan 记录的 `paid_time` 早于同 application 的 `disbursed_time`。

- **可能成因**：
  - 时间戳错误（同 F17 的另一表现）
  - 或字段语义：`loan.paid_time` 在 ng01 侧可能不指"最后还款时间"而是别的（如"计划到期时间"）
- **动作**：结合 F17 一起看，若同一批 Loan 触发两条规则，通常是时间戳 bug。

### C3 — `status 过审但 principal/total_amount == 0`（83）

83 条已过审但金额字段未填。

- **可能成因**：与 A5 相关联——上游批量漏字段可能同时漏 `reviewed_time` 和 `principal/total_amount`。
- **动作**：交叉验证与 A5 是否高度重合（同一批记录）。若是，一次修复即可。

### A12 — `reviewed_time == 0 && disbursed_time > 0`（9）

9 条记录：无审核时间但有放款时间。

- **可能成因**：A5 的子集特例。数据完整性问题。

### F4 — Loan 金额加总不等（8）

8 条 Loan 记录 `total_amount != principal + interest + fee + penalty - reduction`。

- **可能成因**：可能是精度/舍入问题，或字段口径与文档 F4 不完全一致（如 reduction 语义）。WARN 级别，不阻断。

### C4 — `total_amount < principal`（2）

2 条 Application 应还 < 本金。极小样本，可能是脏数据。

## 未触发的规则（值得注意）

在 100 个文件、1901 条 apply 记录中，以下规则**零触发**：

- A1/A2/A3/A4/A6/A7/A8/A9/A10/A11/A13/A14（其他 A 段规则）
- B1-B8（Application 时间字段的数值/单调性/未来时间）
- C1/C2/C5/C6（金额基础规则）
- D.1/D1/D2/D3（Application × Loan 关系）
- E1/E2/E3/E4/E5（标识 & 银行卡必填）
- F1/F2/F3/F5/F6/F7/F8/F9/F10/F12/F13/F14/F15/F16（Loan 其他规则）

**含义**：
- ng01 payload 的**必填字段完整性**（mobile / id_number / app_id / product_id / bank_account_number）100% 达标
- Application 级**时间字段的合法性和单调性**没问题（问题只在 Loan.paid_time / A5 缺失场景）
- **Application × Loan 状态映射**（D.1 表）100% 一致——这是很好的信号
- **Loan 内部结构**（period 序列、roll_sequence 展期链）100% 一致

## 主要发现（一句话概括）

ng01 数据的字段**存在性**很好（无一漏），但字段**填写完整度**不够——放款/审核相关的时间戳和金额字段有系统性缺失，很可能是 payload 生成侧几个字段的写入被漏掉，而非随机脏数据。

优先级排序：
1. **P0 — 澄清 `disbursed_amount` 语义**（C7 一条影响 37%）
2. **P1 — 修复 A5/C3/A12/C6 共同点**（大概率是同一批漏写，可能一次修复覆盖 434+ 条命中）
3. **P2 — 抽查 F17/F11**（loan.paid_time 场景，168+84 条，需先看实际值）
4. **P3 — F4/C4 少量**（可以先归档观察，暂不修）

## 复现方式

```bash
source /opt/udp/ss/s3.env && export S3_ACCESS_KEY S3_SECRET_KEY
UDPDS_VALIDATOR_ARCHIVE_TEST=1 \
UDPDS_VALIDATOR_ARCHIVE_BID=ng01 \
UDPDS_VALIDATOR_ARCHIVE_DATE=2026-08-10 \
UDPDS_VALIDATOR_ARCHIVE_MAX=100 \
go test ./internal/validator/... -run TestArchiveRegression -v -timeout 300s
```

改 `UDPDS_VALIDATOR_ARCHIVE_MAX` 拉更多文件；改 `UDPDS_VALIDATOR_ARCHIVE_DATE` 换日期；改 `UDPDS_VALIDATOR_ARCHIVE_BID` 换 bid。

## 后续可扩展

- **拉全天** — 当前 100 文件是字典序前 100，只覆盖凌晨时段。全天扩到几千文件后分布可能变化。
- **多日趋势** — 采样连续几天，看 A5/C7 命中率是否稳定（稳定→系统性问题；波动→与业务操作相关）。
- **规则触发关联度** — 交叉分析 A5/C3/A12 是否命中同一批 application_no。同批命中说明是"一批字段漏写"，可一次修复。
