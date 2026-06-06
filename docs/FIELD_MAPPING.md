# 字段映射要点

完整版见 `nigeria-backend-api/docs/字段映射.md`。以下为同步 Job 开发时的关键口径。

## 已确认规则

| 项 | 规则 |
|----|------|
| `user_id` | 源 `id + 100000000` |
| `info_user_id` | 同 `user_id` |
| `group_user_id` | **同 `user_id`**（源 `id + 100000000`） |
| **mobile** | **本期不走 VT**，源库 `user.mobile` 直传目标库 |
| **金额** | **不 ×100**，目标 `bigint` 存**整奈拉**（与源 VARCHAR 数值一致，去小数后 CAST） |
| **UTM** | 与风控请求体 `device.utm` 一致，见下文 |

## 目标表（7 张）

`user`, `user_info`, `user_bankcard`, `user_product`, `application`, `loan`, `id_mapping`

DDL：`docs/schema/Target.sql`

## UTM（与风控 body 一致）

代码参考：`AdjustCallbackUtmAssembler` + `AdjustCallbackQueryServiceImpl#getUtmByDeviceIds`（`nigeria-risk` → `enrichDeviceWithUtm`）。

### 关联方式

1. 从 `user` 取设备 ID：`gps_adid`（→ aaid）、`idfa`、`idfv`
2. 查 `adjust_callback_record`：`gps_adid = aaid OR idfa OR idfv`，`ORDER BY create_time DESC LIMIT 1`
3. 用 `AdjustCallbackUtmAssembler.fromRecord` 规则组装

### 风控 `device.utm` → 目标 `user` 字段

| 风控 utm key | 源列（adjust_callback_record） | 目标 user 字段 | 说明 |
|--------------|-------------------------------|----------------|------|
| `utm_source` | `network_name`，空则 `tracker_name` | `utm_source` | 经 `mapUtmSource()` 规范化 |
| `utm_medium` | `campaign_tracker` | `utm_medium` | |
| `utm_campaign` | `campaign_name` | `utm_campaign` | |
| `utm_content` | `creative_name` | `utm_content` | |
| `utm_term` | `adgroup_tracker` | `utm_term` | |
| `utm_id` | `creative_tracker` | `campaign_id` | |
| `utm_group_id` | `adgroup_name` | `ad_group_id` | |
| — | `network_name` / `tracker_name` **原值** | `advertiser_id` | 不做 mapUtmSource |

### `mapUtmSource` 规则（Flink SQL 需复刻）

- 空 / `unattributed` → NULL
- 含 `organic` → `organic`
- 含 `google` → `google`
- 含 `tiktok` → `tiktok`
- 含 `facebook` / `instagram` / `messenger` → `facebook`
- 含 `kuai` / `kwai` / `kuaishou` → `kwai`
- 其他 → 原值转小写

## user 表（阶段 B 已实现部分）

| 目标字段 | 源字段 / 逻辑 |
|----------|----------------|
| user_id | id + offset |
| app_id | app_config.id（JOIN app_code） |
| group_user_id | id + offset（同 user_id） |
| mobile | **user.mobile 直传（不走 VT）** |
| reg_device_uuid | device_id |
| reg_time | UNIX_TIMESTAMP(create_time) * 1000 |
| closed_time | 0 |
| test_flag | 0 |
| utm_* / campaign_id / ad_group_id / advertiser_id | 见上节 UTM |

## 金额（不 ×100）

```sql
-- Flink SQL 示例：源 VARCHAR "35000.00" → 目标 35000
CAST(ROUND(CAST(NULLIF(TRIM(src_amount), '') AS DECIMAL(20,2)), 0) AS BIGINT)
```

| 源示例 | 目标 |
|--------|------|
| `"35000.00"` | `35000` |
| `NULL` / `''` | `0` |

## 仍待扩展 / 确认

- **其他敏感字段 VT**：`id_number`、`bank_account`、`gaid_idfa` 等（mobile 本期除外）
- **application_no / sn / loan_no**：生成规则
- **Flink UDF**：`mapUtmSource` 可在 SQL CASE 近似，复杂规则建议 Java UDF

## 时区

源库与 Flink 均使用 `Africa/Lagos`（`table.local-time-zone`）。
