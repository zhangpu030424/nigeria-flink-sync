# 字段映射摘要

完整版见 `nigeria-backend-api/docs/字段映射.md`。本文档与 Flink SQL 实现对齐。

## 3.1 `user` ← `user`

| 目标字段 | 源 / 表达式 | 说明 |
|---------|------------|------|
| `user_id` | `user.id + 100000000` | +1 亿 |
| `app_id` | `CAST(user.app_code AS INT)` | 文档写 `appcod`，源表列为 `app_code` |
| `group_user_id` | 同 `user_id` | |
| `info_user_id` | 同 `user_id` | |
| `mobile` | `user.mobile` | 规范化为 `+234...` 后调用 VT `/v2t` 得 token |
| `closed_time` | `0` | 固定 |
| `reg_device_uuid` | `user.device_id` | |
| `reg_time` | `UNIX_TIMESTAMP(create_time)*1000` | 毫秒 |
| `test_flag` | `0` | 默认；GP/测试号识别待接 `google_review_account_config` |
| `utm_source` | adjust `network_name` → `tracker_name` | `mapUtmSource()`，见下 |
| `utm_medium` | `campaign_tracker` | |
| `utm_campaign` | `campaign_name` | |
| `utm_content` | `creative_name` | |
| `utm_term` | `adgroup_tracker` | |
| `campaign_id` | `creative_tracker` | 对应风控 `utm_id` |
| `ad_group_id` | `campaign_tracker` | |
| `advertiser_id` | `adgroup_tracker` | |
| `created_at` / `updated_at` | — | **不传**，由目标库 DEFAULT / ON UPDATE 自动生成 |

### UTM 查表

**关联条件（已确认）：** `adjust_callback_record.adid = user.adid`，取该 `adid` 最新一条回调。

源库视图 `v_adjust_latest_by_adid`，物化表 `adjust_latest_by_adid`（Flink `dim_user_adjust` Lookup 按 `adid` Join）。

```sql
-- 源库逻辑等价于
SELECT * FROM adjust_callback_record acr
INNER JOIN user u ON acr.adid = u.adid
-- 每 adid 取 MAX(id) 最新一条，见 sql/ddl/source_views_adjust.sql
```

**`mapUtmSource`：** 空/unattributed→NULL；organic；google；tiktok；facebook/instagram/messenger→facebook；kuai/kwai/kuaishou→kwai；否则原值小写。

实现：`sql/02_sync_user_test.sql` + Java `AdjustCallbackUtmAssembler#mapUtmSource` 同逻辑。

### mobile VT

1. SQL 内规范化：`8123456788` → `+2348123456788`
2. UDF `vt_tokenize()` POST `${VT_BASE_URL}/v2t`，见 `udf/VtTokenizeFunction.java`
