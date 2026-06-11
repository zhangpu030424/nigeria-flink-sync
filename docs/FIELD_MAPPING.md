# 字段映射摘要

完整版见 `nigeria-backend-api/docs/字段映射.md`。本文档与 Flink SQL 实现对齐。

## 3.1 `user` ← `user`

| 目标字段 | 源 / 表达式 | 说明 |
|---------|------------|------|
| `user_id` | `user.id + 100000000` | +1 亿 |
| `app_id` | `CAST(user.app_code AS INT)` | 文档写 `appcod`，源表列为 `app_code` |
| `group_user_id` | 同 `user_id` | |
| `info_user_id` | 同 `user_id` | |
| `mobile` | `vt_token_cache.token` | 全量读宽表 `mobile_token`；增量 Lookup `vt_token_cache`（Flink 不调 `/v2t`） |
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

### VT 字段（统一：两阶段全量 + 增量 Lookup/UDF 兜底）

| 表 | VT 字段 | 阶段 1（有 token） | 阶段 2（运行时 /v2t） | 增量 |
|----|---------|-------------------|------------------------|------|
| `user` | mobile | `02_sync_user_fast` | `02_sync_user_fast_vt_miss` | `02_sync_user_incr` |
| `user_info` | id_number | `02_sync_user_info_fast` | `02_sync_user_info_fast_vt_miss` | `02_sync_user_info_incr` |
| `user_bankcard` | bank_account | `02_sync_user_bankcard_fast` | `02_sync_user_bankcard_fast_vt_miss` | `02_sync_user_bankcard_incr` |
| `application` | mobile/id_number/bank/gaid | `02_sync_application_fast` | `02_sync_application_fast_vt_miss` | CDC 宽表（新单需重建宽表段） |

编排：`sync-all-auto.sh` / `sync-pipeline-auto.sh` → `sync-job-auto.sh` 对上述 4 表 **自动阶段 1→2→增量**。  
`user_product` / `loan` 无 VT，仍单阶段全量。  
可选 `vt-preload.sh` 扩大阶段 1、减少阶段 2 对 VT 接口压力。
