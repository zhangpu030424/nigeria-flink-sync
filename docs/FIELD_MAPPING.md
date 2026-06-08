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

### UTM 查表（§3.1.1）

与 `AdjustCallbackQueryServiceImpl#getUtmByDeviceIds` 一致：

1. 从 `user` 取 `gps_adid`、`idfa`、`idfv`
2. `adjust_callback_record`：`WHERE gps_adid = ? OR idfa = ? OR idfv = ?`，`ORDER BY create_time DESC LIMIT 1`
3. 源库需先执行 `sql/ddl/source_views_adjust.sql` 建三视图，Flink JDBC Lookup 引用

**`mapUtmSource`：** 空/unattributed→NULL；organic；google；tiktok；facebook/instagram/messenger→facebook；kuai/kwai/kuaishou→kwai；否则原值小写。

实现：`sql/02_sync_user_test.sql` + Java `AdjustCallbackUtmAssembler#mapUtmSource` 同逻辑。

### mobile VT

1. SQL 内规范化：`8123456788` → `+2348123456788`
2. UDF `vt_tokenize()` POST `${VT_BASE_URL}/v2t`，见 `udf/VtTokenizeFunction.java`
