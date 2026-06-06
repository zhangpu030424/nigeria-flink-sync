# 字段映射要点

完整版见 `nigeria-backend-api/docs/字段映射.md`。以下为同步 Job 开发时的关键口径。

## ID 偏移

| 字段 | 规则 |
|------|------|
| `user_id` | 源 `id + 100000000` |
| `info_user_id` | 同 `user_id` |
| `group_user_id` | 源 `id`（**不加**偏移） |

## 目标表（7 张）

`user`, `user_info`, `user_bankcard`, `user_product`, `application`, `loan`, `id_mapping`

DDL：`docs/schema/Target.sql`

## user 表（阶段 B 已实现部分）

| 目标字段 | 源字段 / 逻辑 |
|----------|----------------|
| user_id | id + offset |
| app_id | app_config.id（JOIN app_code） |
| group_user_id | id |
| mobile | mobile（正式需 VT 加密，见下） |
| reg_device_uuid | device_id |
| reg_time | UNIX_TIMESTAMP(create_time) * 1000 |
| closed_time | 0 |
| test_flag | 0 |

## 待扩展

- **mobile / id_number 等**：调用 VT 服务 `http://101.47.23.241:9505/v2t`（Flink UDF）
- **UTM 归因**：源表 `adjust_callback_record`，按 `gps_adid/idfa/idfv` 取最新 `create_time`（与 `AdjustCallbackUtmAssembler` 一致）
- **application_no / sn / loan_no**：待业务确认生成规则
- **金额字段**：是否 ×100 待确认

## 时区

源库与 Flink 均使用 `Africa/Lagos`（`table.local-time-zone`）。
