package com.nigeria.flink.job;

import com.nigeria.flink.udf.VtBatchRowProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * 全量 user 同步（宽表 CDC + 批量 VT 10 万条/次 + JDBC Sink）。
 * 提交: ./scripts/run-user-fast-vt.sh
 */
public class UserSyncFastJob {

    public static void main(String[] args) throws Exception {
        SyncEnv env = SyncEnv.load();

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(env.parallelism);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

        tEnv.executeSql(sourceDdl(env));
        tEnv.executeSql(sinkDdl(env));

        // CDC 3.1.1 对有主键的表输出 upsert changelog，toDataStream 无法消费；走 changelog 流并只保留 INSERT
        Table prepared = tEnv.sqlQuery(transformSql());
        DataStream<Row> insertsOnly = tEnv.toChangelogStream(prepared)
                .filter(row -> row.getKind() == RowKind.INSERT);
        DataStream<Row> tokenized = insertsOnly.process(new VtBatchRowProcessFunction());

        Schema outSchema = Schema.newBuilder()
                .column("user_id", DataTypes.BIGINT())
                .column("app_id", DataTypes.INT())
                .column("group_user_id", DataTypes.BIGINT())
                .column("info_user_id", DataTypes.BIGINT())
                .column("mobile", DataTypes.STRING())
                .column("closed_time", DataTypes.BIGINT())
                .column("reg_device_uuid", DataTypes.STRING())
                .column("reg_time", DataTypes.BIGINT())
                .column("test_flag", DataTypes.TINYINT())
                .column("utm_source", DataTypes.STRING())
                .column("utm_medium", DataTypes.STRING())
                .column("utm_campaign", DataTypes.STRING())
                .column("utm_content", DataTypes.STRING())
                .column("utm_term", DataTypes.STRING())
                .column("campaign_id", DataTypes.STRING())
                .column("ad_group_id", DataTypes.STRING())
                .column("advertiser_id", DataTypes.STRING())
                .build();

        tEnv.fromChangelogStream(tokenized, outSchema, ChangelogMode.insertOnly())
                .executeInsert("sink_user");
    }

    private static String sourceDdl(SyncEnv env) {
        return String.format("""
                CREATE TABLE src_user_staging (
                    id BIGINT,
                    app_code STRING,
                    mobile STRING,
                    device_id STRING,
                    create_time TIMESTAMP(3),
                    network_name STRING,
                    tracker_name STRING,
                    campaign_tracker STRING,
                    campaign_name STRING,
                    creative_name STRING,
                    adgroup_tracker STRING,
                    creative_tracker STRING,
                    adgroup_name STRING,
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'mysql-cdc',
                    'hostname' = '%s',
                    'port' = '%s',
                    'username' = '%s',
                    'password' = '%s',
                    'database-name' = '%s',
                    'table-name' = 'user_sync_staging',
                    'server-time-zone' = 'Africa/Lagos',
                    'scan.incremental.snapshot.chunk.size' = '%s',
                    'scan.snapshot.fetch.size' = '%s'
                )
                """,
                env.sourceHost, env.sourcePort, env.sourceUser, env.sourcePassword, env.sourceDb,
                env.cdcChunkSize, env.cdcFetchSize);
    }

    private static String sinkDdl(SyncEnv env) {
        return String.format("""
                CREATE TABLE sink_user (
                    user_id BIGINT,
                    app_id INT,
                    group_user_id BIGINT,
                    info_user_id BIGINT,
                    mobile STRING,
                    closed_time BIGINT,
                    reg_device_uuid STRING,
                    reg_time BIGINT,
                    test_flag TINYINT,
                    utm_source STRING,
                    utm_medium STRING,
                    utm_campaign STRING,
                    utm_content STRING,
                    utm_term STRING,
                    campaign_id STRING,
                    ad_group_id STRING,
                    advertiser_id STRING,
                    PRIMARY KEY (mobile, app_id, closed_time) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = 'jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
                    'table-name' = 'user',
                    'username' = '%s',
                    'password' = '%s',
                    'sink.buffer-flush.max-rows' = '%s',
                    'sink.buffer-flush.interval' = '500ms',
                    'sink.max-retries' = '3'
                )
                """,
                env.targetHost, env.targetPort, env.targetDb,
                env.targetUser, env.targetPassword, env.sinkBufferRows);
    }

    private static String transformSql() {
        return """
                SELECT
                    id + 100000000 AS user_id,
                    CAST(app_code AS INT) AS app_id,
                    id + 100000000 AS group_user_id,
                    id + 100000000 AS info_user_id,
                    CASE
                        WHEN mobile IS NULL OR TRIM(mobile) = '' THEN CAST(NULL AS STRING)
                        WHEN TRIM(mobile) LIKE '+%' THEN TRIM(mobile)
                        WHEN TRIM(mobile) LIKE '234%' THEN CONCAT('+', TRIM(mobile))
                        WHEN TRIM(mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(mobile), 2))
                        ELSE CONCAT('+234', TRIM(mobile))
                    END AS mobile_plain,
                    CAST(0 AS BIGINT) AS closed_time,
                    COALESCE(device_id, '') AS reg_device_uuid,
                    UNIX_TIMESTAMP(DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS reg_time,
                    CAST(0 AS TINYINT) AS test_flag,
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), '')) IS NULL
                            OR TRIM(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) = ''
                            THEN CAST(NULL AS STRING)
                        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%unattributed%%'
                            THEN CAST(NULL AS STRING)
                        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%organic%%'
                            THEN 'organic'
                        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%google%%'
                            THEN 'google'
                        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%tiktok%%'
                            THEN 'tiktok'
                        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%facebook%%'
                            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%instagram%%'
                            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%messenger%%'
                            THEN 'facebook'
                        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%kuai%%'
                            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%kwai%%'
                            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%%kuaishou%%'
                            THEN 'kwai'
                        ELSE LOWER(TRIM(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))))
                    END AS utm_source,
                    campaign_tracker AS utm_medium,
                    campaign_name AS utm_campaign,
                    creative_name AS utm_content,
                    adgroup_tracker AS utm_term,
                    campaign_tracker AS campaign_id,
                    adgroup_tracker AS ad_group_id,
                    adgroup_tracker AS advertiser_id
                FROM src_user_staging
                """;
    }
}
