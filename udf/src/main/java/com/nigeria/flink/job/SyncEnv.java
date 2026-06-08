package com.nigeria.flink.job;

final class SyncEnv {

    final String sourceHost;
    final String sourcePort;
    final String sourceUser;
    final String sourcePassword;
    final String sourceDb;
    final String targetHost;
    final String targetPort;
    final String targetUser;
    final String targetPassword;
    final String targetDb;
    final int parallelism;
    final String cdcChunkSize;
    final String cdcFetchSize;
    final String sinkBufferRows;

    private SyncEnv(
            String sourceHost,
            String sourcePort,
            String sourceUser,
            String sourcePassword,
            String sourceDb,
            String targetHost,
            String targetPort,
            String targetUser,
            String targetPassword,
            String targetDb,
            int parallelism,
            String cdcChunkSize,
            String cdcFetchSize,
            String sinkBufferRows) {
        this.sourceHost = sourceHost;
        this.sourcePort = sourcePort;
        this.sourceUser = sourceUser;
        this.sourcePassword = sourcePassword;
        this.sourceDb = sourceDb;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.targetUser = targetUser;
        this.targetPassword = targetPassword;
        this.targetDb = targetDb;
        this.parallelism = parallelism;
        this.cdcChunkSize = cdcChunkSize;
        this.cdcFetchSize = cdcFetchSize;
        this.sinkBufferRows = sinkBufferRows;
    }

    static SyncEnv load() {
        return new SyncEnv(
                required("SOURCE_MYSQL_HOST"),
                env("SOURCE_MYSQL_PORT", "3306"),
                required("SOURCE_MYSQL_USER"),
                required("SOURCE_MYSQL_PASSWORD"),
                required("SOURCE_MYSQL_DATABASE"),
                required("TARGET_MYSQL_HOST"),
                env("TARGET_MYSQL_PORT", "3306"),
                required("TARGET_MYSQL_USER"),
                required("TARGET_MYSQL_PASSWORD"),
                required("TARGET_MYSQL_DATABASE"),
                Integer.parseInt(env("FLINK_PARALLELISM", "8")),
                env("FLINK_CDC_CHUNK_SIZE", "100000"),
                env("FLINK_CDC_FETCH_SIZE", "10000"),
                env("FLINK_SINK_BUFFER_ROWS", "10000"));
    }

    private static String required(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing env: " + key);
        }
        return value.trim();
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }
}
