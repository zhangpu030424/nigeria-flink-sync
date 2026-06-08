package com.nigeria.flink.udf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * DataStream 攒批 VT：默认 10 万条/次 POST /v2t；不足一批时由 processing-time 定时器刷尾批。
 * mobile_plain 固定在第 5 列（index=4）。
 */
public class VtBatchRowProcessFunction extends ProcessFunction<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(VtBatchRowProcessFunction.class);
    public static final int MOBILE_PLAIN_INDEX = 4;

    private transient VtBatchClient client;
    private transient List<Row> buffer;
    private transient int batchSize;
    private transient long flushIntervalMs;
    private transient long pendingTimerTs;

    @Override
    public void open(Configuration parameters) {
        String baseUrl = System.getenv().getOrDefault("VT_BASE_URL", "http://101.47.27.225");
        int maxRetries = parseIntEnv("VT_BATCH_MAX_RETRIES", 3);
        int timeoutSec = parseIntEnv("VT_BATCH_TIMEOUT_SEC", 300);
        batchSize = parseIntEnv("VT_BATCH_SIZE", 100_000);
        flushIntervalMs = parseLongEnv("VT_BATCH_FLUSH_MS", 5_000L);
        client = new VtBatchClient(baseUrl, maxRetries, Duration.ofSeconds(timeoutSec));
        buffer = new ArrayList<>(Math.min(batchSize, 4096));
        pendingTimerTs = -1L;
        LOG.info("VtBatchRowProcessFunction open: batchSize={}, flushIntervalMs={}", batchSize, flushIntervalMs);
    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
        buffer.add(value);
        if (buffer.size() == 1) {
            long fireAt = ctx.timerService().currentProcessingTime() + flushIntervalMs;
            pendingTimerTs = fireAt;
            ctx.timerService().registerProcessingTimeTimer(fireAt);
        }
        if (buffer.size() >= batchSize) {
            flush(ctx, out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
        if (pendingTimerTs == timestamp && !buffer.isEmpty()) {
            LOG.info("VT batch timer flush, buffered={}", buffer.size());
            flushAndCollect(out);
        }
    }

    private void flush(Context ctx, Collector<Row> out) {
        if (pendingTimerTs > 0) {
            ctx.timerService().deleteProcessingTimeTimer(pendingTimerTs);
            pendingTimerTs = -1L;
        }
        flushAndCollect(out);
    }

    private void flushAndCollect(Collector<Row> out) {
        if (buffer.isEmpty()) {
            return;
        }
        List<String> plainMobiles = new ArrayList<>(buffer.size());
        for (Row row : buffer) {
            Object mobile = row.getField(MOBILE_PLAIN_INDEX);
            plainMobiles.add(mobile == null ? null : mobile.toString());
        }

        long t0 = System.currentTimeMillis();
        List<String> tokens = client.tokenizeBatch(plainMobiles);
        long costMs = System.currentTimeMillis() - t0;
        LOG.info("VT /v2t batch done: rows={}, costMs={}", plainMobiles.size(), costMs);

        for (int i = 0; i < buffer.size(); i++) {
            Row in = buffer.get(i);
            Object[] fields = new Object[in.getArity()];
            for (int j = 0; j < in.getArity(); j++) {
                fields[j] = in.getField(j);
            }
            fields[MOBILE_PLAIN_INDEX] = tokens.get(i);
            out.collect(Row.ofKind(RowKind.INSERT, fields));
        }
        buffer.clear();
        pendingTimerTs = -1L;
    }

    private static int parseIntEnv(String key, int defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long parseLongEnv(String key, long defaultValue) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
