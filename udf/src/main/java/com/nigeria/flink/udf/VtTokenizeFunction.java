package com.nigeria.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 调用 VT Service POST /v2t，将规范化后的 mobile 转为 token。
 * 环境变量 VT_BASE_URL（TaskManager 容器内需可访问）。
 */
public class VtTokenizeFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(VtTokenizeFunction.class);
    private static final Pattern TOKEN_PATTERN = Pattern.compile("\"tokens\"\\s*:\\s*\\[\\s*\"([^\"]+)\"");
    private static final int MAX_RETRIES = 3;

    private transient HttpClient httpClient;
    private transient String baseUrl;

    @Override
    public void open(org.apache.flink.table.functions.FunctionContext context) {
        baseUrl = System.getenv().getOrDefault("VT_BASE_URL", "http://101.47.23.241:9505");
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        LOG.info("VtTokenizeFunction initialized, VT_BASE_URL={}", baseUrl);
    }

    public String eval(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return null;
        }

        String lastError = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                String token = callVt(value);
                if (token != null && !token.isEmpty()) {
                    return token;
                }
                lastError = "empty token in response";
            } catch (Exception e) {
                lastError = e.getMessage();
                LOG.warn("VT /v2t attempt {}/{} failed for {}: {}", attempt, MAX_RETRIES, mask(value), lastError);
            }
            if (attempt < MAX_RETRIES) {
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        String msg = String.format("VT /v2t failed after %d retries (%s), mobile=%s, url=%s/v2t",
                MAX_RETRIES, lastError, mask(value), baseUrl);
        LOG.error(msg);
        throw new RuntimeException(msg);
    }

    private String callVt(String value) throws Exception {
        String body = "[\"" + escapeJson(value) + "\"]";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v2t"))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode() + " body=" + truncate(response.body(), 200));
        }
        Matcher matcher = TOKEN_PATTERN.matcher(response.body());
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new RuntimeException("no token in response: " + truncate(response.body(), 200));
    }

    private static String mask(String mobile) {
        if (mobile == null || mobile.length() < 8) {
            return "***";
        }
        return mobile.substring(0, Math.min(5, mobile.length())) + "****"
                + mobile.substring(mobile.length() - 4);
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
