package com.nigeria.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

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
 * 环境变量 VT_BASE_URL，默认 http://101.47.23.241:9505
 */
public class VtTokenizeFunction extends ScalarFunction {

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\"tokens\"\\s*:\\s*\\[\\s*\"([^\"]+)\"");

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
    }

    public String eval(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return null;
        }
        try {
            String body = "[\"" + escapeJson(value) + "\"]";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/v2t"))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return null;
            }
            Matcher matcher = TOKEN_PATTERN.matcher(response.body());
            if (matcher.find()) {
                return matcher.group(1);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
