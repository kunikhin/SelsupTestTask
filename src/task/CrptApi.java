package task;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CrptApi {

    private final long refillPeriodNanos;
    private final int capacity;
    private int tokens;
    private long lastRefillNanos;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final HttpClient httpClient;

    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit <= 0) {
            throw new IllegalArgumentException("Request limit must be positive");
        }

        this.capacity = requestLimit;
        this.tokens = requestLimit;
        this.refillPeriodNanos = timeUnit.toNanos(1);
        this.lastRefillNanos = System.nanoTime();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public void createDocument(Document document, String signature) {
        try {
            lock.lock();
            try {
                refillTokens();
                while (tokens <= 0) {
                    condition.awaitNanos(calculateWaitNanos());
                    refillTokens();
                }
                tokens--;
            } finally {
                lock.unlock();
            }

            sendHttpRequest(document.toJson(), signature);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiException("Operation interrupted", e);
        } catch (Exception e) {
            throw new ApiException("Failed to create document", e);
        }
    }

    private void refillTokens() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillNanos;

        if (elapsedNanos > refillPeriodNanos) {
            tokens = capacity;
            lastRefillNanos = now;
            condition.signalAll();
        }
    }

    private long calculateWaitNanos() {
        return refillPeriodNanos - (System.nanoTime() - lastRefillNanos);
    }

    private void sendHttpRequest(String jsonBody, String signature) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(API_URL))
                .header("Content-Type", "application/json")
                .header("Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        HttpResponse<String> response = httpClient.send(
                request,
                HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() != 200) {
            throw new ApiException("API error: " + response.body());
        }
    }

    public static class Document implements JsonSerializable {
        private Description description;
        private String doc_id;
        private String doc_status;
        private String doc_type;
        private String importRequest;
        private String owner_inn;
        private String participant_inn;
        private String producer_inn;
        private String production_date;
        private String production_type;
        private Product[] products;
        private String reg_date;
        private String reg_number;

        @Override
        public String toJson() {
            JsonBuilder builder = new JsonBuilder()
                    .addField("description", description)
                    .addField("doc_id", doc_id)
                    .addField("doc_status", doc_status)
                    .addField("doc_type", doc_type)
                    .addField("importRequest", importRequest)
                    .addField("owner_inn", owner_inn)
                    .addField("participant_inn", participant_inn)
                    .addField("producer_inn", producer_inn)
                    .addField("production_date", production_date)
                    .addField("production_type", production_type)
                    .addField("products", products)
                    .addField("reg_date", reg_date)
                    .addField("reg_number", reg_number);
            return builder.build();
        }
    }

    public static class Description implements JsonSerializable {
        private String participantInn;

        @Override
        public String toJson() {
            return new JsonBuilder()
                    .addField("participantInn", participantInn)
                    .build();
        }
    }

    public static class Product implements JsonSerializable {
        private String certificate_document;
        private String certificate_document_date;
        private String certificate_document_number;
        private String owner_inn;
        private String producer_inn;
        private String production_date;
        private String tnved_code;
        private String uit_code;
        private String uitu_code;

        @Override
        public String toJson() {
            return new JsonBuilder()
                    .addField("certificate_document", certificate_document)
                    .addField("certificate_document_date", certificate_document_date)
                    .addField("certificate_document_number", certificate_document_number)
                    .addField("owner_inn", owner_inn)
                    .addField("producer_inn", producer_inn)
                    .addField("production_date", production_date)
                    .addField("tnved_code", tnved_code)
                    .addField("uit_code", uit_code)
                    .addField("uitu_code", uitu_code)
                    .build();
        }
    }

    private static class JsonBuilder {
        private final StringBuilder sb = new StringBuilder();
        private boolean hasFields = false;

        public JsonBuilder() {
            sb.append("{");
        }

        public JsonBuilder addField(String name, String value) {
            if (value == null) return this;
            addCommaIfNeeded();
            sb.append("\"").append(name).append("\":")
                    .append("\"").append(escapeJson(value)).append("\"");
            return this;
        }

        public JsonBuilder addField(String name, JsonSerializable value) {
            if (value == null) return this;
            addCommaIfNeeded();
            sb.append("\"").append(name).append("\":")
                    .append(value.toJson());
            return this;
        }

        public JsonBuilder addField(String name, JsonSerializable[] values) {
            if (values == null) return this;
            addCommaIfNeeded();
            sb.append("\"").append(name).append("\":[");
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(values[i].toJson());
            }
            sb.append("]");
            return this;
        }

        public String build() {
            sb.append("}");
            return sb.toString();
        }

        private void addCommaIfNeeded() {
            if (hasFields) {
                sb.append(",");
            } else {
                hasFields = true;
            }
        }

        private String escapeJson(String s) {
            if (s == null) return null;
            return s.replace("\"", "\\\"");
        }
    }

    private interface JsonSerializable {
        String toJson();
    }

    public static class ApiException extends RuntimeException {
        public ApiException(String message) { super(message); }
        public ApiException(String message, Throwable cause) { super(message, cause); }
    }
}