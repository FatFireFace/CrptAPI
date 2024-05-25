import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CrptApi {
    private final HttpClient httpClient;
    private final String apiUrl = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private final int requestLimit;
    private final Duration interval;
    private final BlockingQueue<Runnable> requestQueue;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private Instant windowStart;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.httpClient = HttpClient.newHttpClient();
        this.requestLimit = requestLimit;
        this.interval = Duration.ofMillis(timeUnit.toMillis(1));
        this.requestQueue = new LinkedBlockingQueue<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.windowStart = Instant.now();

        scheduler.scheduleAtFixedRate(this::resetRequestCount, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void createDocument(Document document, String signature) throws InterruptedException {
        requestQueue.put(() -> {
            try {
                lock.lock();
                while (true) {
                    Instant now = Instant.now();
                    if (requestCount.get() < requestLimit || Duration.between(windowStart, now).compareTo(interval) >= 0) {
                        if (Duration.between(windowStart, now).compareTo(interval) >= 0) {
                            windowStart = now;
                            requestCount.set(0);
                        }
                        requestCount.incrementAndGet();
                        break;
                    }
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }

            String requestBody;
            try {
                requestBody = createRequestBody(document);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(apiUrl))
                        .header("Content-Type", "application/json")
                        .header("Signature", signature)
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();

                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        Executors.newSingleThreadExecutor().execute(requestQueue.take());
    }

    private void resetRequestCount() {
        try {
            lock.lock();
            windowStart = Instant.now();
            requestCount.set(0);
        } finally {
            lock.unlock();
        }
    }

    private String createRequestBody(Document document) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(document);
    }

    public static class Document {
        public Description description;
        public String doc_id;
        public String doc_status;
        public String doc_type;
        public boolean importRequest;
        public String owner_inn;
        public String participant_inn;
        public String producer_inn;
        public String production_date;
        public String production_type;
        public Product[] products;
        public String reg_date;
        public String reg_number;

        public static class Description {
            public String participantInn;
        }

        public static class Product {
            public String certificate_document;
            public String certificate_document_date;
            public String certificate_document_number;
            public String owner_inn;
            public String producer_inn;
            public String production_date;
            public String tnved_code;
            public String uit_code;
            public String uitu_code;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CrptApi api = new CrptApi(TimeUnit.SECONDS, 5);

        Document document = new Document();
        // Заполните поля документа...

        api.createDocument(document, "ваша подпись");
    }
}
