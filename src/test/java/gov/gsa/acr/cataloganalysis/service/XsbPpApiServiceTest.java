package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.PpApiConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@MockBeans({@MockBean(PpApiConfig.class)})
@ContextConfiguration(classes = {XsbPpApiService.class})
@TestPropertySource(properties = { "spring.http.client.connect-timeout=5000", "spring.http.client.connect-timeout=3s" })
class XsbPpApiServiceTest {

   public static MockWebServer mockBackEnd;
    @Autowired
    private XsbPpApiService xsbPpApiService;
    @Autowired
    private PpApiConfig apiConfig;

    @BeforeAll
    static void setUp() throws IOException {
        mockBackEnd = new MockWebServer();
        mockBackEnd.start();
    }

    @AfterAll
    static void tearDown() throws IOException {
        mockBackEnd.shutdown();
    }

    @BeforeEach
    void setUpBeforeEach(){
        when(apiConfig.getHostport()).thenReturn(String.format("http://localhost:%s", mockBackEnd.getPort()));
        when(apiConfig.getAuthUrl()).thenReturn("/api/auth");
        when(apiConfig.getStatsUrl()).thenReturn("/api/catalog/stats");
        when(apiConfig.getUsername()).thenReturn("ACR");
        when(apiConfig.getPassword()).thenReturn("fake");
    }


    @Test
    void getAuthToken() throws IOException {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                log.info("getAuthToken {}", request.getPath());
                switch (request.getPath()) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/products":
                        return new MockResponse().setResponseCode(200).setBody("{\"products\": [{\"id\": 101, \"name\": \"Laptop\"}]}");
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getAuthToken()).expectNext("anAccessToken").verifyComplete();
    }

    @Test
    void getAuthToken_ServerError() throws IOException {
        AtomicInteger count = new AtomicInteger(0);

        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                log.info("getAuthToken_ServerError {}", request.getPath());
                switch (request.getPath()) {
                    case "/api/auth":
                        count.incrementAndGet();
                        return new MockResponse()
                                .setResponseCode(500) // Set the HTTP status code (e.g., Internal Server Error)
                                .setHeader("Content-Type", "application/json") // Optional: set content type
                                .setBody("{\"errorMessage\": \"Something went wrong on the server.\"}"); // Optional: set error body
                    default:
                        return new MockResponse().setResponseCode(401).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getAuthToken()).expectError().verify(Duration.ofSeconds(120));
        assertEquals(7, count.get());
    }



    @Test
    void getAuthToken_ServerErrorSuccesOn4th() throws IOException {
        AtomicInteger count = new AtomicInteger(0);
        String token = """
                {
                "access_token": "SuccessAfterFewTries",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                log.info("getAuthToken_ServerError {}", request.getPath());
                switch (request.getPath()) {
                    case "/api/auth":
                        count.incrementAndGet();
                        if (count.get() < 4)
                            return new MockResponse()
                                .setResponseCode(500) // Set the HTTP status code (e.g., Internal Server Error)
                                .setHeader("Content-Type", "application/json") // Optional: set content type
                                .setBody("{\"errorMessage\": \"Something went wrong on the server.\"}"); // Optional: set error body
                        else
                            return new MockResponse()
                                    .setResponseCode(200)
                                    .addHeader("Content-Type", "application/json; charset=utf-8")
                                    .setBody(token);
                    default:
                        return new MockResponse().setResponseCode(401).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getAuthToken()).expectNext("SuccessAfterFewTries").expectComplete().verify(Duration.ofSeconds(120));
        assertEquals(4, count.get());
    }


    @Test
    void getAuthToken_AuthError() throws IOException {
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                log.info("getAuthToken_ServerError {}", request.getPath());
                switch (request.getPath()) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(401) // Set the HTTP status code (e.g., Internal Server Error)
                                .setHeader("Content-Type", "application/json") // Optional: set content type
                                .setBody("{\"errorMessage\": \"Something went wrong on the server.\"}"); // Optional: set error body
                    default:
                        return new MockResponse().setResponseCode(401).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getAuthToken()).expectError().verify();
    }

    @Test
    void getAuthToken_600Error() throws IOException {
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                log.info("getAuthToken_ServerError {}", request.getPath());
                switch (request.getPath()) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(601) // Set the HTTP status code (e.g., Internal Server Error)
                                .setHeader("Content-Type", "application/json") // Optional: set content type
                                .setBody("{\"errorMessage\": \"Something went wrong on the server.\"}"); // Optional: set error body
                    default:
                        return new MockResponse().setResponseCode(401).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getAuthToken()).expectError().verify();
    }

    @Test
    void getLatestXsbStats() {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                HttpUrl url = request.getRequestUrl();
                String path = url.encodedPath();
                String param1 = url.queryParameter("sort-by");
                String param2 = url.queryParameter("sort-order");
                String param3 = url.queryParameter("filters");
                String param4 = url.queryParameter("start");
                String param5 = url.queryParameter("limit");
                log.info("getGsaFeedDate {}, {}, {}, {}, {}, {}", path, param1, param2, param3, param4, param5);
                switch (path) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/catalog/stats":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(gsaFeedDateTestData());
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getLatestXsbStats("2024-12-31")).expectNextCount(5).verifyComplete();
    }

    @Test
    void getGsaFeedDate() {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                HttpUrl url = request.getRequestUrl();
                String path = url.encodedPath();
                String param1 = url.queryParameter("sort-by");
                String param2 = url.queryParameter("sort-order");
                String param3 = url.queryParameter("filters");
                String param4 = url.queryParameter("start");
                String param5 = url.queryParameter("limit");
                log.info("getGsaFeedDate {}, {}, {}, {}, {}, {}", path, param1, param2, param3, param4, param5);
                switch (path) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/catalog/stats":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(gsaFeedDateTestData());
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getGsaFeedDate("2024-12-31")).expectNext("2025-08-15").verifyComplete();
    }

    @Test
    void getGsaFeedDate2() {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                HttpUrl url = request.getRequestUrl();
                String path = url.encodedPath();
                String param1 = url.queryParameter("sort-by");
                String param2 = url.queryParameter("sort-order");
                String param3 = url.queryParameter("filters");
                String param4 = url.queryParameter("start");
                String param5 = url.queryParameter("limit");
                log.info("getGsaFeedDate {}, {}, {}, {}, {}, {}", path, param1, param2, param3, param4, param5);
                switch (path) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/catalog/stats":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(gsaFeedDateTestDataStructure2());
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getGsaFeedDate("2024-12-31")).expectNext("2025-08-20").verifyComplete();
    }


    @Test
    void getGsaFeedDate3() {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                HttpUrl url = request.getRequestUrl();
                String path = url.encodedPath();
                String param1 = url.queryParameter("sort-by");
                String param2 = url.queryParameter("sort-order");
                String param3 = url.queryParameter("filters");
                String param4 = url.queryParameter("start");
                String param5 = url.queryParameter("limit");
                log.info("getGsaFeedDate {}, {}, {}, {}, {}, {}", path, param1, param2, param3, param4, param5);
                switch (path) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/catalog/stats":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(gsaFeedDateTestDataStructure3());
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getGsaFeedDate("2024-12-31")).expectNext("2025-09-01").verifyComplete();
    }

    @Test
    void getGsaFeedDate4() {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                HttpUrl url = request.getRequestUrl();
                String path = url.encodedPath();
                String param1 = url.queryParameter("sort-by");
                String param2 = url.queryParameter("sort-order");
                String param3 = url.queryParameter("filters");
                String param4 = url.queryParameter("start");
                String param5 = url.queryParameter("limit");
                log.info("getGsaFeedDate {}, {}, {}, {}, {}, {}", path, param1, param2, param3, param4, param5);
                switch (path) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/catalog/stats":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(gsaFeedDateTestDataEmpty());
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getGsaFeedDate("2024-12-31")).verifyComplete();
    }


    @Test
    void getGsaFeedDate5() {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                HttpUrl url = request.getRequestUrl();
                String path = url.encodedPath();
                String param1 = url.queryParameter("sort-by");
                String param2 = url.queryParameter("sort-order");
                String param3 = url.queryParameter("filters");
                String param4 = url.queryParameter("start");
                String param5 = url.queryParameter("limit");
                log.info("getGsaFeedDate {}, {}, {}, {}, {}, {}", path, param1, param2, param3, param4, param5);
                switch (path) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/catalog/stats":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody("");
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getGsaFeedDate("2024-12-31")).verifyComplete();
    }

    @Test
    void getGsaFeedDateInvalidInputDate() {
        String token = """
                {
                "access_token": "anAccessToken",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                HttpUrl url = request.getRequestUrl();
                String path = url.encodedPath();
                String param1 = url.queryParameter("sort-by");
                String param2 = url.queryParameter("sort-order");
                String param3 = url.queryParameter("filters");
                String param4 = url.queryParameter("start");
                String param5 = url.queryParameter("limit");
                log.info("getGsaFeedDate {}, {}, {}, {}, {}, {}", path, param1, param2, param3, param4, param5);
                switch (path) {
                    case "/api/auth":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/catalog/stats":
                        return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(gsaFeedDateTestData());
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getGsaFeedDate(null).block();
        }, "Should throw IllegalArgumentException for invalid date format");

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getGsaFeedDate("").block();
        }, "Should throw IllegalArgumentException for invalid date format");

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getGsaFeedDate("2025-13-01").block();
        }, "Should throw IllegalArgumentException for invalid date format");

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getGsaFeedDate("invalid").block();
        }, "Should throw IllegalArgumentException for invalid date format");

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getLatestXsbStats(null).subscribe();
        }, "Should throw IllegalArgumentException for invalid date format");

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getLatestXsbStats("").subscribe();
        }, "Should throw IllegalArgumentException for invalid date format");

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getLatestXsbStats("2025-13-01").subscribe();
        }, "Should throw IllegalArgumentException for invalid date format");

        assertThrows(IllegalArgumentException.class, () -> {
            xsbPpApiService.getLatestXsbStats("invalid").subscribe();
        }, "Should throw IllegalArgumentException for invalid date format");

    }


    @Test
    void getAuthToken_ConnectionProblem() throws IOException {
        AtomicInteger count = new AtomicInteger(0);
        String token = """
                {
                "access_token": "SuccessAfterFewTries",
                "expires_in": "never"
                }
                """;
        var dispatcher = new Dispatcher() {
            @SneakyThrows
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                log.info("getAuthToken {}", request.getPath());
                switch (request.getPath()) {
                    case "/api/auth":
                        if (count.get() < 4){
                            count.incrementAndGet();
                            return new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST);
                        }
                        else return new MockResponse()
                                .setResponseCode(200)
                                .addHeader("Content-Type", "application/json; charset=utf-8")
                                .setBody(token);
                    case "/api/products":
                        return new MockResponse().setResponseCode(200).setBody("{\"products\": [{\"id\": 101, \"name\": \"Laptop\"}]}");
                    default:
                        return new MockResponse().setResponseCode(404).setBody("Not Found");
                }
            }
        };

        mockBackEnd.setDispatcher(dispatcher);
        StepVerifier.create(xsbPpApiService.getAuthToken()).expectNext("SuccessAfterFewTries").expectComplete().verify(Duration.ofSeconds(120));
        assertEquals(4, count.get());
    }


    /**
     * Placeholder method that simulates calling an external API endpoint.
     * In production, this would make an actual HTTP call to retrieve GSA feed data.
     *
     * This method demonstrates different JSON structures to show the generic parsing capability.
     * The determineGsaFeedDate method will work with any of these structures.
     *
     * @return JSON string representing the GSA feed data array
     */
    private String gsaFeedDateTestData() {
        // This is a placeholder implementation
        // In production, this would make an actual HTTP call to the external API

        // You can uncomment different JSON structures to test the generic parsing:

        // Structure 1: Original structure with 2 objects
        return """
            [
                {
                    "id": 175619,
                    "completedPercent": 100.0,
                    "gsaFeedDate": "2025-07-02T00:00:00.000-04:00",
                    "priority": "MEDIUM",
                    "user": {
                        "email": "acrops@gsa.gov",
                        "username": "ACR",
                        "isSftpUser": true,
                        "maxPriority": "HIGH"
                    },
                    "completedDate": "2025-07-27T22:27:08.110-04:00",
                    "recordCount": 2003,
                    "createdDate": "2025-07-27T17:53:50.000-04:00",
                    "uploadFileName": "47QSEA21D0051-3159008_20250727215334.gsa",
                    "complete": true,
                    "reportFileCount": 1,
                    "reportFileNames": [
                        "47QSEA21D0051-3159008_20250727215334_810684885778227873_report_1.gsa"
                    ],
                    "uploadStatus": "SUCCESS",
                    "uploadErrorDescription": null,
                    "submittingSystemMode": null,
                    "submittingId": null,
                    "submittingSystem": null,
                    "skipGreenInferences": false,
                    "skipGreenInferencesDate": null
                },
                {
                    "id": 175620,
                    "completedPercent": 100.0,
                    "gsaFeedDate": "2025-06-01T00:00:00.000-04:00",
                    "priority": "MEDIUM",
                    "user": {
                        "email": "acrops@gsa.gov",
                        "username": "ACR",
                        "isSftpUser": true,
                        "maxPriority": "HIGH"
                    },
                    "completedDate": "2025-07-28T22:27:08.110-04:00",
                    "recordCount": 200,
                    "createdDate": "2025-07-27T17:53:50.000-04:00",
                    "uploadFileName": "47QSEA21D0051-3159008_20250727215335.gsa",
                    "complete": true,
                    "reportFileCount": 1,
                    "reportFileNames": [
                        "47QSEA21D0051-3159008_20250727215335_810684885778227873_report_1.gsa"
                    ],
                    "uploadStatus": "SUCCESS",
                    "uploadErrorDescription": null,
                    "submittingSystemMode": null,
                    "submittingId": null,
                    "submittingSystem": null,
                    "skipGreenInferences": false,
                    "skipGreenInferencesDate": null
                },
                {
                    "id": 175620,
                    "completedPercent": 100.0,
                    "gsaFeedDate": "2025-08-15T00:00:00.000-04:00",
                    "priority": "MEDIUM",
                    "user": {
                        "email": "acrops@gsa.gov",
                        "username": "ACR",
                        "isSftpUser": true,
                        "maxPriority": "HIGH"
                    },
                    "completedDate": "2025-07-28T22:27:08.110-04:00",
                    "recordCount": 200,
                    "createdDate": "2025-07-27T17:53:50.000-04:00",
                    "uploadFileName": "47QSEA21D0051-3159008_20250727215335.gsa",
                    "complete": true,
                    "reportFileCount": 1,
                    "reportFileNames": [
                        "47QSEA21D0051-3159008_20250727215335_810684885778227873_report_1.gsa"
                    ],
                    "uploadStatus": "SUCCESS",
                    "uploadErrorDescription": null,
                    "submittingSystemMode": null,
                    "submittingId": null,
                    "submittingSystem": null,
                    "skipGreenInferences": false,
                    "skipGreenInferencesDate": null
                },
                {
                    "id": 175621,
                    "completedPercent": 100.0,
                    "gsaFeedDate": "2025-08-15T00:00:00.000-04:00",
                    "priority": "MEDIUM",
                    "user": {
                        "email": "acrops@gsa.gov",
                        "username": "ACR",
                        "isSftpUser": true,
                        "maxPriority": "HIGH"
                    },
                    "completedDate": "2025-07-28T22:27:08.110-04:00",
                    "recordCount": 200,
                    "createdDate": "2025-07-27T17:53:50.000-04:00",
                    "uploadFileName": "47QSEA21D0051-3159008_20250727215335.gsa",
                    "complete": true,
                    "reportFileCount": 1,
                    "reportFileNames": [
                        "47QSEA21D0051-3159008_20250727215335_810684885778227873_report_1.gsa"
                    ],
                    "uploadStatus": "SUCCESS",
                    "uploadErrorDescription": null,
                    "submittingSystemMode": null,
                    "submittingId": null,
                    "submittingSystem": null,
                    "skipGreenInferences": false,
                    "skipGreenInferencesDate": null
                },
                {
                    "id": 175622,
                    "completedPercent": 100.0,
                    "gsaFeedDate": "2025-08-15T00:01:00.000-04:00",
                    "priority": "MEDIUM",
                    "user": {
                        "email": "acrops@gsa.gov",
                        "username": "ACR",
                        "isSftpUser": true,
                        "maxPriority": "HIGH"
                    },
                    "completedDate": "2025-07-28T22:27:08.110-04:00",
                    "recordCount": 200,
                    "createdDate": "2025-07-27T17:53:50.000-04:00",
                    "uploadFileName": "47QSEA21D0051-3159008_20250727215335.gsa",
                    "complete": true,
                    "reportFileCount": 1,
                    "reportFileNames": [
                        "47QSEA21D0051-3159008_20250727215335_810684885778227873_report_1.gsa"
                    ],
                    "uploadStatus": "SUCCESS",
                    "uploadErrorDescription": null,
                    "submittingSystemMode": null,
                    "submittingId": null,
                    "submittingSystem": null,
                    "skipGreenInferences": false,
                    "skipGreenInferencesDate": null
                }
            ]
            """;
    }

    private String gsaFeedDateTestDataStructure2(){
        // Structure 2: Different structure with 3 objects (uncomment to test)
        return """
            [
                {"gsaFeedDate": "2025-08-15T00:00:00.000-04:00", "id": 1},
                {"gsaFeedDate": "2025-08-10T00:00:00.000-04:00", "id": 2},
                {"gsaFeedDate": "2025-08-20T00:00:00.000-04:00", "id": 3}
            ]
            """;

    }

    private String gsaFeedDateTestDataStructure3(){
        // Structure 3: Minimal structure (uncomment to test)

        return """
            [
                {"gsaFeedDate": "2025-09-01T00:00:00.000-04:00"},
                {"gsaFeedDate": "2025-08-25T00:00:00.000-04:00"}
            ]
            """;
    }

    private String gsaFeedDateTestDataEmpty(){
        // Structure 3: Minimal structure (uncomment to test)

        return """
            [
            ]
            """;
    }



}