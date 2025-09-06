package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.PpApiConfig;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@MockBeans({@MockBean(PpApiConfig.class)})
@ContextConfiguration(classes = {XsbPpApiService.class})
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

        when(apiConfig.getHostport()).thenReturn(String.format("http://localhost:%s", mockBackEnd.getPort()));
        when(apiConfig.getAuthUrl()).thenReturn("/api/auth");
        when(apiConfig.getUsername()).thenReturn("ACR");
        when(apiConfig.getPassword()).thenReturn("fake");


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

        when(apiConfig.getHostport()).thenReturn(String.format("http://localhost:%s", mockBackEnd.getPort()));
        when(apiConfig.getAuthUrl()).thenReturn("/api/auth");
        when(apiConfig.getUsername()).thenReturn("ACR");
        when(apiConfig.getPassword()).thenReturn("fake");

        StepVerifier.create(xsbPpApiService.getAuthToken()).expectError().verify(Duration.ofSeconds(120));
        assertEquals(7, count.get());
    }



    @Test
    void getAuthToken_ServerErrorSuccesOn4th() throws IOException {
        AtomicInteger count = new AtomicInteger(0);
        String token = """
                {
                "access_token": "anAccessToken",
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

        when(apiConfig.getHostport()).thenReturn(String.format("http://localhost:%s", mockBackEnd.getPort()));
        when(apiConfig.getAuthUrl()).thenReturn("/api/auth");
        when(apiConfig.getUsername()).thenReturn("ACR");
        when(apiConfig.getPassword()).thenReturn("fake");

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

        when(apiConfig.getHostport()).thenReturn(String.format("http://localhost:%s", mockBackEnd.getPort()));
        when(apiConfig.getAuthUrl()).thenReturn("/api/auth");
        when(apiConfig.getUsername()).thenReturn("ACR");
        when(apiConfig.getPassword()).thenReturn("fake");

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

        when(apiConfig.getHostport()).thenReturn(String.format("http://localhost:%s", mockBackEnd.getPort()));
        when(apiConfig.getAuthUrl()).thenReturn("/api/auth");
        when(apiConfig.getUsername()).thenReturn("ACR");
        when(apiConfig.getPassword()).thenReturn("fake");

        StepVerifier.create(xsbPpApiService.getAuthToken()).expectError().verify();
    }

    @Test
    void getLatestXsbStats() {
    }

    @Test
    void getGsaFeedDate() {
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
    private String getGsaFeedDataFromApi() {
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
                }
            ]
            """;

        // Structure 2: Different structure with 3 objects (uncomment to test)
        /*
        return """
            [
                {"gsaFeedDate": "2025-08-15T00:00:00.000-04:00", "id": 1},
                {"gsaFeedDate": "2025-08-10T00:00:00.000-04:00", "id": 2},
                {"gsaFeedDate": "2025-08-20T00:00:00.000-04:00", "id": 3}
            ]
            """;
        */

        // Structure 3: Minimal structure (uncomment to test)
        /*
        return """
            [
                {"gsaFeedDate": "2025-09-01T00:00:00.000-04:00"},
                {"gsaFeedDate": "2025-08-25T00:00:00.000-04:00"}
            ]
            """;
        */
    }



}