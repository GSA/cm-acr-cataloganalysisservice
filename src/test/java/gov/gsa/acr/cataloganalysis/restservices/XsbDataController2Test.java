package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Slf4j
@TestPropertySource(locations="classpath:application-test.properties")
class XsbDataController2Test {
    @Autowired
    WebTestClient webTestClient;

    @Autowired
    XsbDataService xsbDataService;

    @MockBean
    ErrorHandler errorHandler;

    @Test
    void trigger_NoSourceType() {
        Trigger trigger= new Trigger();

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nTrigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or SFTP)."));
    }

    @Test
    void trigger_NoFiles() {
        Trigger trigger= new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.S3);

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nTrigger argument must include files attribute (an array with file names or file name patterns)."));
    }


    @Test
    void trigger_NoSourceFolder() {
        Trigger trigger= new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.LOCAL);

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nA valid sourceFolder attribute is required for LOCAL sourceType. Received, null"));
    }


    @Test
    void trigger_ErrorHandlerError() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.SFTP);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        Exception e = new RuntimeException("Unexpected error. Unable to delete old error files from previous executions.");

        doThrow(e).when(errorHandler).init(anyString());

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nUnexpected error. Unable to delete old error files from previous executions."));

    }
}