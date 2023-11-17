package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.util.ConcurrentModificationException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Slf4j
@TestPropertySource(locations="classpath:application-test.properties")
class XsbDataControllerTest {
    @Autowired
    WebTestClient webTestClient;

   @MockBean
    private XsbDataService xsbDataService;

    @Test
    void testTriggerEmptyBody() {
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenReturn(Mono.empty());

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).contains("Bad Request"));
    }

    @Test
    void triggerEndPoint() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenReturn(Mono.empty());

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("\nTriggered\n"));
    }

    @Test
    void trigger_alreadyWorking() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenThrow(new ConcurrentModificationException("Working"));
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isEqualTo(503)
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("\nWorking\n"));
    }


    @Test
    void trigger_error() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenReturn(Mono.error(new RuntimeException("Dummy")));
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("\nTriggered\n"));
    }


    @Test
    void trigger_unexpectedException() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenThrow(new RuntimeException("Dummy"));
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("Dummy"));
    }



    @Test
    void sftp() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenReturn(Flux.empty());
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/download")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isNull());

    }

    @Test
    void sftp_multiple() {
        Path [] dummyFiles = {Path.of("file1"), Path.of("file2")};
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenReturn(Flux.fromArray(dummyFiles));

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/download")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("file1\nfile2\n"));
    }


    @Test
    void sftp_error() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenReturn(Flux.error(new RuntimeException("Dummy")));

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/download")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(String.class).value(response -> assertThat(response).isNotBlank());
    }

    @Test
    void sftp_exception() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenThrow(new RuntimeException("Dummy Exception"));

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/download")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("Dummy Exception"));
    }
}