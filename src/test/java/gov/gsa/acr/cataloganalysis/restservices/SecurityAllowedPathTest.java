package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.scheduler.ScheduledTasks;
import gov.gsa.acr.cataloganalysis.service.XsbPpApiService;
import gov.gsa.acr.cataloganalysis.util.TokenService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
@MockBeans({@MockBean(XsbDataRepository.class),@MockBean(TokenService.class), @MockBean(ScheduledTasks.class), @MockBean(XsbPpApiService.class)})
@TestPropertySource(locations="classpath:application-allowedpathsecurity.properties")
@AutoConfigureWebTestClient
class SecurityAllowedPathTest {
    @Autowired
    WebTestClient webTestClient;

    @Autowired
    TokenService tokenService;

    @BeforeEach
    void setUp() {
        when(tokenService.validate(any())).thenReturn(false);
    }

    @Test
    void authFilterTestDisabledSecurity() {
        // valid Token
        webTestClient.get().uri("/api/info")
                .header(HttpHeaders.AUTHORIZATION,"Bearer validToken")
                .exchange()
                .expectStatus().isOk();

        // invalid token
        webTestClient.get().uri("/api/info")
                .header(HttpHeaders.AUTHORIZATION,"Bearer invalidToken")
                .exchange()
                .expectStatus().isOk();

        // invalid token - without "Bearer" keyword
        webTestClient.get().uri("/api/info")
                .header(HttpHeaders.AUTHORIZATION,"bogusToken")
                .exchange()
                .expectStatus().isOk();


        Trigger trigger= new Trigger();
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isUnauthorized();

    }
}