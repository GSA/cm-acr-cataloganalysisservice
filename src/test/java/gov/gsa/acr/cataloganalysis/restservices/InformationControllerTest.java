package gov.gsa.acr.cataloganalysis.restservices;

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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
@MockBeans({@MockBean(TokenService.class), @MockBean(ScheduledTasks.class), @MockBean(XsbPpApiService.class)})
@TestPropertySource(locations="classpath:application-test.properties")
@AutoConfigureWebTestClient
class InformationControllerTest {
    @Autowired
    WebTestClient webTestClient;

    @Autowired
    TokenService tokenService;

    @BeforeEach
    void authorizeCalls() {
        when(tokenService.validate(any())).thenReturn(true);
    }

    @Test
    void testRootEndPoint() {
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(greeting -> assertThat(greeting).isEqualTo("Welcome to Catalog Analysis"));
    }

    @Test
    void authFilterReturnsUnauthorized() {
        when(tokenService.validate(any())).thenReturn(false);
        // Create a GET request to test an endpoint
        webTestClient.get().uri("/api/info")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void testInfoEndPoint() {
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/info")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(greeting -> assertThat(greeting).isEqualTo("A service for analyzing catalogs in ACR"));
    }

    @Test
    void authFilterTestTokens() {
        when(tokenService.validate("validToken")).thenReturn(true);
        when(tokenService.validate("invalidToken")).thenReturn(false);
        when(tokenService.validate(null)).thenReturn(false);
        // valid Token
        webTestClient.get().uri("/api/info")
                .header(HttpHeaders.AUTHORIZATION,"Bearer validToken")
                .exchange()
                .expectStatus().isOk();

        // invalid token
        webTestClient.get().uri("/api/info")
                .header(HttpHeaders.AUTHORIZATION,"Bearer invalidToken")
                .exchange()
                .expectStatus().isUnauthorized();

        // invalid token - without "Bearer" keyword
        webTestClient.get().uri("/api/info")
                .header(HttpHeaders.AUTHORIZATION,"bogusToken")
                .exchange()
                .expectStatus().isUnauthorized();

    }

}