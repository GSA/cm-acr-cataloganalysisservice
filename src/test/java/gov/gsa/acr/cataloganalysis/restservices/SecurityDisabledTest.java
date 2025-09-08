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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
@MockBeans({@MockBean(TokenService.class), @MockBean(ScheduledTasks.class), @MockBean(XsbPpApiService.class)})
@TestPropertySource(locations="classpath:application-disabledsecurity.properties")
@AutoConfigureWebTestClient
class SecurityDisabledTest {
    @Autowired
    WebTestClient webTestClient;

    @Autowired
    TokenService tokenService;

    @BeforeEach
    void authorizeCalls() {
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

    }

}