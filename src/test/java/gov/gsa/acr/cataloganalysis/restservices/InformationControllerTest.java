package gov.gsa.acr.cataloganalysis.restservices;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Slf4j
@TestPropertySource(locations="classpath:application-test.properties")
@AutoConfigureWebTestClient
class InformationControllerTest {
    @Autowired
    WebTestClient webTestClient;

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
    void testInfoEndPoint() {
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/info")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(greeting -> assertThat(greeting).isEqualTo("A service for analyzing catalogs in ACR"));
    }
}