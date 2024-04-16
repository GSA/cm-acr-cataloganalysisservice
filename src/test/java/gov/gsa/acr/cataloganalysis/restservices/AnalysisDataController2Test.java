package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@MockBeans(@MockBean(XsbDataRepository.class))
@AutoConfigureWebTestClient
@Slf4j
@TestPropertySource(locations="classpath:application-test.properties")
class AnalysisDataController2Test {
    @Autowired
    WebTestClient webTestClient;

    @Autowired
    AnalysisDataProcessingService analysisDataProcessingService;

    @Autowired
    XsbDataRepository xsbDataRepository;

    @MockBean
    ErrorHandler errorHandler;

    @Test
    void trigger_useJson() throws InterruptedException {
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just("{\"files\":[\"36F79722D0055*\", \"test1_8thAug2*\"], \"purgeOldData\":true}"), String.class)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nTrigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB)."));
        Thread.sleep(5000);
    }

    @Test
    void trigger_NoSourceType() throws InterruptedException {
        Trigger trigger= new Trigger();

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nTrigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB)."));
        Thread.sleep(5000);
    }

    @Test
    void trigger_NoFiles() throws InterruptedException {
        Trigger trigger= new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.S3);

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nTrigger argument must include files attribute (an array with file names or file name patterns)."));
        Thread.sleep(5000);
    }


    @Test
    void trigger_NoSourceFolder() throws InterruptedException {
        Trigger trigger= new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("\nA valid sourceFolder attribute is required for LOCAL sourceType. Received, null"));
        Thread.sleep(5000);
    }


    @Test
    void trigger_ErrorHandlerError() throws InterruptedException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
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
                .expectStatus()
                .isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("\nTriggered\n"));

        Thread.sleep(5000);
    }

    @Test
    void testGetEtsCount(){
        when(xsbDataRepository.etsCount()).thenReturn(Mono.just(10));
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/ets/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Integer.class).value(count -> assertThat(count).isEqualTo(10));

    }

    @Test
    void testGetLowOutlierCount(){
        when(xsbDataRepository.isLowOutlierCount()).thenReturn(Mono.just(11));
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/low-outlier/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Integer.class).value(count -> assertThat(count).isEqualTo(11));

    }

    @Test
    void testGetMiaRiskCount(){
        when(xsbDataRepository.isMIARiskCount()).thenReturn(Mono.just(12));
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/mia-risk/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Integer.class).value(count -> assertThat(count).isEqualTo(12));

    }

    @Test
    void testGetExceedsMarketThresholdCount(){
        when(xsbDataRepository.exceedsMarketThresholdCount()).thenReturn(Mono.just(13));
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/exceeds-market-threshold/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Integer.class).value(count -> assertThat(count).isEqualTo(13));

    }

    @Test
    void testGetProhibitedCount(){
        when(xsbDataRepository.isProhibitedCount()).thenReturn(Mono.just(14));
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/isProhibited/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Integer.class).value(count -> assertThat(count).isEqualTo(14));

    }

    @Test
    void testGetTaaRiskCount(){
        when(xsbDataRepository.isTAARiskCount()).thenReturn(Mono.just(15));
        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/taa-risk/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Integer.class).value(count -> assertThat(count).isEqualTo(15));

    }

    @Test
    void testBaseController(){
        BaseController baseController = new BaseController();
        assertNotNull(baseController);
    }
}