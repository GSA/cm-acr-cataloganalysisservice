package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import gov.gsa.acr.cataloganalysis.service.XsbDataParser;
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
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Slf4j
@TestPropertySource(locations="classpath:application-test.properties")
class AnalysisDataControllerTest {
    @Autowired
    WebTestClient webTestClient;

    @Autowired
    XsbDataParser xsbDataParser;

   @MockBean
    private AnalysisDataProcessingService analysisDataProcessingService;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");


    @Test
    void testTriggerEmptyBody() {
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.empty());

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
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.empty());

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
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenThrow(new ConcurrentModificationException("Working"));
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
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.error(new RuntimeException("Dummy")));
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
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenThrow(new RuntimeException("Dummy"));
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
        Mockito.when(analysisDataProcessingService.downloadReports(any())).thenReturn(Flux.empty());
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
        Mockito.when(analysisDataProcessingService.downloadReports(any())).thenReturn(Flux.fromArray(dummyFiles));

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
        Mockito.when(analysisDataProcessingService.downloadReports(any())).thenReturn(Flux.error(new RuntimeException("Dummy")));

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
        Mockito.when(analysisDataProcessingService.downloadReports(any())).thenThrow(new RuntimeException("Dummy Exception"));

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/download")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("Dummy Exception"));
    }


    @Test
    void parse() {
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.parseXsbFiles(any())).thenReturn(Flux.empty());
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/parse")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response).isNull());

    }

    @Test
    void parse_multiple() {
        String xsbDataString1 = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~dummy~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~dummy~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData x1 = xsbDataParser.parseXsbData("testFile.gsa", xsbDataString1, taaCountryCodes);
        XsbData [] dummyXsbData = {x1};
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.parseXsbFiles(any())).thenReturn(Flux.fromArray(dummyXsbData));

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/parse")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response.getPartNumber()).isEqualTo(x1.getPartNumber()));
    }


    @Test
    void parse_error() {
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.parseXsbFiles(any())).thenReturn(Flux.error(new RuntimeException("Dummy")));

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/parse")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(String.class).value(response -> assertThat(response).isNotBlank());
    }

    @Test
    void parse_exception() {
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.parseXsbFiles(any())).thenThrow(new RuntimeException("Dummy Exception"));

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/parse")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().is5xxServerError();
    }



}