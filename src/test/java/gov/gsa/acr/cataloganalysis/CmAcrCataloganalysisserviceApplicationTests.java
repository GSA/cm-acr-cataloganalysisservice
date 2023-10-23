package gov.gsa.acr.cataloganalysis;

import gov.gsa.acr.cataloganalysis.service.XsbDataParser;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.File;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes = {XsbDataParser.class})
class CmAcrCataloganalysisserviceApplicationTests {

    @Test
    void contextLoads() {
        log.info("Current working directory {}", new File("").getAbsolutePath());
    }

    @Test
    void firstTest(){
        Flux<String> strFlux = Flux.just("These","Strings","will","create","flux").log();
        StepVerifier.create(strFlux)
                .expectNext("These")
                .expectNext("Strings")
                .expectNext("will")
                .expectNext("create")
                .expectNext("flux")
                .verifyComplete();
    }

}
