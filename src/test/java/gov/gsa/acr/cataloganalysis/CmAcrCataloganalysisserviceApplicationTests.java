package gov.gsa.acr.cataloganalysis;

import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.service.XsbDataParser;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@SpringBootTest
@Slf4j
@MockBeans({@MockBean(ErrorHandler.class)})
@ContextConfiguration(classes = {XsbDataParser.class})
class CmAcrCataloganalysisserviceApplicationTests {

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

    @Test
    void testMainMethod() {
        String[] args = {"dummy", "args"};
        MockedStatic<SpringApplication> mockedSettings = mockStatic(SpringApplication.class);
        when(SpringApplication.run(CmAcrCataloganalysisserviceApplication.class, args)).thenReturn(null);
        CmAcrCataloganalysisserviceApplication.main(args);
        mockedSettings.close();
    }
}
