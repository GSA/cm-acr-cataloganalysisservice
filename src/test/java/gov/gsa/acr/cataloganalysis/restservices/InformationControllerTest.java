package gov.gsa.acr.cataloganalysis.restservices;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes ={InformationController.class})
@TestPropertySource(locations="classpath:application-test.properties")
class InformationControllerTest {

    @Autowired
    InformationController informationController;

    @Test
    void greet() {
        assertEquals("Welcome to Catalog Analysis",informationController.greet());
    }

    @Test
    void info() {
        assertEquals("A service for analyzing catalogs in ACR", informationController.info());
    }
}