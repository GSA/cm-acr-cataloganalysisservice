package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.PpApiConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@MockBeans({@MockBean(PpApiConfig.class)})
@ContextConfiguration(classes = {XsbPpApiService.class})
@TestPropertySource(properties = { "spring.http.client.connect-timeout=5000", "spring.http.client.connect-timeout=3s" })
class XsbPpApiServiceTest3 {
    @Autowired
    private XsbPpApiService xsbPpApiService;
    @Autowired
    private PpApiConfig apiConfig;

    @BeforeEach
    void setUp() {
        when(apiConfig.getHostport()).thenReturn(String.format("http://localhost"));
        when(apiConfig.getAuthUrl()).thenReturn("/api/auth");
        when(apiConfig.getStatsUrl()).thenReturn("/api/catalog/stats");
        when(apiConfig.getUsername()).thenReturn("ACR");
        when(apiConfig.getPassword()).thenReturn("fake");
    }

    @Test
    @DisplayName("Test retry logic for server connection failure")
    void getAuthToken() {
        StepVerifier.create(xsbPpApiService.getAuthToken()).expectError().verify();
    }
}