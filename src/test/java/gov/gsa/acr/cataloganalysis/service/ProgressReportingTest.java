package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.util.AcrXsbFilesUtil;
import gov.gsa.acr.cataloganalysis.util.AcrXsbS3Util;
import gov.gsa.acr.cataloganalysis.util.AcrXsbSftpUtil;
import gov.gsa.acr.cataloganalysis.util.XsbSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(XsbDataParser.class),@MockBean(ErrorHandler.class), @MockBean(XsbDataRepository.class), @MockBean(AcrXsbSftpUtil.class), @MockBean(AcrXsbS3Util.class), @MockBean(TransactionalDataService.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  XsbDataService.class, AcrXsbFilesUtil.class, XsbSourceFactory.class})
class ProgressReportingTest {
    @Autowired
    private XsbDataService xsbDataService;

    @Autowired
    private XsbDataParser xsbDataParser;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");

    @Test
    void testProgressReporting() {
        assertEquals(1, xsbDataService.getProgressReportingIntervalSeconds());

        Path[] filesToParse = {
                Path.of("junitTestData/testFileWithErrors.gsa"),
                Path.of("junitTestData/testValidFile.gsa")
        };

        when(xsbDataParser.validateHeader(anyString())).thenReturn(true);
        when(xsbDataParser.parseXsbData(anyString(), anyString(), any())).thenAnswer((Answer<XsbData>) invocationOnMock -> {
            Thread.sleep(200);
            return new XsbData();
        });

        StepVerifier.create(xsbDataService.parseXsbFiles(Flux.fromIterable(Arrays.asList(filesToParse)), taaCountryCodes))
                .expectNextCount(31)
                .verifyComplete();
           }
}