package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.util.AcrXsbFilesUtil;
import gov.gsa.acr.cataloganalysis.util.AcrXsbS3Util;
import gov.gsa.acr.cataloganalysis.util.AcrXsbSftpUtil;
import gov.gsa.acr.cataloganalysis.util.XsbSourceFactory;
import io.r2dbc.postgresql.codec.Json;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@SpringBootTest
@Slf4j
@MockBeans({@MockBean(ErrorHandler.class), @MockBean(XsbDataRepository.class)})
@ContextConfiguration(classes = {S3ClientConfiguration.class,  XsbDataService.class, XsbSourceFactory.class, AcrXsbS3Util.class, XsbDataParser.class, AcrXsbSftpUtil.class, AcrXsbFilesUtil.class})
@TestPropertySource(locations="classpath:application-test.properties")
class XsbDataServiceTest {

    @Autowired
    private XsbDataRepository xsbDataRepository;
    @Autowired
    private XsbDataParser xsbDataParser;
    @Autowired
    private ErrorHandler errorHandler;
    @Autowired
    private XsbDataService xsbDataService;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");

    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectory(Path.of("tmp"));
    }

    @AfterEach
    void tearDown() throws IOException {
        try (Stream<Path> stream = Files.list(Path.of("tmp"))) {
            stream.forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.error("Unable to delete error file: " + p, e);
                }
            });
        }
        catch (IOException e){
            log.error("Unable to delete error files.", e);
        }
        Files.deleteIfExists(Path.of("tmp"));
    }

    @Test
    void testParsingEmptyFile() {
        Path emptyFile = Path.of("junitTestData/emptyFile_1.gsa");
        StepVerifier.create(xsbDataService.parseXsbFile(emptyFile, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(emptyFile.toString()), eq("Ignoring File. No value present"), Mockito.any(NoSuchElementException.class) );
    }

    @Test
    void testParsingInvalidFile() {
        Path invalidFile = Path.of("junitTestData/invalid.gsa");
        StepVerifier.create(xsbDataService.parseXsbFile(invalidFile, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(invalidFile.toString()), eq("Ignoring File. " + invalidFile), Mockito.any(NoSuchFileException.class) );
    }

    @Test
    void testParsingFileWithInvalidHeader() {
        Path invalidHdrFile = Path.of("junitTestData/testFileWithInvalidHeader.gsa");
        StepVerifier.create(xsbDataService.parseXsbFile(invalidHdrFile, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(invalidHdrFile.toString()), matches("Ignoring File. Header String for file.*"), Mockito.any(NoSuchElementException.class) );
    }

    @Test
    void testParsingFileWithInvalidRecords() {
        Path invalidRecordsFile = Path.of("junitTestData/testFileWithErrors.gsa");
        StepVerifier.create(xsbDataService.parseXsbFile(invalidRecordsFile, taaCountryCodes))
                .expectNextCount(16)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(5)).handleParsingError(Mockito.anyString(),eq(invalidRecordsFile.toString()), Mockito.anyString());
    }

    @Test
    void testParsingFileWithJustHeader() {
        Path fileWithJustHeader = Path.of("junitTestData/testFileWithJustHeader.gsa");
        StepVerifier.create(xsbDataService.parseXsbFile(fileWithJustHeader, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }

    @Test
    void testParsingValidFile() {
        Path fileWithJustHeader = Path.of("junitTestData/testValidFile.gsa");
        StepVerifier.create(xsbDataService.parseXsbFile(fileWithJustHeader, taaCountryCodes))
                .expectNextCount(10)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }

    @Test
    void testParseXsbFiles() {
        Path[] filesToParse = {
                Path.of("junitTestData/emptyFile_1.gsa"),
                Path.of("junitTestData/invalid.gsa"),
                Path.of("junitTestData/testFileWithInvalidHeader.gsa"),
                Path.of("junitTestData/testFileWithErrors.gsa"),
                Path.of("junitTestData/testFileWithJustHeader.gsa"),
                Path.of("junitTestData/testValidFile.gsa")
        };

        StepVerifier.create(xsbDataService.parseXsbFiles(Flux.empty(), taaCountryCodes))
                .expectComplete()
                .verify();

        StepVerifier.create(xsbDataService.parseXsbFiles(Flux.fromIterable(Arrays.asList(filesToParse)), taaCountryCodes))
                .expectNextCount(26)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(Mockito.anyString(), matches("Ignoring File.*"), Mockito.any(NoSuchFileException.class) );
        Mockito.verify(errorHandler, Mockito.times(2)).handleFileError(Mockito.anyString(), matches("Ignoring File.*"), Mockito.any(NoSuchElementException.class) );
        Mockito.verify(errorHandler, Mockito.times(5)).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    }

    @Test
    void testSaveNullDataRecordToStaging() {
        Random rn = new Random();
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.just(rn.nextInt(100)));
        StepVerifier.create(xsbDataService.saveDataRecordToStaging(null))
                .expectComplete()
                .verify();
    }

    @Test
    void testSaveInvalidDataRecordToStaging() {
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenThrow(new RuntimeException("Dummy"));
        XsbData xsbData = new XsbData();
        xsbData.setContractNumber("contract number");
        xsbData.setManufacturer("manufacturer");
        xsbData.setPartNumber("part number");
        xsbData.setXsbData(Json.of("{\"dummy\": \"string\"}"));
        StepVerifier.create(xsbDataService.saveDataRecordToStaging(xsbData))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleDBError(eq(xsbData), eq("Dummy"));

    }


    @Test
    void testSaveInvalidDataRecordToStaging2() {
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.error(new RuntimeException("Dummy")));
        XsbData xsbData = new XsbData();
        xsbData.setContractNumber("contract number");
        xsbData.setManufacturer("manufacturer");
        xsbData.setPartNumber("part number");
        xsbData.setXsbData(Json.of("{\"dummy\": \"string\"}"));
        StepVerifier.create(xsbDataService.saveDataRecordToStaging(xsbData))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleDBError(eq(xsbData), eq("Dummy"));

    }


    @Test
    void testSaveDataRecordToStaging() {
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData xsbData = xsbDataParser.parseXsbData("testFile.gsa", xsbDataString, taaCountryCodes);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.just(1));
        StepVerifier.create(xsbDataService.saveDataRecordToStaging(xsbData))
                .expectNext(1)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(any(XsbData.class), anyString());
    }

    @Test
    void testDontMoveDataFromStagingToFinal() {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(false);
        StepVerifier.create(xsbDataService.moveDataFromStagingToFinal())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(eq(""), anyString(), any(Exception.class));
    }

    @Test
    void testMoveDataFromStagingToFinal_deleteAllException() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Exception e = new RuntimeException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenThrow(e);
        StepVerifier.create(xsbDataService.moveDataFromStagingToFinal())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_deleteAllError() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Exception e = new RuntimeException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.error(e));
        StepVerifier.create(xsbDataService.moveDataFromStagingToFinal())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_movXsbDataException() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Exception e = new RuntimeException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenThrow(e);
        StepVerifier.create(xsbDataService.moveDataFromStagingToFinal())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_moveXsbDataError() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Exception e = new RuntimeException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.error(e));
        StepVerifier.create(xsbDataService.moveDataFromStagingToFinal())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_movXsbDataSuccess() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(xsbDataService.moveDataFromStagingToFinal())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(eq(""), eq(errMsg), any(Exception.class));
    }

    @Test
    void testDeleteOldStagingData_Exception() {
        Exception e = new RuntimeException("Dummy");
        final AtomicBoolean firstCalled = new AtomicBoolean(false);
        final AtomicBoolean finallyCalled =  new AtomicBoolean(false);
        assertFalse(firstCalled.get());
        assertFalse(finallyCalled.get());

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenThrow(e);
        StepVerifier.create(
                xsbDataService.deleteOldStagingData()
                        .doFirst(() -> firstCalled.compareAndSet(false, true))
                        .doFinally(s -> finallyCalled.compareAndSet(false, true))
                )
                .expectError(RuntimeException.class)
                .verify();

        assertTrue(firstCalled.get());
        assertTrue(finallyCalled.get());
    }

    @Test
    void testDeleteOldStagingData_Error() {
        Exception e = new RuntimeException("Dummy");
        final AtomicBoolean firstCalled = new AtomicBoolean(false);
        final AtomicBoolean finallyCalled =  new AtomicBoolean(false);
        assertFalse(firstCalled.get());
        assertFalse(finallyCalled.get());

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.error(e));
        StepVerifier.create(xsbDataService.deleteOldStagingData()
                        .doFirst(() -> firstCalled.compareAndSet(false, true))
                        .doFinally(s -> finallyCalled.compareAndSet(false, true))
                )
                .expectError(RuntimeException.class)
                .verify();

        assertTrue(firstCalled.get());
        assertTrue(finallyCalled.get());
    }


    @Test
    void testDeleteOldStagingData() {
        final AtomicBoolean firstCalled = new AtomicBoolean(false);
        final AtomicBoolean finallyCalled =  new AtomicBoolean(false);
        assertFalse(firstCalled.get());
        assertFalse(finallyCalled.get());

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        StepVerifier.create(xsbDataService.deleteOldStagingData()
                        .doFirst(() -> firstCalled.compareAndSet(false, true))
                        .doFinally(s -> finallyCalled.compareAndSet(false, true))
                )
                .verifyComplete();

        assertTrue(firstCalled.get());
        assertTrue(finallyCalled.get());
    }


    @Test
    void testFindTaaCompliantCountries_Exception() {
        Exception e = new RuntimeException("Dummy");
        when(xsbDataRepository.findTaaCompliantCountries()).thenThrow(e);
        StepVerifier.create(xsbDataService.findTaaCompliantCountries())
                .expectError()
                .verify();
    }


    @Test
    void testFindTaaCompliantCountries_Error() {
        Exception e = new RuntimeException("Dummy");
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.error(e));
        StepVerifier.create(xsbDataService.findTaaCompliantCountries())
                .expectError()
                .verify();
    }

    @Test
    void testFindTaaCompliantCountries() {
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
        StepVerifier.create(xsbDataService.findTaaCompliantCountries())
                .expectNext(Arrays.asList("AF", "AG", "AM", "AO", "AT"))
                .verifyComplete();
    }

    @Test
    void trigger() {
    }

}