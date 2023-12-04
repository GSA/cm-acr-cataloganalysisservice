package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceFactory;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceLocal;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceS3;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.DataUploadResults;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(ErrorHandler.class), @MockBean(XsbDataRepository.class), @MockBean(AnalysisSourceXsb.class), @MockBean(AnalysisSourceS3.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  AnalysisDataProcessingService.class, XsbDataParser.class, AnalysisSourceLocal.class, AnalysisSourceFactory.class, TransactionalDataService.class})
class AnalysisDataProcessingServiceTest {

    @Value("${error.file.directory}")
    private String errorDirectory;

    @Autowired
    private XsbDataRepository xsbDataRepository;
    @Autowired
    private XsbDataParser xsbDataParser;
    @Autowired
    private ErrorHandler errorHandler;
    @Autowired
    private AnalysisSourceS3 xsbSourceS3Files;
    @Autowired
    private AnalysisDataProcessingService analysisDataProcessingService;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");

    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectory(Path.of("tmp"));
    }

    @AfterEach
    void tearDown() {
        StepVerifier.create(analysisDataProcessingService.deleteTmpDir(Path.of("tmp")))
                .expectNext(true)
                .verifyComplete();
    }


    @Test
    void testParsingEmptyFile() {
        Path emptyFile = Path.of("junitTestData/emptyFile_1.gsa");
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(emptyFile, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(emptyFile.toString()), eq("Ignoring File. No value present"), Mockito.any(NoSuchElementException.class) );
    }

    @Test
    void testParsingInvalidFile() {
        Path invalidFile = Path.of("junitTestData/invalid.gsa");
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(invalidFile, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(invalidFile.toString()), eq("Ignoring File. " + invalidFile), Mockito.any(NoSuchFileException.class) );
    }

    @Test
    void testParsingFileWithInvalidHeader() {
        Path invalidHdrFile = Path.of("junitTestData/testFileWithInvalidHeader.gsa");
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(invalidHdrFile, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(invalidHdrFile.toString()), matches("Ignoring File. Header String for file.*"), Mockito.any(NoSuchElementException.class) );
    }


    @Test
    void testParsingFileWithInvalidRecords() {
        Path invalidRecordsFile = Path.of("junitTestData/testFileWithErrors.gsa");
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(invalidRecordsFile, taaCountryCodes))
                .expectNextCount(16)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(5)).handleParsingError(Mockito.anyString(),eq(invalidRecordsFile.toString()), Mockito.anyString());
    }


    @Test
    void testParsingFileWithJustHeader() {
        Path fileWithJustHeader = Path.of("junitTestData/testFileWithJustHeader.gsa");
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(fileWithJustHeader, taaCountryCodes))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void testParsingValidFile() {
        Path fileWithJustHeader = Path.of("junitTestData/testValidFile.gsa");
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(fileWithJustHeader, taaCountryCodes))
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

        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(Flux.empty(), taaCountryCodes))
                .verifyComplete();

        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(Flux.fromIterable(Arrays.asList(filesToParse)), taaCountryCodes))
                .expectNextCount(26)
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(Mockito.anyString(), matches("Ignoring File.*"), Mockito.any(NoSuchFileException.class) );
        Mockito.verify(errorHandler, Mockito.times(2)).handleFileError(Mockito.anyString(), matches("Ignoring File.*"), Mockito.any(NoSuchElementException.class) );
        Mockito.verify(errorHandler, Mockito.times(5)).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    }


    @Test
    void testSaveNullDataRecordToStaging() {
        Random rn = new Random();
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.just(rn.nextInt(100)));
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(null))
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
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
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
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleDBError(eq(xsbData), eq("Dummy"));

    }


    @Test
    void testSaveDataRecordToStaging() {
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData xsbData = xsbDataParser.parseXsbData("testFile.gsa", xsbDataString, taaCountryCodes);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.just(1));
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectNext(1)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(any(XsbData.class), anyString());
    }

    @Test
    void testDontMoveDataFromStagingToFinal() {
        Trigger trigger = new Trigger();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(false);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), anyString(), any(Exception.class));
    }

    @Test
    void testDontMoveDataFromStagingToFinal_totalErrorsWithinAcceptableThresholdException() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_deleteAllException() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_deleteAllError() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Exception e = new Exception("Dummy");
        Trigger trigger = new Trigger();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.error(e));
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_movXsbDataException() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_moveXsbDataError() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        Exception e = new Exception("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.error(e));
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_movXsbDataSuccess() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(eq(""), eq(errMsg), any(Exception.class));
    }

    @Test
    void testMoveDataFromStagingToFinal_noPurge() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.FALSE);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(eq(""), eq(errMsg), any(Exception.class));
    }

    @Test
    void testMoveDataFromStagingToFinal_forceReplaceRollback() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setForcedError(1);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), any(Exception.class));
    }

    @Test
    void testMoveDataFromStagingToFinal_ForceUpdateRollback() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.FALSE);
        trigger.setForcedError(1);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), any(Exception.class));
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
                analysisDataProcessingService.deleteOldStagingData()
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
        StepVerifier.create(analysisDataProcessingService.deleteOldStagingData()
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
        StepVerifier.create(analysisDataProcessingService.deleteOldStagingData()
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
        StepVerifier.create(analysisDataProcessingService.findTaaCompliantCountries())
                .expectError()
                .verify();
    }


    @Test
    void testFindTaaCompliantCountries_Error() {
        Exception e = new RuntimeException("Dummy");
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.error(e));
        StepVerifier.create(analysisDataProcessingService.findTaaCompliantCountries())
                .expectError()
                .verify();
    }

    @Test
    void testFindTaaCompliantCountries_noCountries() {
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(List.of()));
        StepVerifier.create(analysisDataProcessingService.findTaaCompliantCountries())
                .verifyError(NoSuchElementException.class);
    }


    @Test
    void testFindTaaCompliantCountries() {
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
        StepVerifier.create(analysisDataProcessingService.findTaaCompliantCountries())
                .expectNext(Arrays.asList("AF", "AG", "AM", "AO", "AT"))
                .verifyComplete();
    }


    @Test
    void testUploadErrorFilesToS3_Exception() {
        Exception e = new RuntimeException("Dummy");
        when(errorHandler.getErrorFiles()).thenReturn(Flux.just(Path.of("dummy")));
        when(xsbSourceS3Files.uploadToS3(any(), anyString())).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.uploadErrorFilesToS3())
                .expectError(RuntimeException.class)
                .verify();
    }


    @Test
    void testUploadErrorFilesToS3_Error() {
        Exception e = new RuntimeException("Dummy");
        when(errorHandler.getErrorFiles()).thenReturn(Flux.just(Path.of("dummy")));
        when(xsbSourceS3Files.uploadToS3(any(), anyString())).thenReturn(Mono.error(e));
        StepVerifier.create(analysisDataProcessingService.uploadErrorFilesToS3())
                .expectError(RuntimeException.class)
                .verify();
    }


    @Test
    void testUploadErrorFilesToS3() {
        when(errorHandler.getErrorFiles()).thenReturn(Flux.just(Path.of("dummy")));
        when(xsbSourceS3Files.uploadToS3(any(), anyString())).thenReturn(Mono.just("errors/dummy"));
        StepVerifier.create(analysisDataProcessingService.uploadErrorFilesToS3())
                .expectNext("errors/dummy")
                .verifyComplete();
    }


    @Test
    void testTriggerDataUpload_alreadyExecuting() throws InterruptedException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);

        Exception e = new RuntimeException("Dummy RuntimeException");

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumRecordsSavedInTempDB();
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
        errorHandler.setErrorDirectory(errorDirectory);
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenAnswer((Answer<Mono<Void>>) invocationOnMock -> {
            Thread.sleep(200);
            return Mono.error(e);
        });

        ExecutorService service = Executors.newFixedThreadPool(2);
        service.submit(() -> {
            log.info("Triggered first time");
            StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                    .verifyError(RuntimeException.class);
        });

        service.submit(() -> {
            log.info("Triggered second time");
            assertThrows (ConcurrentModificationException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        });

        Thread.sleep(300);
        log.info("Triggered third time");
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .verifyError(RuntimeException.class);
    }

    @Test
    void testTriggerDataUpload_nullTrigger() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(null));
        assertEquals("Illegal argument, trigger, cannot be null!", e.getMessage());
    }


    @Test
    void testTriggerDataUpload_nullUniqueFileNames() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include files attribute (an array with file names or file name patterns).", e.getMessage());
    }

    @Test
    void testTriggerDataUpload_ErrorHandlerError() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        Exception e = new RuntimeException("Dummy");

        doThrow(e).when(errorHandler).init(anyString());

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Dummy", thrown.getMessage());
    }

    @Test
    void testTriggerDataUpload_noSourceType() {
        Trigger trigger = new Trigger();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB).", e.getMessage());
    }


    @Test
    void testTriggerDataUpload_noSourceFolderForLocalSourceType() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("A valid sourceFolder attribute is required for LOCAL sourceType. Received, null", e.getMessage());
    }

    @Test
    void testValidateTrigger() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(null));
        assertEquals("Illegal argument, trigger, cannot be null!", e.getMessage());

        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB).", e.getMessage());

        trigger.setUniqueFileNames(uniqueFileNames);
        e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB).", e.getMessage());

        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        assertDoesNotThrow( () -> analysisDataProcessingService.triggerDataUpload(trigger));

        trigger.setSourceType(Trigger.AnalysisSourceType.S3);
        assertDoesNotThrow( () -> analysisDataProcessingService.triggerDataUpload(trigger));

        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("A valid sourceFolder attribute is required for LOCAL sourceType. Received, null", e.getMessage());

        trigger.setSourceFolder("");
        e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("A valid sourceFolder attribute is required for LOCAL sourceType. Received, ", e.getMessage());

        trigger.setSourceFolder("invalid");
        e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("A valid sourceFolder attribute is required for LOCAL sourceType. Received, invalid", e.getMessage());

        trigger.setSourceFolder("junitTestData");
        assertDoesNotThrow( () -> analysisDataProcessingService.triggerDataUpload(trigger));

        trigger.setUniqueFileNames(null);
        e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include files attribute (an array with file names or file name patterns).", e.getMessage());

        trigger.setFiles(new String[]{"aa", "bb", "cc", "dd"});
        uniqueFileNames = trigger.getUniqueFileNames();
        assertNotNull(uniqueFileNames);
        assertEquals(4, uniqueFileNames.size());
        assertEquals(4, trigger.getFiles().length);

        trigger.setFiles(new String[]{"aa", "bb", "cc", "dd", "aa", "bb"});
        uniqueFileNames = trigger.getUniqueFileNames();
        assertNotNull(uniqueFileNames);
        assertEquals(4, uniqueFileNames.size());
        assertEquals(6, trigger.getFiles().length);

    }

    @Test
    void testTriggerDataUpload_deleteOldStagingData_failure() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        Exception e = new RuntimeException("Dummy");

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumRecordsSavedInTempDB();
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.error(e));

        errorHandler.setErrorDirectory(errorDirectory);

        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(RuntimeException.class)
                .verify();

        Mockito.verify(xsbDataRepository, Mockito.times(1)).findTaaCompliantCountries();

    }


    @Test
    void testTriggerDataUpload_TAAError() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        Exception e = new RuntimeException("Dummy");

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumRecordsSavedInTempDB();
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.error(e));

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());

        errorHandler.setErrorDirectory(errorDirectory);

        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(RuntimeException.class)
                .verify();

        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAllXsbDataTemp();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).findTaaCompliantCountries();
    }


    @Test
    void testTriggerDataUpload_NoTAACountriesFound() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumRecordsSavedInTempDB();
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.empty());

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());

        errorHandler.setErrorDirectory(errorDirectory);

        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(NoSuchElementException.class)
                .verify();

        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAllXsbDataTemp();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).findTaaCompliantCountries();

    }

    @Test
    void testTriggerDataUpload_noFiles() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumRecordsSavedInTempDB();
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        doCallRealMethod().when(errorHandler).setNumRecordsSavedInTempDB(any());

        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(0);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.never()).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();


    }


    @Test
    void testTriggerDataUpload() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumRecordsSavedInTempDB();
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        doCallRealMethod().when(errorHandler).setNumRecordsSavedInTempDB(any());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.just(123));
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(26);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(26)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(5)).handleParsingError(anyString(), anyString(), anyString());

    }


    @Test
    void testDownload_getXsbFiles() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("dummy.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);


        StepVerifier.create(analysisDataProcessingService.downloadReports(trigger))
                .verifyComplete();

        uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        StepVerifier.create(analysisDataProcessingService.downloadReports(trigger).doOnNext(p -> log.info("File: {}", p)))
                .expectNextCount(4)
                .verifyComplete();

    }

    @Test
    void testProgressMonitoring() {
        assertEquals(1, analysisDataProcessingService.getProgressReportingIntervalSeconds());

        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumRecordsSavedInTempDB();
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        doCallRealMethod().when(errorHandler).setNumRecordsSavedInTempDB(any());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenAnswer((Answer<Mono<Integer>>) invocationOnMock -> {
            Thread.sleep(200);
            return Mono.just(123);
        });

        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(26);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(26)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(5)).handleParsingError(anyString(), anyString(), anyString());

    }


    @Test
    void testParse_getXsbFiles(){
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("dummy.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(taaCountryCodes));
        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(trigger))
                .verifyComplete();

        uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("testValidFile.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(trigger).doOnNext(x->log.info("xsbData: " + x.getXsbData())))
                .expectNextCount(10)
                .verifyComplete();

    }

}