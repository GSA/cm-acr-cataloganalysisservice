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
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    Set<String> nonTAACountryCodes = Set.of("AD", "AE", "AL", "AR", "AZ", "BA", "BN", "BO", "BR", "BW", "BY", "CG", "CI", "CM", "CN", "DZ", "EC", "EG", "EH", "FJ", "GE", "GH", "GP", "ID", "IN", "IQ", "JO", "KE", "KG", "KW", "KZ", "LB", "LK", "LY", "MC", "MH", "MK", "MN", "MO", "MU", "MV", "MY", "NA", "NG", "NR", "NU", "PG", "PH", "PK", "PW", "PY", "QA", "RS", "RU", "SA", "SC", "SM", "SR", "SY", "SZ", "TH", "TJ", "TL", "TM", "TN", "TO", "TR", "UY", "UZ", "VE", "VN", "ZA", "ZW");

    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectory(Path.of("tmp"));
    }

    @AfterEach
    void tearDown() {
        analysisDataProcessingService.deleteDir(Path.of("tmp"));
    }

    @Test
    void testParsingNullFile() {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(null, nonTAACountryCodes, true, null))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("null"), eq("Ignoring File. Cannot invoke \"java.nio.file.Path.getFileSystem()\" because \"path\" is null"), Mockito.any(NullPointerException.class) );
    }


    @Test
    void testParsingEmptyFile() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/emptyFile_1.gsa");
        Path emptyFile = Path.of("tmp/emptyFile_1.gsa");
        Files.copy(srcFile, emptyFile);
        assertTrue(Files.exists(emptyFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(emptyFile, nonTAACountryCodes, true, null))
                .expectComplete()
                .verify();
        assertTrue(Files.exists(srcFile));
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(emptyFile.toString()), eq("Ignoring File. No value present"), Mockito.any(NoSuchElementException.class) );
    }

    @Test
    void testParsingInvalidFile() {
        Path invalidFile = Path.of("junitTestData/invalid.gsa");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(invalidFile, nonTAACountryCodes, false, null))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(invalidFile.toString()), eq("Ignoring File. " + invalidFile), Mockito.any(NoSuchFileException.class) );
    }

    @Test
    void testParsingFileWithInvalidHeader() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/testFileWithInvalidHeader.gsa");
        Path invalidHdrFile = Path.of("tmp/testFileWithInvalidHeader.gsa");
        Files.copy(srcFile, invalidHdrFile);
        assertTrue(Files.exists(invalidHdrFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(invalidHdrFile, nonTAACountryCodes, false, null))
                .expectComplete()
                .verify();
        assertTrue(Files.exists(invalidHdrFile));
        assertTrue(Files.exists(srcFile));
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(invalidHdrFile.toString()), matches("Ignoring File. Header String for file.*"), Mockito.any(NoSuchElementException.class) );
    }


    @Test
    void testParsingFileWithInvalidRecords() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/testFileWithErrors.gsa");
        Path invalidRecordsFile = Path.of("tmp/testFileWithErrors.gsa");
        Files.copy(srcFile, invalidRecordsFile);
        assertTrue(Files.exists(invalidRecordsFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(invalidRecordsFile, nonTAACountryCodes, true, null))
                .expectNextCount(17)
                .expectComplete()
                .verify();

        assertTrue(Files.exists(srcFile));
        Mockito.verify(errorHandler, Mockito.times(4)).handleParsingError(Mockito.anyString(),eq(invalidRecordsFile.toString()), Mockito.anyString());
    }


    @Test
    void testParsingFileWithJustHeader() throws IOException {
        Path srcFile = Path.of("junitTestData/testFileWithJustHeader.gsa");
        Path fileWithJustHeader = Path.of("tmp/testFileWithJustHeader.gsa");
        Files.copy(srcFile, fileWithJustHeader);
        assertTrue(Files.exists(fileWithJustHeader));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(fileWithJustHeader, nonTAACountryCodes, true, null))
                .expectComplete()
                .verify();

        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void testParsingValidFile() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path validFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, nonTAACountryCodes, true, null))
                .expectNextCount(10)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void testParsingFileWithoutMdfGroupId() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/47QSCA18D000Z-3003677_20240507141350_8389982608816780647_report_1.gsa");
        Path validFile = Path.of("tmp/47QSCA18D000Z-3003677_20240507141350_8389982608816780647_report_1.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, nonTAACountryCodes, true, null))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("mdfGroupId")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("mdfGroupId")))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }

    @Test
    void testParsingFileWithMdfGroupId() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/47QSMA19D08P6-3013582_20240807190023_4963524368564643147_report_1.gsa");
        Path validFile = Path.of("tmp/47QSMA19D08P6-3013582_20240807190023_4963524368564643147_report_1.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, nonTAACountryCodes, true, null))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"mdfGroupId\":\""+46020247+"\"")))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void testParsingValidFileWithoutGsaFeedDate() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path validFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));

        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, nonTAACountryCodes, true, null))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .expectComplete()
                .verify();
    }


    @Test
    void testParsingValidFileWithGsaFeedDate() throws IOException {
        LocalDate now = LocalDate.now();
        String expectedGsaFeedDate = now.toString();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path vallidFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));

        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, nonTAACountryCodes, true, now))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .expectComplete()
                .verify();

    }


    @Test
    void testParseXsbFileForcedQuit() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.getForceQuit()).thenReturn(true);
        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path vallidFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, nonTAACountryCodes, true, null))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
        when(errorHandler.getForceQuit()).thenReturn(false);
    }


    @Test
    void testParsingValidFileReturnsImmediatelyWhenTooManyErrors() throws IOException {
        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path vallidFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(false);
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, nonTAACountryCodes, true, null))
                .expectNextCount(0)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void testParseXsbFiles() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path[] srcFiles = {
                Path.of("junitTestData/emptyFile_1.gsa"),
                Path.of("junitTestData/invalid.gsa"),
                Path.of("junitTestData/testFileWithInvalidHeader.gsa"),
                Path.of("junitTestData/testFileWithErrors.gsa"),
                Path.of("junitTestData/testFileWithJustHeader.gsa"),
                Path.of("junitTestData/testValidFile.gsa")
        };

        Path[] filesToParse = {
                Path.of("tmp/emptyFile_1.gsa"),
                Path.of("tmp/invalid.gsa"),
                Path.of("tmp/testFileWithInvalidHeader.gsa"),
                Path.of("tmp/testFileWithErrors.gsa"),
                Path.of("tmp/testFileWithJustHeader.gsa"),
                Path.of("tmp/testValidFile.gsa")
        };

        Files.copy(srcFiles[0], filesToParse[0]);
        // Don't copy file at index 1, junitTestData/invalid.gsa. As this is an invalid file and does not exist.
        Files.copy(srcFiles[2], filesToParse[2]);
        Files.copy(srcFiles[3], filesToParse[3]);
        Files.copy(srcFiles[4], filesToParse[4]);
        Files.copy(srcFiles[5], filesToParse[5]);

        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(Flux.empty(), nonTAACountryCodes, false, null))
                .verifyComplete();

        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(Flux.fromIterable(Arrays.asList(filesToParse)), nonTAACountryCodes, true, null))
                .expectNextCount(27)
                .verifyComplete();

        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(Mockito.anyString(), matches("Ignoring File.*"), Mockito.any(NoSuchFileException.class) );
        Mockito.verify(errorHandler, Mockito.times(2)).handleFileError(Mockito.anyString(), matches("Ignoring File.*"), Mockito.any(NoSuchElementException.class) );
        Mockito.verify(errorHandler, Mockito.times(4)).handleParsingError(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

    }


    @Test
    void testParseXsbFilesWithoutGsaFeedDate() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path[] srcFiles = {
                Path.of("junitTestData/emptyFile_1.gsa"),
                Path.of("junitTestData/invalid.gsa"),
                Path.of("junitTestData/testFileWithInvalidHeader.gsa"),
                Path.of("junitTestData/testFileWithErrors.gsa"),
                Path.of("junitTestData/testFileWithJustHeader.gsa"),
                Path.of("junitTestData/testValidFile.gsa")
        };

        Path[] filesToParse = {
                Path.of("tmp/emptyFile_1.gsa"),
                Path.of("tmp/invalid.gsa"),
                Path.of("tmp/testFileWithInvalidHeader.gsa"),
                Path.of("tmp/testFileWithErrors.gsa"),
                Path.of("tmp/testFileWithJustHeader.gsa"),
                Path.of("tmp/testValidFile.gsa")
        };

        Files.copy(srcFiles[0], filesToParse[0]);
        // Don't copy file at index 1, junitTestData/invalid.gsa. As this is an invalid file and does not exist.
        Files.copy(srcFiles[2], filesToParse[2]);
        Files.copy(srcFiles[3], filesToParse[3]);
        Files.copy(srcFiles[4], filesToParse[4]);
        Files.copy(srcFiles[5], filesToParse[5]);


        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(Flux.fromIterable(Arrays.asList(filesToParse)), nonTAACountryCodes, true, null))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .expectNextCount(17)
                .assertNext(xsbData -> assertFalse(xsbData.getXsbData().asString().contains("gsaFeedDate")))
                .expectComplete()
                .verify();

    }


    @Test
    void testParseXsbFilesWithGsaFeedDate() throws IOException {
        LocalDate now = LocalDate.now();
        String expectedGsaFeedDate = now.toString();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        Path[] srcFiles = {
                Path.of("junitTestData/emptyFile_1.gsa"),
                Path.of("junitTestData/invalid.gsa"),
                Path.of("junitTestData/testFileWithInvalidHeader.gsa"),
                Path.of("junitTestData/testFileWithErrors.gsa"),
                Path.of("junitTestData/testFileWithJustHeader.gsa"),
                Path.of("junitTestData/testValidFile.gsa")
        };

        Path[] filesToParse = {
                Path.of("tmp/emptyFile_1.gsa"),
                Path.of("tmp/invalid.gsa"),
                Path.of("tmp/testFileWithInvalidHeader.gsa"),
                Path.of("tmp/testFileWithErrors.gsa"),
                Path.of("tmp/testFileWithJustHeader.gsa"),
                Path.of("tmp/testValidFile.gsa")
        };

        Files.copy(srcFiles[0], filesToParse[0]);
        // Don't copy file at index 1, junitTestData/invalid.gsa. As this is an invalid file and does not exist.
        Files.copy(srcFiles[2], filesToParse[2]);
        Files.copy(srcFiles[3], filesToParse[3]);
        Files.copy(srcFiles[4], filesToParse[4]);
        Files.copy(srcFiles[5], filesToParse[5]);


        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(Flux.fromIterable(Arrays.asList(filesToParse)), nonTAACountryCodes, true, now))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .expectNextCount(17)
                .assertNext(xsbData -> assertTrue(xsbData.getXsbData().asString().contains("\"gsaFeedDate\":\""+expectedGsaFeedDate+"\"")))
                .expectComplete()
                .verify();

    }


    @Test
    void testSaveNullDataRecordToStaging() {
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(null))
                .expectComplete()
                .verify();
    }

    @Test
    void testSaveInvalidDataRecordToStaging() {
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenThrow(new RuntimeException("Dummy"));
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
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
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
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
    void testSaveInvalidDataRecordToStagingForcedQuit() {
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.error(new RuntimeException("Dummy")));
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.getForceQuit()).thenReturn(true);
        XsbData xsbData = new XsbData();
        xsbData.setContractNumber("contract number");
        xsbData.setManufacturer("manufacturer");
        xsbData.setPartNumber("part number");
        xsbData.setXsbData(Json.of("{\"dummy\": \"string\"}"));
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(eq(xsbData), eq("Dummy"));
        when(errorHandler.getForceQuit()).thenReturn(false);
    }


    @Test
    void testSaveDataRecordToStaging() {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~~|~";
        XsbData xsbData = xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", nonTAACountryCodes, null);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectNext()
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(any(XsbData.class), anyString());
    }


    @Test
    void testSaveDataRecordToStagingForcedQuit() {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.getForceQuit()).thenReturn(true);
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";

        assertThrows(NullPointerException.class, ()->xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", nonTAACountryCodes, null), "ignore");

        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(null))
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(any(XsbData.class), anyString());
        when(errorHandler.getForceQuit()).thenReturn(false);
    }



    @Test
    void testNothingToMoveFromStagingToFinal() {
        Trigger trigger = new Trigger();
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 0))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.never()).totalErrorsWithinAcceptableThreshold();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(eq(""), anyString(), any(Exception.class));
    }


    @Test
    void testDontMoveDataFromStagingToFinal() {
        Trigger trigger = new Trigger();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(false);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), anyString(), any(Exception.class));
    }

    @Test
    void testDontMoveDataFromStagingToFinalForceQuit() {
        Trigger trigger = new Trigger();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.getForceQuit()).thenReturn(true);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(eq(""), anyString(), any(Exception.class));
        when(errorHandler.getForceQuit()).thenReturn(true);
    }



    @Test
    void testDontMoveDataFromStagingToFinal_totalErrorsWithinAccepatableThresholdException() {
        String msg = "Moving 1 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();

        Mockito.verify(errorHandler, Mockito.times(1)).totalErrorsWithinAcceptableThreshold();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }


    @Test
    void testDontMoveDataFromStagingToFinal_onlyStage() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setOnlyStageData(Boolean.TRUE);
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();

        Mockito.verify(errorHandler, Mockito.times(0)).totalErrorsWithinAcceptableThreshold();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(0)).handleFileError(eq(""), eq(errMsg), eq(e));
    }


    @Test
    void testDontMoveDataFromStagingToFinal_totalErrorsWithinAcceptableThresholdException() {
        String msg = "Moving 11 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 11))
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_deleteAllException() {
        String msg = "Moving 5 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.TRUE);
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 5))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_deleteAllError() {
        String msg = "Moving 50 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Exception e = new Exception("Dummy");
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.TRUE);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.error(e));
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 50))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_movXsbDataException() {
        String msg = "Moving 1 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.TRUE);
        Exception e = new IllegalArgumentException("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), Mockito.any(IllegalArgumentException.class));
    }

    @Test
    void testMoveDataFromStagingToFinal_moveXsbDataError() {
        String msg = "Moving 1 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.TRUE);
        Exception e = new Exception("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(e));
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), eq(e));
    }

    @Test
    void testMoveDataFromStagingToFinal_onlyStageData() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setOnlyStageData(Boolean.TRUE);
        Exception e = new Exception("Dummy");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(e));
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.times(0)).handleFileError(eq(""), eq(errMsg), eq(e));
    }


    @Test
    void testMoveDataFromStagingToFinal_movXsbDataSuccess() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.TRUE);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();
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

        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(eq(""), eq(errMsg), any(Exception.class));
    }

    @Test
    void testMoveDataFromStagingToFinal_forceReplaceRollback() {
        String msg = "Moving 12 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.TRUE);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(new RuntimeException("testUpdate_InvalidNumberOfPartitions")));

        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 12))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(""), eq(errMsg), any(Exception.class));
    }

    @Test
    void testMoveDataFromStagingToFinal_ForceUpdateRollback() {
        String msg = "Moving 7 product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Trigger trigger = new Trigger();
        trigger.setPurgeOldData(Boolean.FALSE);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenThrow(new RuntimeException("Dummy"));
        StepVerifier.create(analysisDataProcessingService.moveDataFromStagingToFinal(trigger, 7))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
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
    void testDeleteOldStagingDataUsingTrigger() {
        Trigger trigger = new Trigger();
        trigger.setForceDeleteStagedData(Boolean.TRUE);
        DataUploadResults expectedResults = new DataUploadResults();

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();
    }


    @Test
    void testDeleteOldStagingDataExceptionUsingTrigger() {
        Exception e = new RuntimeException("Dummy");
        Trigger trigger = new Trigger();
        trigger.setForceDeleteStagedData(Boolean.TRUE);
        DataUploadResults expectedResults = new DataUploadResults();

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(RuntimeException.class)
                .verify();

    }


    @Test
    void testDeleteOldStagingDataErrorUsingTrigger() {
        Exception e = new RuntimeException("Dummy");
        Trigger trigger = new Trigger();
        trigger.setForceDeleteStagedData(Boolean.TRUE);
        DataUploadResults expectedResults = new DataUploadResults();

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.error(e));
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(RuntimeException.class)
                .verify();

    }


    @Test
    void testFindNonTaaCompliantCountries_Exception() {
        Exception e = new RuntimeException("Dummy");
        when(xsbDataRepository.findNonTAACompliantCountries()).thenThrow(e);
        StepVerifier.create(analysisDataProcessingService.findNonTaaCompliantCountries())
                .expectError()
                .verify();
    }


    @Test
    void testFindNonTaaCompliantCountries_Error() {
        Exception e = new RuntimeException("Dummy");
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.error(e));
        StepVerifier.create(analysisDataProcessingService.findNonTaaCompliantCountries())
                .expectError()
                .verify();
    }

    @Test
    void testFindNonTaaCompliantCountries_noCountries() {
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Set.of()));
        StepVerifier.create(analysisDataProcessingService.findNonTaaCompliantCountries())
                .expectNext(Set.of());
    }


    @Test
    void testFindNonTaaCompliantCountries() {
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
        StepVerifier.create(analysisDataProcessingService.findNonTaaCompliantCountries())
                .expectNext(Set.of("AF", "AG", "AM", "AO", "AT"))
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
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        doCallRealMethod().when(errorHandler).setForceQuit(anyBoolean());
        doCallRealMethod().when(errorHandler).getForceQuit();
        doCallRealMethod().when(errorHandler).getErrorFiles();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")).delayElements(Duration.ofSeconds(5)));
        when(xsbSourceS3Files.getAnalyzedCatalogs(anyString(), anySet(), anyString())).thenReturn(Flux.just(Path.of("dummy")));
        errorHandler.setErrorDirectory(errorDirectory);

        Mono<DataUploadResults> mono = analysisDataProcessingService.triggerDataUpload(trigger);
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(() -> {
            log.info("Triggered first time");
            mono.subscribe(results -> log.info("First process results {}", results), ex -> log.error("Unexpected Error", ex));
        });

        Thread.sleep(1000);

        log.info("Triggered second time");
        try {
            StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                    .verifyError(ConcurrentModificationException.class);
            fail("Test Failed: Expected a ConcurrentModificationException");
        } catch (ConcurrentModificationException ex) {
            log.info("Test Passed: Expected ConcurrentModificationException");
            assertEquals("Process is currently running!", ex.getMessage());
        } catch (Exception exception){
            fail("Test Failed: Expected a ConcurrentModificationException");
        }

        service.awaitTermination(20, TimeUnit.SECONDS);
        Thread.sleep(10000);
        try {
            log.info("Triggered third time");
            when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
            StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                    .verifyError(RuntimeException.class);
            log.info("Test Passed again: Did not expect a ConcurrentModificationException now");
        } catch (ConcurrentModificationException ex) {
            fail("ConcurrentModificationException Should NOT have been thrown now.");
        }
    }

    @Test
    void testTriggerDataUpload_alreadyExecuting_2() throws InterruptedException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        doCallRealMethod().when(errorHandler).setForceQuit(anyBoolean());
        doCallRealMethod().when(errorHandler).getForceQuit();
        doCallRealMethod().when(errorHandler).getErrorFiles();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")).delayElements(Duration.ofSeconds(5)));
        when(xsbSourceS3Files.getAnalyzedCatalogs(anyString(), anySet(), anyString())).thenReturn(Flux.just(Path.of("dummy")));
        errorHandler.setErrorDirectory(errorDirectory);

        Mono<DataUploadResults> mono = analysisDataProcessingService.triggerDataUpload(trigger);
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(() -> {
            log.info("Triggered first time");
            mono.subscribe(results -> log.info("First process results {}", results), ex -> log.error("Unexpected Error", ex));
        });

        Thread.sleep(1000);

        log.info("Triggered second time");
        try {
            StepVerifier.create(analysisDataProcessingService.triggerDataUpload(null))
                    .verifyError(ConcurrentModificationException.class);
            fail("Test Failed: Expected a ConcurrentModificationException");
        } catch (ConcurrentModificationException ex) {
            log.info("Test Passed: Expected ConcurrentModificationException");
            assertEquals("Process is currently running!", ex.getMessage());
        } catch (Exception exception){
            fail("Test Failed: Expected a ConcurrentModificationException");
        }

        service.awaitTermination(20, TimeUnit.SECONDS);
        Thread.sleep(10000);
        try {
            log.info("Triggered third time");
            when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
            StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                    .verifyError(RuntimeException.class);
            log.info("Test Passed again: Did not expect a ConcurrentModificationException now");
        } catch (ConcurrentModificationException ex) {
            fail("ConcurrentModificationException Should NOT have been thrown now.");
        }
    }



    @Test
    void testTriggerDataupload_simultaneousThreads() throws InterruptedException {
        Trigger trigger = new Trigger () {
            @Override
            public Set<String> getUniqueFileNames(){
                try {
                    log.info("Dummy subclass, sleeping 10 seconds to create a race condition");
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.getUniqueFileNames();
            }
        };


        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("emptyFile_1.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        doCallRealMethod().when(errorHandler).setForceQuit(anyBoolean());
        doCallRealMethod().when(errorHandler).getForceQuit();
        doCallRealMethod().when(errorHandler).getErrorFiles();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
        when(xsbSourceS3Files.getAnalyzedCatalogs(anyString(), anySet(), anyString())).thenReturn(Flux.just(Path.of("dummy")));
        errorHandler.setErrorDirectory(errorDirectory);

        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(() -> {
            Mono<DataUploadResults> mono = analysisDataProcessingService.triggerDataUpload(trigger);
            log.info("Triggered first time");
            mono.subscribe(results -> log.info("Results: " + results), ex -> log.error("Unexpected Error", ex));
        });
        Thread.sleep(2000);

        log.info("Triggered second time");
        try {
            Mono<DataUploadResults> mono2 = analysisDataProcessingService.triggerDataUpload(trigger);
            assertNull(mono2);
            fail("Test Failed: Expected a ConcurrentModificationException");
        } catch (ConcurrentModificationException ex) {
            assertEquals("Process is currently running! Cannot run more than one data uploads at the same time.", ex.getMessage());
            log.info("Test Passed: Expected ConcurrentModificationException");
        } catch (Exception exception){
            fail("Test Failed: Expected a ConcurrentModificationException");
        }

        service.awaitTermination(20, TimeUnit.SECONDS);
       //Thread.sleep(10000);

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
        trigger.setGsaFeedDate(LocalDate.now());
        Exception e = new IllegalArgumentException("Dummy");

        doThrow(e).when(errorHandler).init(anyString());


        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .verifyError(IllegalArgumentException.class);

        //assertEquals("Dummy", thrown.getMessage());
    }

    @Test
    void testTriggerOnlyMoveStagedData() {
        try {
            Trigger t = new Trigger();
            t.setOnlyMoveStagedData(Boolean.TRUE);
            Trigger.validate(t);
        }
        catch (Exception e){
            fail ("Unexpected Error thrown.", e );
        }
    }


    @Test
    void testTriggerNullGsaFeedDate() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        assertThrows(IllegalArgumentException.class, () -> Trigger.validate(trigger), "Trigger argument must include a valid GSA Feed Date in yyyy-MM-dd format. The GSA Feed Date may not be a future date.");
    }

    @Test
    void testTriggerDataUpload_EmptyUniqueFileNames() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        trigger.setUniqueFileNames(uniqueFileNames);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Trigger.validate(trigger));
        assertEquals("Trigger argument must include files attribute (an array with file names or file name patterns).", e.getMessage());
    }

    @Test
    void testTriggerFutureGsaFeedDate() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now().plusDays(1));
        assertThrows(IllegalArgumentException.class, () -> Trigger.validate(trigger), "Trigger argument must include a valid GSA Feed Date in yyyy-MM-dd format. The GSA Feed Date may not be a future date.");
    }


    @Test
    void testBothOnlyStageDataAndOnlyMoveStagedDataAreTrue() {
        try {
            Trigger t = new Trigger();
            t.setOnlyMoveStagedData(Boolean.TRUE);
            t.setOnlyStageData(Boolean.TRUE);
            Trigger.validate(t);
            fail("Should have thrown an IllegalArgumentException");
        }
        catch (IllegalArgumentException e){
            assertEquals("onlyStageData and onlyMoveStagedData cannot be both TRUE simultaneously. Nothing will happen in this case. One (or both) has to be FALSE", e.getMessage());
        }
        catch (Exception e){
            fail ("Unexpected Error thrown.", e );
        }
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
    }

    @Test
    void testValidateTrigger2() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setUniqueFileNames(uniqueFileNames);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB).", e.getMessage());

    }

    @Test
    void testValidateTrigger3() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setUniqueFileNames(uniqueFileNames);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB).", e.getMessage());

    }


    @Test
    void testValidateTrigger4() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        trigger.setGsaFeedDate(LocalDate.now());
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(NullPointerException.class)
                .verify();
    }

    @Test
    void testValidateTrigger5() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setSourceType(Trigger.AnalysisSourceType.S3);
        trigger.setGsaFeedDate(LocalDate.now());
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(NullPointerException.class)
                .verify();
    }

    @Test
    void testValidateTrigger6() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("A valid sourceFolder attribute is required for LOCAL sourceType. Received, null", e.getMessage());
    }

    @Test
    void testValidateTrigger7() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setSourceFolder("");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("A valid sourceFolder attribute is required for LOCAL sourceType. Received, ", e.getMessage());
    }


    @Test
    void testValidateTrigger8() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);

        trigger.setSourceFolder("invalid");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("A valid sourceFolder attribute is required for LOCAL sourceType. Received, invalid", e.getMessage());
    }


    @Test
    void testValidateTrigger9() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setGsaFeedDate(LocalDate.now());

        trigger.setSourceFolder("junitTestData");
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(NullPointerException.class)
                .verify();
    }

    @Test
    void testValidateTrigger10() {
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");

        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        trigger.setUniqueFileNames(null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
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

        trigger.setFiles(new String[]{});
        uniqueFileNames = trigger.getUniqueFileNames();
        assertNull(uniqueFileNames);
        assertEquals(0, trigger.getFiles().length);
    }


    @Test
    void testTriggerDataUpload_deleteOldStagingData_failure() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());
        Exception e = new RuntimeException("Dummy");

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.error(e));

        errorHandler.setErrorDirectory(errorDirectory);

        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(RuntimeException.class)
                .verify();

        Mockito.verify(xsbDataRepository, Mockito.times(1)).findNonTAACompliantCountries();

    }


    @Test
    void testTriggerDataUpload_TAAError() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());
        Exception e = new RuntimeException("Dummy");

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.error(e));

        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());

        errorHandler.setErrorDirectory(errorDirectory);

        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(RuntimeException.class)
                .verify();

        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAllXsbDataTemp();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).findNonTAACompliantCountries();
    }


    @Test
    void testTriggerDataUpload_NoTAACountriesFound() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());

        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.empty());

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(0);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.never()).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();

    }

    @Test
    void testTriggerDataUpload_noFiles() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());

        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(0);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.never()).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();


    }


    @Test
    void testTriggerDataUpload() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        trigger.setPurgeOldData(Boolean.TRUE);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(33);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(33)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(4)).handleParsingError(anyString(), anyString(), anyString());

    }

    @Test
    void testTriggerStageOnlyData() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setOnlyStageData(Boolean.TRUE);
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(33);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(33)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(0)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(4)).handleParsingError(anyString(), anyString(), anyString());

    }

    @Test
    void testTriggerMoveOnlyData() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setOnlyMoveStagedData(Boolean.TRUE);
        trigger.setPurgeOldData(Boolean.FALSE);

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
        when(xsbDataRepository.xsbDataTempCount()).thenReturn(Mono.just(10));


        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(10);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(0)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(0)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();

        Mockito.verify(errorHandler, Mockito.times(0)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(0)).handleParsingError(anyString(), anyString(), anyString());

    }



    @Test
    void testTriggerMoveOnlyData_xsbDataTempCountThrowsException() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setOnlyMoveStagedData(Boolean.TRUE);
        trigger.setPurgeOldData(Boolean.FALSE);

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));
        when(xsbDataRepository.xsbDataTempCount()).thenThrow(new RuntimeException("Dummy"));


        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(10);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectError(RuntimeException.class).verify();

        Mockito.verify(xsbDataRepository, Mockito.times(0)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(0)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();

        Mockito.verify(errorHandler, Mockito.times(0)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(0)).handleParsingError(anyString(), anyString(), anyString());

    }

    @Test
    void testProgressMonitoring() {
        assertEquals(1, analysisDataProcessingService.getProgressReportingIntervalSeconds());

        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        trigger.setPurgeOldData(Boolean.TRUE);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("test*.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenAnswer((Answer<Mono<Void>>) invocationOnMock -> {
            Thread.sleep(200);
            return Mono.empty();
        });

        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());

        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(33);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(33)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(4)).handleParsingError(anyString(), anyString(), anyString());

    }


    @Test
    void testRateLimit(){
        String contractNumber = "4QSMEw_refresh";
        assertEquals("4QSMEw", contractNumber.replace("_refresh", ""));

        //faster
        assertEquals("4QSMEw", contractNumber.substring(0, contractNumber.indexOf("_refresh")));
        contractNumber = "GS-0F-4567T";
        assertEquals("GS-0F-4567T", contractNumber.replace("_refresh", ""));

        int ii = contractNumber.indexOf("_refresh");

        assertEquals("GS-0F-4567T", ii  < 0 ? contractNumber : contractNumber.substring(0, ii));

        contractNumber = "abcdef_refresh";
        ii = contractNumber.indexOf("_refresh");
        assertEquals("abcdef", ii  < 0 ? contractNumber : contractNumber.substring(0, ii));

        contractNumber = "abcdef";
        ii = contractNumber.indexOf("_refresh");
        assertEquals("abcdef", ii  < 0 ? contractNumber : contractNumber.substring(0, ii));



        Flux.range(0, 20)
                .log("first.")
                .flatMap(a -> {
                    if (a == 0) return Mono.just('a').doFinally(s -> log.info("a completed"));
                    if (a == 1) return Mono.just('b').doFinally(s -> log.info("b completed"));
                    if (a == 2) return Mono.just('c').doFinally(s -> log.info("c completed"));
                    if (a == 3) return Mono.just('d').doFinally(s -> log.info("d completed"));
                    if (a == 4) return Mono.just('e').doFinally(s -> log.info("e completed"));
                    if (a == 5) return Mono.just('f').doFinally(s -> log.info("f completed"));
                    if (a == 6) return Mono.just('g').doFinally(s -> log.info("g completed"));
                    if (a == 7) return Mono.just('h').doFinally(s -> log.info("h completed"));
                    if (a == 8) return Mono.just('i').doFinally(s -> log.info("i completed"));
                    if (a == 9) return Mono.just('j').doFinally(s -> log.info("j completed"));
                    if (a == 10) return Mono.error(new RuntimeException("Dummy 10"));
                    return Mono.just('z');

                }, 5)
                .log("second.")
                .doOnComplete(() -> log.info("CompletedCompleted"))
                .onErrorResume(e -> {
                    log.error("ErrorErrorError ", e);
                    return Flux.empty();
                })
                .blockLast();
    }


    @Test
    void testTriggerDataUpload_forceQuitNoProcessRunning() {
        Trigger trigger = new Trigger();
        trigger.setForceQuit(Boolean.TRUE);
        DataUploadResults expected = new DataUploadResults();
        expected.setForcedQuit(Boolean.TRUE);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));
        assertEquals("Trigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or XSB).", e.getMessage());
    }

    @Test
    void testTriggerDataUpload_forceQuitProcessRunning() throws InterruptedException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("emptyFile_1.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        Trigger trigger2 = new Trigger();
        trigger2.setForceQuit(Boolean.TRUE);

        DataUploadResults expected = new DataUploadResults();
        expected.setForcedQuit(Boolean.TRUE);



        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        doCallRealMethod().when(errorHandler).setForceQuit(anyBoolean());
        doCallRealMethod().when(errorHandler).getForceQuit();
        doCallRealMethod().when(errorHandler).getErrorFiles();
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")).delayElements(Duration.ofSeconds(5)));
        when(xsbSourceS3Files.getAnalyzedCatalogs(anyString(), anySet(), anyString())).thenReturn(Flux.just(Path.of("dummy")));
        errorHandler.setErrorDirectory(errorDirectory);

        Mono<DataUploadResults> mono = analysisDataProcessingService.triggerDataUpload(trigger);
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(() -> {
            log.info("Triggered first time");
            mono.subscribe(results -> assertTrue(results.getForcedQuit()), ex -> log.error("Unexpected Error", ex));
        });



        log.info("About to sleep for 2 seconds");
        Thread.sleep(2000);
        log.info("Woke up after 2 second nap. ");

        assertFalse(errorHandler.getForceQuit());
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger2).doOnSuccess(results -> log.info("Results " + results)))
                .expectNext(expected)
                .verifyComplete();
        assertTrue(errorHandler.getForceQuit());

        service.awaitTermination(20, TimeUnit.SECONDS);
        log.info("Thread finished. Wait few more seconds to let the messages pring");
        Thread.sleep(10000);
        log.info("Finished ");

    }

    @Test
    void testGetDataUploadResults_NullErrorHandler() {
        StepVerifier.create(analysisDataProcessingService.getDataUploadResults(null, null, 0))
                .expectError(IllegalArgumentException.class)
                .verify();

        StepVerifier.create(analysisDataProcessingService.getDataUploadResults(null, null, 0))
                .expectErrorMessage("Error Handler cannot be null")
                .verify();
    }

    @Test
    void testTriggerTAACompliance() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        trigger.setPurgeOldData(Boolean.TRUE);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("TS-21F-00015-taa_1234567_20250506123456_123456789_report_1.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());


        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AD", "AE", "AL", "AR", "AZ", "BA", "BN", "BO", "BR", "BW", "BY", "CG", "CI", "CM", "CN", "DZ", "EC", "EG", "EH", "FJ", "GE", "GH", "GP", "ID", "IN", "IQ", "JO", "KE", "KG", "KW", "KZ", "LB", "LK", "LY", "MC", "MH", "MK", "MN", "MO", "MU", "MV", "MY", "NA", "NG", "NR", "NU", "PG", "PH", "PK", "PW", "PY", "QA", "RS", "RU", "SA", "SC", "SM", "SR", "SY", "SZ", "TH", "TJ", "TL", "TM", "TN", "TO", "TR", "UY", "UZ", "VE", "VN", "ZA", "ZW")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(10);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(10)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();

        Mockito.verify(errorHandler, Mockito.times(0)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(0)).handleParsingError(anyString(), anyString(), anyString());

    }

    @Test
    void testTriggerMIACompliance() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        trigger.setPurgeOldData(Boolean.TRUE);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("TS-21F-00015-mia_1234567_20250506123456_123456789_report_1.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());


        doCallRealMethod().when(errorHandler).setErrorDirectory(anyString());
        doCallRealMethod().when(errorHandler).getNumDbErrors();
        doCallRealMethod().when(errorHandler).getNumParsingErrors();
        doCallRealMethod().when(errorHandler).getNumFileErrors();
        doCallRealMethod().when(errorHandler).init(anyString());
        when(errorHandler.getErrorFiles()).thenReturn(Flux.empty());
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findNonTAACompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AD", "AE", "AL", "AR", "AZ", "BA", "BN", "BO", "BR", "BW", "BY", "CG", "CI", "CM", "CN", "DZ", "EC", "EG", "EH", "FJ", "GE", "GH", "GP", "ID", "IN", "IQ", "JO", "KE", "KG", "KW", "KZ", "LB", "LK", "LY", "MC", "MH", "MK", "MN", "MO", "MU", "MV", "MY", "NA", "NG", "NR", "NU", "PG", "PH", "PK", "PW", "PY", "QA", "RS", "RU", "SA", "SC", "SM", "SR", "SY", "SZ", "TH", "TJ", "TL", "TM", "TN", "TO", "TR", "UY", "UZ", "VE", "VN", "ZA", "ZW")));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of());
        expectedResults.setNumRecordsSavedInTempDB(26);
        expectedResults.setNumFileErrors(0);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(0);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(26)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();

        Mockito.verify(errorHandler, Mockito.times(0)).handleFileError(anyString(), anyString(), any());
        Mockito.verify(errorHandler, Mockito.times(0)).handleParsingError(anyString(), anyString(), anyString());

    }

}



