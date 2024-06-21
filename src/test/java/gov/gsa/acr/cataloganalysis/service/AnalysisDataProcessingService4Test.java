package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceFactory;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceLocal;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceS3;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;


@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(XsbDataParser.class), @MockBean(AnalysisSourceLocal.class), @MockBean(ErrorHandler.class), @MockBean(XsbDataRepository.class), @MockBean(AnalysisSourceXsb.class), @MockBean(AnalysisSourceS3.class), @MockBean(TransactionalDataService.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  AnalysisDataProcessingService.class, AnalysisSourceFactory.class})
class AnalysisDataProcessingService4Test {

    @Autowired
    private XsbDataParser xsbDataParser;
    @Autowired
    private ErrorHandler errorHandler;
    @Autowired
    private AnalysisDataProcessingService analysisDataProcessingService;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");


    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectory(Path.of("tmp"));
    }

    @AfterEach
    void tearDown() {
        analysisDataProcessingService.deleteDir(Path.of("tmp"));
    }

    @Test
    void parseXsbFile_XsbDataParserThrowsIllegalArgumentExceptionWithDummyMessage() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataParser.validateHeader(anyString())).thenReturn(true);
        when(xsbDataParser.parseXsbData(any(), any(), any(), any())).thenThrow(new IllegalArgumentException("Dummy"));

        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(IllegalArgumentException.class, ()->xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null), "dummy");


        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path validFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, taaCountryCodes, true, null))
                .expectNextCount(0)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(10)).handleParsingError(Mockito.anyString(),eq(validFile.toString()), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void parseXsbFile_XsbDataParserThrowsIllegalArgumentExceptionWithIgnoreMessage() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataParser.validateHeader(anyString())).thenReturn(true);
        when(xsbDataParser.parseXsbData(any(), any(), any(), any())).thenThrow(new IllegalArgumentException("ignore"));

        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(IllegalArgumentException.class, ()->xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null), "ignore");

        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path validFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, taaCountryCodes, true, null))
                .expectNextCount(0)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(10)).handleParsingError(Mockito.anyString(),eq(validFile.toString()), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void parseXsbFile_XsbDataParserThrowsNullPointerExceptionWithDoNotIgnoreMessage() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataParser.validateHeader(anyString())).thenReturn(true);
        when(xsbDataParser.parseXsbData(any(), any(), any(), any())).thenThrow(new NullPointerException("Do not ignore"));

        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(NullPointerException.class, ()->xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null), "Do not ignore");

        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path validFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, taaCountryCodes, true, null))
                .expectNextCount(0)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.times(10)).handleParsingError(Mockito.anyString(),eq(validFile.toString()), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void parseXsbFile_XsbDataParserThrowsNullPointerExceptionWithIgnoreMessage() throws IOException {
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(xsbDataParser.validateHeader(anyString())).thenReturn(true);
        when(xsbDataParser.parseXsbData(any(), any(), any(), any())).thenThrow(new NullPointerException("ignore"));

        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(NullPointerException.class, ()->xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null), "ignore");

        Path srcFile = Path.of("junitTestData/testValidFile.gsa");
        Path validFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, validFile);
        assertTrue(Files.exists(validFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, taaCountryCodes, true, null))
                .expectNextCount(0)
                .expectComplete()
                .verify();
        Mockito.verify(errorHandler, Mockito.never()).handleParsingError(Mockito.anyString(), eq(validFile.toString()), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleDBError(Mockito.any(XsbData.class), Mockito.anyString());
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class));
    }


    @Test
    void testGenerateFileReport_processCancelled(){
        int cntr = 0;
        when(errorHandler.getDataUploadFailed()).thenReturn(false);
        when(errorHandler.getNumFileErrors()).thenReturn(new AtomicInteger(-1));
        when(errorHandler.getNumParsingErrors()).thenReturn(new AtomicInteger(-1));
        when(errorHandler.getNumDbErrors()).thenReturn(new AtomicInteger(-1));
        // Signale type CANCEL
        List<String> report = analysisDataProcessingService.generateFinalReport(0, SignalType.CANCEL, null, null);

        assertEquals(4, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("INFO:Saved 0 records in the ACR DB.", report.get(cntr++));
        assertEquals("WARN:The process was canceled. Please see the logs.", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
    }


    @Test
    void testGenerateFileReport_NullErrors(){
        int cntr = 0;
        when(errorHandler.getDataUploadFailed()).thenReturn(false);
        when(errorHandler.getNumFileErrors()).thenReturn(null);
        when(errorHandler.getNumParsingErrors()).thenReturn(null);
        when(errorHandler.getNumDbErrors()).thenReturn(null);
        // Signale type CANCEL
        List<String> report = analysisDataProcessingService.generateFinalReport(0, SignalType.CANCEL, null, null);

        assertEquals(4, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("INFO:Saved 0 records in the ACR DB.", report.get(cntr++));
        assertEquals("WARN:The process was canceled. Please see the logs.", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
    }

    @Test
    void testGenerateFileReport_NumFileErrorsGreaterThanZero(){
        int cntr = 0;
        // Creating Arrays of dummy error file names
        String a[] = new String[] { "xsb_error_msg_1.txt", "B", "xsb_error_msg_2.txt", "D" };

        // Getting the list view of Array
        List<String> list = Arrays.asList(a);

        when(errorHandler.getErrorFileNames()).thenReturn(list);
        when(errorHandler.getDataUploadFailed()).thenReturn(false);
        when(errorHandler.getNumFileErrors()).thenReturn(new AtomicInteger(1));
        when(errorHandler.getNumParsingErrors()).thenReturn(null);
        when(errorHandler.getNumDbErrors()).thenReturn(null);
        // Signale type CANCEL
        List<String> report = analysisDataProcessingService.generateFinalReport(0, SignalType.CANCEL, null, null);

        assertEquals(6, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("INFO:Saved 0 records in the ACR DB.", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 for reasons for any of the errors.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_msg_1.txt", report.get(cntr++));
        assertEquals("WARN:\txsb_error_msg_2.txt", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
        assertEquals(6, cntr);
    }

}