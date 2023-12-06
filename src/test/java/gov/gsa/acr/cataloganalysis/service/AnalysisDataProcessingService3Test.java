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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(XsbDataRepository.class), @MockBean(AnalysisSourceXsb.class), @MockBean(AnalysisSourceS3.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  AnalysisDataProcessingService.class, XsbDataParser.class, AnalysisSourceLocal.class, AnalysisSourceFactory.class, TransactionalDataService.class, ErrorHandler.class})
class AnalysisDataProcessingService3Test {
    @Value("${error.file.directory}")
    private String errorDirectory;

    @Autowired
    private ErrorHandler errorHandler;

    @Autowired
    private AnalysisDataProcessingService analysisDataProcessingService;

    private int cntr;

    @BeforeEach
    void setUp() {
        cntr = 0;
    }

    @Test
    void testGenerateFinalReport_NoErrors() {
        errorHandler.setErrorDirectory(errorDirectory);
        errorHandler.init("Dummy Header");

        List<String> report = analysisDataProcessingService.generateFinalReport(10);

        assertEquals(4, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("INFO:Saved 10 records in the ACR DB.", report.get(cntr++));
        assertEquals("INFO:Finished without any issues!!", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
    }


    @Test
    void testGenerateFinalReport_DataUploadFailed() {
        errorHandler.setErrorDirectory(errorDirectory);
        errorHandler.init("Dummy Header");

        errorHandler.setDataUploadFailed(Boolean.TRUE);

        List<String> report = analysisDataProcessingService.generateFinalReport(0);

        assertEquals(3, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("ERROR:Error moving data from staging to final DB table. Please see logs for error reason.", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));

        // Reset the error handler. And the report should be all clean
        cntr = 0;
        errorHandler.init("Dummy Header");
        report = analysisDataProcessingService.generateFinalReport(20);
        assertEquals(4, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("INFO:Saved 20 records in the ACR DB.", report.get(cntr++));
        assertEquals("INFO:Finished without any issues!!", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));

    }


    @Test
    void testGenerateFinalReport_DataUploadFailedErrorFileGenerated() {
        List<String> dummyFileNames = new ArrayList<>();
        errorHandler.setErrorDirectory(errorDirectory);
        errorHandler.init("Dummy Header");

        errorHandler.setDataUploadFailed(Boolean.TRUE);
        errorHandler.handleFileError("", "Dummy Message", new RuntimeException("Dummy Exception"));
        dummyFileNames.add("xsb_error_msg_dummy_0.txt");
        errorHandler.setErrorFileNames(dummyFileNames);

        List<String> report = analysisDataProcessingService.generateFinalReport(0);

        assertEquals(5, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("ERROR:Error moving data from staging to final DB table. Please see logs for error reason.", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 for reasons for any of the errors.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_msg_dummy_0.txt", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
    }


    @Test
    void testGenerateFinalReport_ParsingErrors() {
        List<String> dummyFileNames = new ArrayList<>();
        errorHandler.setErrorDirectory(errorDirectory);
        errorHandler.init("Dummy Header");

        errorHandler.handleParsingError("Dummy record 1", "Dummy File", "Dummy Exception 1");
        errorHandler.handleParsingError("Dummy record 2", "Dummy File", "Dummy Exception 2");
        errorHandler.handleParsingError("Dummy record 3", "Dummy File", "Dummy Exception 3");

        dummyFileNames.add("xsb_error_parse_dummy_0.gsa");
        dummyFileNames.add("xsb_error_parse_dummy_1.gsa");
        errorHandler.setErrorFileNames(dummyFileNames);

        List<String> report = analysisDataProcessingService.generateFinalReport(15);

        assertEquals(8, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("INFO:Saved 15 records in the ACR DB.", report.get(cntr++));
        assertEquals("WARN:Number of parsing errors: 3", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 to get a list of all records that had parsing issues.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_parse_dummy_0.gsa", report.get(cntr++));
        assertEquals("WARN:\txsb_error_parse_dummy_1.gsa", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 for reasons for any of the errors.", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
    }


    @Test
    void testGenerateFinalReport_DbErrors() {
        List<String> dummyFileNames = new ArrayList<>();
        errorHandler.setErrorDirectory(errorDirectory);
        errorHandler.init("Dummy Header");

        XsbData xsbData = new XsbData();

        xsbData.setSourceXsbDataString("Dummy XSB Data 1");
        errorHandler.handleDBError(xsbData, "Dummy error 1");
        xsbData.setSourceXsbDataString("Dummy XSB Data 2");
        errorHandler.handleDBError(xsbData, "Dummy error 2");

        dummyFileNames.add("xsb_error_db_dummy_0.gsa");
        dummyFileNames.add("xsb_error_db_dummy_1.gsa");
        dummyFileNames.add("xsb_error_msg_dummy_0.txt");
        errorHandler.setErrorFileNames(dummyFileNames);

        List<String> report = analysisDataProcessingService.generateFinalReport(25);

        assertEquals(9, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("INFO:Saved 25 records in the ACR DB.", report.get(cntr++));
        assertEquals("WARN:Number of db errors: 2", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 to get a list of all records that had DB issues.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_db_dummy_0.gsa", report.get(cntr++));
        assertEquals("WARN:\txsb_error_db_dummy_1.gsa", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 for reasons for any of the errors.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_msg_dummy_0.txt", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
    }


    @Test
    void testGenerateFinalReport_AllErrors() {
        List<String> dummyFileNames = new ArrayList<>();
        errorHandler.setErrorDirectory(errorDirectory);
        errorHandler.init("Dummy Header");

        errorHandler.setDataUploadFailed(Boolean.TRUE);

        errorHandler.handleParsingError("Dummy record 1", "Dummy File", "Dummy Exception 1");
        errorHandler.handleParsingError("Dummy record 2", "Dummy File", "Dummy Exception 2");
        errorHandler.handleParsingError("Dummy record 3", "Dummy File", "Dummy Exception 3");

        dummyFileNames.add("xsb_error_parse_dummy_0.gsa");
        dummyFileNames.add("xsb_error_parse_dummy_1.gsa");

        XsbData xsbData = new XsbData();

        xsbData.setSourceXsbDataString("Dummy XSB Data 1");
        errorHandler.handleDBError(xsbData, "Dummy error 1");
        xsbData.setSourceXsbDataString("Dummy XSB Data 2");
        errorHandler.handleDBError(xsbData, "Dummy error 2");

        dummyFileNames.add("xsb_error_db_dummy_0.gsa");
        dummyFileNames.add("xsb_error_db_dummy_1.gsa");
        dummyFileNames.add("xsb_error_msg_dummy_0.txt");
        dummyFileNames.add("xsb_error_msg_dummy_1.txt");
        errorHandler.setErrorFileNames(dummyFileNames);

        List<String> report = analysisDataProcessingService.generateFinalReport(25);

        assertEquals(14, report.size());
        assertEquals("INFO:===================== Final Report =====================", report.get(cntr++));
        assertEquals("ERROR:Error moving data from staging to final DB table. Please see logs for error reason.", report.get(cntr++));
        assertEquals("WARN:Number of parsing errors: 3", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 to get a list of all records that had parsing issues.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_parse_dummy_0.gsa", report.get(cntr++));
        assertEquals("WARN:\txsb_error_parse_dummy_1.gsa", report.get(cntr++));
        assertEquals("WARN:Number of db errors: 2", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 to get a list of all records that had DB issues.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_db_dummy_0.gsa", report.get(cntr++));
        assertEquals("WARN:\txsb_error_db_dummy_1.gsa", report.get(cntr++));
        assertEquals("WARN:Please see the below file(s) saved in S3 for reasons for any of the errors.", report.get(cntr++));
        assertEquals("WARN:\txsb_error_msg_dummy_0.txt", report.get(cntr++));
        assertEquals("WARN:\txsb_error_msg_dummy_1.txt", report.get(cntr++));
        assertEquals("INFO:========================================================", report.get(cntr++));
    }


}