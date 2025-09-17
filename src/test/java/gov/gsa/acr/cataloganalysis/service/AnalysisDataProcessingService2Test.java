package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceFactory;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceLocal;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceS3;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.util.EmailUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(XsbDataParser.class), @MockBean(AnalysisSourceLocal.class), @MockBean(ErrorHandler.class), @MockBean(XsbDataRepository.class), @MockBean(AnalysisSourceXsb.class), @MockBean(AnalysisSourceS3.class), @MockBean(TransactionalDataService.class), @MockBean(EmailUtil.class), @MockBean(JavaMailSender.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  AnalysisDataProcessingService.class, AnalysisSourceFactory.class})
class AnalysisDataProcessingService2Test {
    private MockedStatic<Files> mockedSettings;

    @Autowired
    private AnalysisDataProcessingService analysisDataProcessingService;

    @Autowired
    private ErrorHandler errorHandler;

    Set<String> nonTAACountryCodes = Set.of("AD", "AE", "AL", "AR", "AZ", "BA", "BN", "BO", "BR", "BW", "BY", "CG", "CI", "CM", "CN", "DZ", "EC", "EG", "EH", "FJ", "GE", "GH", "GP", "ID", "IN", "IQ", "JO", "KE", "KG", "KW", "KZ", "LB", "LK", "LY", "MC", "MH", "MK", "MN", "MO", "MU", "MV", "MY", "NA", "NG", "NR", "NU", "PG", "PH", "PK", "PW", "PY", "QA", "RS", "RU", "SA", "SC", "SM", "SR", "SY", "SZ", "TH", "TJ", "TL", "TM", "TN", "TO", "TR", "UY", "UZ", "VE", "VN", "ZA", "ZW");


    @BeforeEach
    void setUp() {mockedSettings = mockStatic(Files.class);}

    @AfterEach
    void tearDown() {mockedSettings.close();}

    @Test
    void testTmpDirectoryCreationError() throws IOException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        when(Files.createTempDirectory(any())).thenThrow(new IOException("Dummy"));
        RuntimeException thrown = assertThrows (RuntimeException.class, () -> analysisDataProcessingService.triggerDataUpload(trigger));

        assertEquals("Unexpected error, cannot create a temporary directory. Cannot proceed without a temporary directory.", thrown.getMessage());
    }

    @Test
    void testDeleteTmpDir_FileListException() throws IOException {
        Path tmpDir = Path.of("tmpDir");
        when(Files.list(tmpDir)).thenThrow(new IOException("Dummy"));
        assertFalse(analysisDataProcessingService.deleteDir(tmpDir));
    }


    @Test
    void testDeleteTmpDir_deleteDir() throws IOException {
        Path tmpDir = Path.of("tmpDir");
        when(Files.isDirectory(tmpDir)).thenReturn(Boolean.TRUE);
        when(Files.list(tmpDir)).thenThrow(new IOException("Dummy"));
        assertFalse(analysisDataProcessingService.deleteDir(tmpDir));
        assertFalse(analysisDataProcessingService.deleteDir(null));
        assertFalse(analysisDataProcessingService.deleteDir(Path.of("")));
    }


    @Test
    void testDeleteTmpDir_FileDelete() throws IOException {
        Path tmpDir = Path.of("tmpDir");
        Path[] files = {Path.of("good"), Path.of("bad"), Path.of("ugly")};
        when(Files.list(tmpDir)).thenReturn(Arrays.stream(files));

        when(Files.deleteIfExists(files[0])).thenReturn(true);
        when(Files.deleteIfExists(files[1])).thenReturn(false);
        when(Files.deleteIfExists(files[2])).thenThrow(new RuntimeException("Could not delete ugly"));
        when(Files.deleteIfExists(tmpDir)).thenThrow(new DirectoryNotEmptyException("tmpDir is not empty"));

        assertTrue (analysisDataProcessingService.deleteFile(files[0]));
        assertFalse (analysisDataProcessingService.deleteFile(files[1]));
        assertFalse (analysisDataProcessingService.deleteFile(files[2]));

        assertFalse(analysisDataProcessingService.deleteDir(tmpDir));
    }


    @Test
    void testDeleteTmpDir_FileDeleteTmpDirException() throws IOException {
        Path tmpDir = Path.of("tmpDir");
        Path[] files = {Path.of("good"), Path.of("bad"), Path.of("ugly")};
        when(Files.list(tmpDir)).thenReturn(Arrays.stream(files));

        when(Files.deleteIfExists(files[0])).thenReturn(true);
        when(Files.deleteIfExists(files[1])).thenReturn(true);
        when(Files.deleteIfExists(files[2])).thenReturn(true);
        when(Files.deleteIfExists(tmpDir)).thenThrow(new RuntimeException("Could not delete tmpDir"));

        assertFalse(analysisDataProcessingService.deleteDir(tmpDir));
    }

    @Test
    void testDeleteTmpDir() throws IOException {
        Path tmpDir = Path.of("tmpDir");
        Path[] files = {Path.of("good"), Path.of("bad"), Path.of("ugly")};
        when(Files.list(tmpDir)).thenReturn(Arrays.stream(files));

        when(Files.deleteIfExists(files[0])).thenReturn(true);
        when(Files.deleteIfExists(files[1])).thenReturn(true);
        when(Files.deleteIfExists(files[2])).thenReturn(true);
        when(Files.isDirectory(tmpDir)).thenReturn(true);
        when(Files.deleteIfExists(tmpDir)).thenReturn(true);

        assertTrue(analysisDataProcessingService.deleteDir(tmpDir));
    }

    @Test
    void testParseXsbFiles_FilesLinesThrowsException() throws IOException {
        Path validFile = Path.of("tmp/testValidFile.gsa");
        when(errorHandler.totalErrorsWithinAcceptableThreshold()).thenReturn(true);
        when(errorHandler.getForceQuit()).thenReturn(false);
        when(Files.lines(any())).thenThrow(new IOException("testParseXsbFiles_FilesLinesThrowsException Dummy exception"));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(validFile, nonTAACountryCodes, true, null))
                .expectComplete()
                .verify();
    }

}