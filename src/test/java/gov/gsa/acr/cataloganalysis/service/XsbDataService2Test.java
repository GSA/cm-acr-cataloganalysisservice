package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.xsbsource.XsbSourceFactory;
import gov.gsa.acr.cataloganalysis.xsbsource.XsbSourceLocalFiles;
import gov.gsa.acr.cataloganalysis.xsbsource.XsbSourceS3Files;
import gov.gsa.acr.cataloganalysis.xsbsource.XsbSourceSftpFiles;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(XsbDataParser.class), @MockBean(XsbSourceLocalFiles.class), @MockBean(ErrorHandler.class), @MockBean(XsbDataRepository.class), @MockBean(XsbSourceSftpFiles.class), @MockBean(XsbSourceS3Files.class), @MockBean(TransactionalDataService.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  XsbDataService.class, XsbSourceFactory.class})
class XsbDataService2Test {
    private MockedStatic<Files> mockedSettings;

    @Autowired
    private XsbDataService xsbDataService;

    @BeforeEach
    void setUp() {mockedSettings = mockStatic(Files.class);}

    @AfterEach
    void tearDown() {mockedSettings.close();}

    @Test
    void testTmpDirectoryCreationError() throws IOException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.SFTP);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);

        when(Files.createTempDirectory(any())).thenThrow(new IOException("Dummy"));
        RuntimeException thrown = assertThrows (RuntimeException.class, () -> xsbDataService.triggerDataUpload(trigger));

        assertEquals("Unexpected error, cannot create a temporary directory. Cannot proceed without a temporary directory.", thrown.getMessage());
    }

    @Test
    void testDeleteTmpDir_FileListException() throws IOException {
        Path tmpDir = Path.of("tmpDir");
        when(Files.list(tmpDir)).thenThrow(new IOException("Dummy"));

        StepVerifier.create(xsbDataService.deleteTmpDir(tmpDir))
                .expectNext(false)
                .verifyComplete();
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

        StepVerifier.create(xsbDataService.deleteTmpDir(tmpDir))
                .expectNext(false)
                .verifyComplete();
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

        StepVerifier.create(xsbDataService.deleteTmpDir(tmpDir))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testDeleteTmpDir() throws IOException {
        Path tmpDir = Path.of("tmpDir");
        Path[] files = {Path.of("good"), Path.of("bad"), Path.of("ugly")};
        when(Files.list(tmpDir)).thenReturn(Arrays.stream(files));

        when(Files.deleteIfExists(files[0])).thenReturn(true);
        when(Files.deleteIfExists(files[1])).thenReturn(true);
        when(Files.deleteIfExists(files[2])).thenReturn(true);
        when(Files.deleteIfExists(tmpDir)).thenReturn(true);

        StepVerifier.create(xsbDataService.deleteTmpDir(tmpDir))
                .expectNext(true)
                .verifyComplete();
    }


    @Test
    void testDownload_ErrorCreatingTmpDir() throws IOException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.SFTP);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);

        when(Files.createTempDirectory(any())).thenThrow(new IOException("Dummy"));
        RuntimeException thrown = assertThrows (RuntimeException.class, () -> xsbDataService.downloadReports(trigger));

        assertEquals("Unexpected Error creating tmp directory", thrown.getMessage());

    }


    @Test
    void testParse_ErrorCreatingTmpDir() throws IOException {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.SFTP);
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("Dummy");
        trigger.setUniqueFileNames(uniqueFileNames);

        when(Files.createTempDirectory(any())).thenThrow(new IOException("Dummy"));
        RuntimeException thrown = assertThrows (RuntimeException.class, () -> xsbDataService.parseXsbFiles(trigger));

        assertEquals("Unexpected Error creating tmp directory", thrown.getMessage());
    }

}