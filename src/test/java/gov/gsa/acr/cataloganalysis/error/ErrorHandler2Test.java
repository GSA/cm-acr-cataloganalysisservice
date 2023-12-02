package gov.gsa.acr.cataloganalysis.error;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes = {ErrorHandler.class})
@TestPropertySource(locations="classpath:application-test.properties")
class ErrorHandler2Test {
    @Autowired
    private ErrorHandler errorHandler;
    private MockedStatic<Files> mockedSettings;

    @BeforeEach
    public void setUp() {
        mockedSettings = mockStatic(Files.class);
    }

    @AfterEach
    public void tearDown() {
        mockedSettings.close();
    }

    @Test
    void testCreateErrorDirectoryCreationException() throws IOException {
        when(Files.createDirectories(any())).thenThrow(new IOException("Dummy"));
        RuntimeException thrown = assertThrows (RuntimeException.class, () -> errorHandler.init("dummy header"));
        assertEquals("Unexpected error. Unable to create a new directory for storing error files.", thrown.getMessage());
        assertEquals(2000, errorHandler.getMaxErrorFileSizeBytes());
        assertEquals("testData/errors", errorHandler.getErrorDirectory());
        assertEquals(0, errorHandler.getNumParsingErrors().get());
        assertEquals(0, errorHandler.getNumDbErrors().get());
        assertEquals(0, errorHandler.getNumFileErrors().get());
        assertEquals(0, errorHandler.getNumRecordsSavedInTempDB().get());
        assertEquals("dummy header", errorHandler.getHeader());
        assertEquals(2, errorHandler.getErrorThreshold());
    }

    @Test
    void testDeleteOldFilesException() throws IOException {
        when(Files.list(any())).thenThrow(new RuntimeException("Dummy"));
        RuntimeException thrown = assertThrows (RuntimeException.class, () -> errorHandler.init("dummy header"));
        assertEquals("Unexpected error. Unable to delete old error files from previous executions.", thrown.getMessage());
        assertEquals(2000, errorHandler.getMaxErrorFileSizeBytes());
        assertEquals("testData/errors", errorHandler.getErrorDirectory());
        assertEquals(0, errorHandler.getNumParsingErrors().get());
        assertEquals(0, errorHandler.getNumDbErrors().get());
        assertEquals(0, errorHandler.getNumFileErrors().get());
        assertEquals(0, errorHandler.getNumRecordsSavedInTempDB().get());
        assertEquals("dummy header", errorHandler.getHeader());
        assertEquals(2, errorHandler.getErrorThreshold());
    }

    @Test
    void testDeleteOldFileException() throws IOException {
        Path path = Path.of("xsb_error_1.gsa");
        when(Files.list(Path.of("testData/errors"))).thenReturn(Stream.of(path));
        when(Files.isRegularFile(path)).thenReturn(true);
        when(Files.deleteIfExists(any())).thenThrow(new IOException("Dummy"));

        RuntimeException thrown = assertThrows (RuntimeException.class, () -> errorHandler.init("dummy header"));
        log.error("This is what we got ", thrown);
        assertEquals("Unexpected error. Unable to delete old error files from previous executions.", thrown.getMessage());
        assertEquals("Unexpected error. Unable to delete old error file from a previous execution: " + path, thrown.getCause().getMessage());
        assertEquals(2000, errorHandler.getMaxErrorFileSizeBytes());
        assertEquals("testData/errors", errorHandler.getErrorDirectory());
        assertEquals(0, errorHandler.getNumParsingErrors().get());
        assertEquals(0, errorHandler.getNumDbErrors().get());
        assertEquals(0, errorHandler.getNumFileErrors().get());
        assertEquals(0, errorHandler.getNumRecordsSavedInTempDB().get());
        assertEquals("dummy header", errorHandler.getHeader());
        assertEquals(2, errorHandler.getErrorThreshold());
    }

    @Test
    void testGetErrorFilesException() throws IOException {
        when(Files.list(any())).thenThrow(new RuntimeException("Dummy"));
        StepVerifier.create(errorHandler.getErrorFiles()).verifyComplete();
    }
}