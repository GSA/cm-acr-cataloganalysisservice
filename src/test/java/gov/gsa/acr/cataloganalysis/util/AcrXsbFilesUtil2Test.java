package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@Slf4j
@MockBean(ErrorHandler.class)
@ContextConfiguration(classes = {AcrXsbFilesUtil.class})
@TestPropertySource(locations="classpath:application-test.properties")
class AcrXsbFilesUtil2Test {

    @Autowired
    private ErrorHandler errorHandler;

    @Autowired
    private AcrXsbFilesUtil acrXsbFilesUtil;

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
    void globToRegex() {
        assertEquals("file.*\\\\\\.gsa", AcrXsbFilesUtil.globToRegex("file*\\.gsa") );
        assertEquals("file\\[\\]\\^\\$\\(\\)\\{\\}\\+\\|\\.gsa", AcrXsbFilesUtil.globToRegex("file[]^$(){}+|.gsa"));
    }


    @Test
    void getXSBFiles_FilesListThrowsException() throws IOException {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("getXsbFilesTest_*.gsa");
        when(Files.list(any())).thenThrow(new RuntimeException("Dummy"));
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("getXsbFilesTest_*.gsa"), eq("Unable to get XSB files from the directory, junitTestData, for file, getXsbFilesTest_*.gsa"), Mockito.any(RuntimeException.class) );
    }

    @Test
    void getXSBFiles_FilesCopyThrowsException() throws IOException {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("getXsbFilesTest_1.gsa");
        when(Files.list(Path.of("junitTestData"))).thenReturn(Stream.of(Path.of("junitTestData/getXsbFilesTest_1.gsa")));
        when(Files.isRegularFile(Path.of("junitTestData/getXsbFilesTest_1.gsa"))).thenReturn(true);
        when(Files.copy(Path.of("junitTestData/getXsbFilesTest_1.gsa"), Path.of("tmp/getXsbFilesTest_1.gsa"), StandardCopyOption.REPLACE_EXISTING)).thenThrow(new RuntimeException("Dummy"));
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq(Path.of("junitTestData/getXsbFilesTest_1.gsa").toString()), eq("Unable to copy "+ Path.of("junitTestData/getXsbFilesTest_1.gsa") + " to "+ Path.of("tmp/getXsbFilesTest_1.gsa")), Mockito.any(RuntimeException.class) );
    }

}