package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes = {AcrXsbFilesUtil.class, AcrXsbFilesUnitTestConfiguration.class})
class AcrXsbFilesUtilTest {
    @Autowired
    private AcrXsbFilesUtil acrXsbFilesUtil;

    @Autowired
    private ErrorHandler errorHandler;

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
    void globToRegex() {
        assertEquals("file.*\\.gsa", AcrXsbFilesUtil.globToRegex("file*.gsa") );
        assertEquals("file.\\.gsa", AcrXsbFilesUtil.globToRegex("file?.gsa"));
        assertEquals("/.*.*/file.\\.gsa", AcrXsbFilesUtil.globToRegex("/**/file?.gsa"));
    }

    @Test
    void getXSBFiles() {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("getXsbFilesTest_*.gsa");
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .expectNext(Path.of("tmp/getXsbFilesTest_1.gsa"))
                .expectNext(Path.of("tmp/getXsbFilesTest_2.gsa"))
                .expectComplete()
                .verify();

        try (Stream<Path> files = Files.list(Path.of("tmp"))) {
            assertEquals(2, files.count());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testErrorWhileCopyingFile() throws IOException {
        // Artificially add a file that we will try to copy over
        Files.createFile(Path.of ("tmp/getXsbFilesTest_1.gsa"));

        String source = Path.of("junitTestData/getXsbFilesTest_1.gsa").toString();
        String destination = Path.of("tmp/getXsbFilesTest_1.gsa").toString();

        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("getXsbFilesTest_1.gsa");
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .expectComplete()
                .verify();

        verify(errorHandler, Mockito.times(1)).handleFileError(eq(source), eq("Unable to copy "+source+" to "+destination), Mockito.any(Exception.class) );

        try (Stream<Path> files = Files.list(Path.of("tmp"))) {
            assertEquals(1, files.count());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testValidSourceFolder() {
        // Test valid Source Folder
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles(null, null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("", null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("invalidDirectory", null, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidFileNames() {
        // Test valid Filenames
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", null, null))
                .expectComplete()
                .verify();

        HashSet<String> testFileNames = new HashSet<>();
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        // Create a set with more than 20 items, to test the upper limit of 20 items only.
        Set<String> set = IntStream.rangeClosed(Character.MIN_VALUE, Character.MAX_VALUE)
                .filter(Character::isLowerCase)
                .mapToObj(i -> Character.valueOf((char) i).toString())
                .collect(Collectors.toSet());
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", set, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidDestinationFolder() {
        // Test valid destination folder
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, ""))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, "invalidDirectory"))
                .expectComplete()
                .verify();
    }

    @Test
    void testNoMatchingFiles() {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(acrXsbFilesUtil.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .expectComplete()
                .verify();
    }



}