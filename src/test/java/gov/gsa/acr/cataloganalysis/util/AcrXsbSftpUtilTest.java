package gov.gsa.acr.cataloganalysis.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes ={AcrXsbSftpUtil.class, AcrXsbFilesUnitTestConfiguration.class})
@TestPropertySource(locations="classpath:application-test.properties")
class AcrXsbSftpUtilTest {

    @Autowired
    private AcrXsbSftpUtil acrXsbSftpUtil
            ;

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
    void getXSBFiles() {
    }


    @Test
    void testValidSourceFolder() {
        // Test valid Source Folder
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles(null, null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("", null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("invalidDirectory", null, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidFileNames() {
        // Test valid Filenames
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", null, null))
                .expectComplete()
                .verify();

        HashSet<String> testFileNames = new HashSet<>();
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        // Create a set with more than 20 items, to test the upper limit of 20 items only.
        Set<String> set = IntStream.rangeClosed(Character.MIN_VALUE, Character.MAX_VALUE)
                .filter(Character::isLowerCase)
                .mapToObj(i -> Character.valueOf((char) i).toString())
                .collect(Collectors.toSet());
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", set, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidDestinationFolder() {
        // Test valid destination folder
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, ""))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, "invalidDirectory"))
                .expectComplete()
                .verify();
    }


    @Test
    void testNoMatchingFiles() {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .expectComplete()
                .verify();
    }

}