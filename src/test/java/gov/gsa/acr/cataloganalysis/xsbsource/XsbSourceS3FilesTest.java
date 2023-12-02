package gov.gsa.acr.cataloganalysis.xsbsource;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfigurationProperties;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.xsbsource.XsbSourceS3Files;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Slf4j
@MockBean(ErrorHandler.class)
@ContextConfiguration(classes ={XsbSourceS3Files.class, S3ClientConfiguration.class})
@TestPropertySource(locations="classpath:application-test.properties")
@EnableConfigurationProperties(S3ClientConfigurationProperties.class)
class XsbSourceS3FilesTest {
    @Autowired
    S3ClientConfigurationProperties props;

    @Autowired
    S3ClientConfiguration s3ClientConfiguration;

    @Autowired
    private XsbSourceS3Files xsbSourceS3Files;

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
    void testConfigurationProperties() {
        assertEquals("us-east-1", props.getRegion().toString());
        assertNull(props.getEndpoint());
        assertNull(props.getKey());
        assertNull(props.getSecret());
        assertEquals("gsa-acr-dev-bucket", props.getBucket());
    }


    @Test
    void testS3ClientConfiguration() {
        S3ClientConfiguration s3ClientConfiguration1 = new S3ClientConfiguration();
        assertNotNull(s3ClientConfiguration1.awsCredentialsProvider(props));

        S3ClientConfigurationProperties newProps = new S3ClientConfigurationProperties();
        newProps.setKey("key");
        newProps.setBucket(props.getBucket());
        newProps.setSecret("secret");
        newProps.setRegion(props.getRegion());
        newProps.setEndpoint(props.getEndpoint());
        newProps.setBucket(props.getBaseDir());

        AwsCredentialsProvider awsCredentialsProvider = s3ClientConfiguration1.awsCredentialsProvider(newProps);
        assertNotNull(awsCredentialsProvider);
        awsCredentialsProvider.resolveCredentials();

        newProps.setKey(props.getKey());
        newProps.setSecret(props.getSecret());
        newProps.setEndpoint(URI.create("s3://s3.us-east-1.amazonaws.com"));

        awsCredentialsProvider = s3ClientConfiguration1.awsCredentialsProvider(newProps);
        assertNotNull(awsCredentialsProvider);

        assertNotNull(s3ClientConfiguration1.s3client(newProps, awsCredentialsProvider));

    }

    @Test
    void testScrubbedName() {
        assertEquals("file_name/", xsbSourceS3Files.getScrubbedSourceDir("/file_name/"));
        assertEquals("file_name/", xsbSourceS3Files.getScrubbedSourceDir("????*<><>file_name*/*////*?"));
        assertEquals("file_name/", xsbSourceS3Files.getScrubbedSourceDir("file_name"));
        assertEquals("file_name/", xsbSourceS3Files.getScrubbedSourceDir("/file_name"));
    }

    @Test
    void testGetXSBFiles() {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("getXsbFilesTest_");
        StepVerifier.create(xsbSourceS3Files.getXSBFiles("/junitTestData/", testFileNames, "tmp").map(String::valueOf))
                .expectNextMatches (s -> s.matches("tmp.*getXsbFilesTest_[1-2]\\.gsa"))
                .expectNextMatches (s -> s.matches("tmp.*getXsbFilesTest_[1-2]\\.gsa"))
                .expectComplete()
                .verify();

        try (Stream<Path> files = Files.list(Path.of("tmp"))) {
            assertEquals(2, files.count());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void testOverwritingFile() throws IOException {
        // Artificially add a file that we will try to copy over
        Files.createFile(Path.of ("tmp/getXsbFilesTest_1.gsa"));

        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("getXsbFilesTest_1.gsa");
        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .expectNext(Path.of("tmp/getXsbFilesTest_1.gsa"))
                .expectComplete()
                .verify();

        try (Stream<Path> files = Files.list(Path.of("tmp"))) {
            assertEquals(1, files.count());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void testValidSourceFolder() {
        // Test valid Source Folder
        StepVerifier.create(xsbSourceS3Files.getXSBFiles(null, null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(xsbSourceS3Files.getXSBFiles("", null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(xsbSourceS3Files.getXSBFiles("invalidDirectory", null, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidFileNames() {
        // Test valid Filenames
        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", null, null))
                .expectComplete()
                .verify();

        HashSet<String> testFileNames = new HashSet<>();
        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        // Create a set with more than 20 items, to test the upper limit of 20 items only.
        Set<String> set = IntStream.rangeClosed(Character.MIN_VALUE, Character.MAX_VALUE)
                .filter(Character::isLowerCase)
                .mapToObj(i -> Character.valueOf((char) i).toString())
                .collect(Collectors.toSet());
        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", set, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidDestinationFolder() {
        // Test valid destination folder
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", testFileNames, ""))
                .expectComplete()
                .verify();

        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", testFileNames, "invalidDirectory"))
                .expectComplete()
                .verify();
    }

    @Test
    void testNoMatchingFiles() {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(xsbSourceS3Files.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .expectComplete()
                .verify();
    }


    @Test
    void testDeleteNonExistentObject() {
        // Did not exist before
        StepVerifier.create(xsbSourceS3Files.list("", "non-existent" ))
                        .verifyComplete();
        StepVerifier.create (xsbSourceS3Files.deleteFromS3("non-existent"))
                .expectNext(true)
                .verifyComplete();

        // Does not exist after
        StepVerifier.create(xsbSourceS3Files.list("", "non-existent" ))
                .verifyComplete();

    }


    @Test
    void testUploadToS3AndDeleteFromS3() {
        // Did not exist before
        StepVerifier.create(xsbSourceS3Files.list("errors/", "testErrorFile.txt" ))
                .verifyComplete();

        // upload a test file
        StepVerifier.create (xsbSourceS3Files.uploadToS3(Path.of("junitTestData/testErrorFile.txt"), "errors/testErrorFile.txt"))
                .expectNext("errors/testErrorFile.txt")
                .verifyComplete();

        // Make sure file uploaded correctly
        StepVerifier.create(xsbSourceS3Files.list("errors/", "testErrorFile.txt" ))
                .expectNext("catalogAnalysis/errors/testErrorFile.txt")
                .verifyComplete();


        StepVerifier.create (xsbSourceS3Files.deleteFromS3("errors/testErrorFile.txt"))
                .expectNext(true)
                .verifyComplete();

        // Does not exist after
        StepVerifier.create(xsbSourceS3Files.list("errors/", "testErrorFile.txt" ))
                .verifyComplete();

    }

    @Test
    void testUploadNonExistentFile() {
        StepVerifier.create(xsbSourceS3Files.list("errors/", "non-existent" ))
                .verifyComplete();
        StepVerifier.create (xsbSourceS3Files.uploadToS3(Path.of("non-existent"), "errors/non-existent"))
                .verifyComplete();

        StepVerifier.create(xsbSourceS3Files.list("errors/", "non-existent" ))
                .verifyComplete();

    }
}