package gov.gsa.acr.cataloganalysis.analysissource;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfigurationProperties;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

@SpringBootTest
@Slf4j
@MockBeans({@MockBean(ErrorHandler.class), @MockBean(S3AsyncClient.class)})
@ContextConfiguration(classes ={AnalysisSourceS3.class, S3ClientConfiguration.class})
@TestPropertySource(locations="classpath:application-test.properties")
@EnableConfigurationProperties(S3ClientConfigurationProperties.class)
class AnalysisSourceS3Test {
    @Autowired
    S3ClientConfigurationProperties props;

    @Autowired
    S3ClientConfiguration s3ClientConfiguration;

    @Autowired
    private AnalysisSourceS3 xsbSourceS3Files;

    @Autowired
    S3AsyncClient s3AsyncClient;

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
        assertEquals("file_name/", xsbSourceS3Files.getScrubbedSourceDir("??|\"\\??*<><>file_name*/*////*?"));
        assertEquals("file_name/", xsbSourceS3Files.getScrubbedSourceDir("file_name"));
        assertEquals("file_name/", xsbSourceS3Files.getScrubbedSourceDir("/file_name"));
        assertEquals("", xsbSourceS3Files.getScrubbedSourceDir(null));
        assertEquals("", xsbSourceS3Files.getScrubbedSourceDir(""));
        assertEquals("", xsbSourceS3Files.getScrubbedSourceDir("?"));
        assertEquals("a/", xsbSourceS3Files.getScrubbedSourceDir("a"));
        assertEquals("", xsbSourceS3Files.getScrubbedSourceDir("??|\"\\??*<><>*/*////*?"));
    }

    @Test
    void testValidFileNames() {
        // Test valid Filenames
        StepVerifier.create(xsbSourceS3Files.getAnalyzedCatalogs("junitTestData", null, null))
                .expectComplete()
                .verify();

        HashSet<String> testFileNames = new HashSet<>();
        StepVerifier.create(xsbSourceS3Files.getAnalyzedCatalogs("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        // Create a set with more than 20 items, to test the upper limit of 20 items only.
        Set<String> set = IntStream.rangeClosed(Character.MIN_VALUE, Character.MAX_VALUE)
                .filter(Character::isLowerCase)
                .mapToObj(i -> Character.valueOf((char) i).toString())
                .collect(Collectors.toSet());
        StepVerifier.create(xsbSourceS3Files.getAnalyzedCatalogs("junitTestData", set, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidDestinationFolder() {
        // Test valid destination folder
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(xsbSourceS3Files.getAnalyzedCatalogs("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        StepVerifier.create(xsbSourceS3Files.getAnalyzedCatalogs("junitTestData", testFileNames, ""))
                .expectComplete()
                .verify();

        StepVerifier.create(xsbSourceS3Files.getAnalyzedCatalogs("junitTestData", testFileNames, "invalidDirectory"))
                .expectComplete()
                .verify();
    }

    @Test
    void testNoMatchingFiles() {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(xsbSourceS3Files.getAnalyzedCatalogs("junitTestData", testFileNames, "tmp"))
                .expectComplete()
                .verify();
    }


    @Test
    void testDeleteNonExistentObject() {
        // Mock the deletion
        SdkHttpResponse httpResponse = SdkHttpResponse.builder().statusCode(404).build();
        SdkResponse deleteObjectResponse = DeleteObjectResponse.builder()
                .versionId("ab")
                .sdkHttpResponse(httpResponse)
                .build();
        CompletableFuture<DeleteObjectResponse> future2 = CompletableFuture.supplyAsync(() ->
                (DeleteObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future2);

        StepVerifier.create (xsbSourceS3Files.deleteFromS3("non-existent"))
                .expectNext(false)
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