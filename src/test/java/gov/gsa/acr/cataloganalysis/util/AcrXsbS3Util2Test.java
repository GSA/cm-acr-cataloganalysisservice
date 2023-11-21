package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfigurationProperties;
import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;


@SpringBootTest
@Slf4j
@MockBeans({@MockBean(ErrorHandler.class), @MockBean(S3AsyncClient.class), @MockBean(S3ClientConfiguration.class)})
@ContextConfiguration(classes ={AcrXsbS3Util.class, S3ClientConfigurationProperties.class })
@TestPropertySource(locations="classpath:application-test.properties")
class AcrXsbS3Util2Test {
    @Autowired
    S3ClientConfiguration s3ClientConfiguration;

    @Autowired
    AcrXsbS3Util acrXsbS3Util;

    @Autowired
    S3AsyncClient s3AsyncClient;

    @Autowired
    ErrorHandler errorHandler;

    //list
    @Test
    void testListFutureThrowsException() {
        CompletableFuture<ListObjectsResponse> future = CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("Dummy exception");
        });
        Mockito.when(s3AsyncClient.listObjects(any(ListObjectsRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.list("source", "pattern"))
                .verifyComplete();
    }


    @Test
    void testListS3ClientThrowsException() {
        Mockito.when(s3AsyncClient.listObjects(any(ListObjectsRequest.class))).thenThrow(new RuntimeException("Another dummy exception"));
        StepVerifier.create(acrXsbS3Util.list("source", "pattern"))
                .verifyComplete();
    }

    @Test
    void testListS3ClientReturnsNullFuture() {
        Mockito.when(s3AsyncClient.listObjects(any(ListObjectsRequest.class))).thenReturn(null);
        StepVerifier.create(acrXsbS3Util.list("source", "pattern"))
                .verifyComplete();
    }

    @Test
    void testListEmpty() {
        CompletableFuture<ListObjectsResponse> future = CompletableFuture.supplyAsync(() -> ListObjectsResponse.builder().build());
        Mockito.when(s3AsyncClient.listObjects(any(ListObjectsRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.list("source", "pattern"))
                .verifyComplete();
    }

    @Test
    void testList() {
        CompletableFuture<ListObjectsResponse> future = CompletableFuture.supplyAsync(() ->
                ListObjectsResponse
                        .builder()
                        .contents(S3Object.builder().key("one").build(), S3Object.builder().key("two").build())
                        .build());
        Mockito.when(s3AsyncClient.listObjects(any(ListObjectsRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.list("source", "pattern"))
                .expectNext("one")
                .expectNext("two")
                .verifyComplete();
    }
    //-----------------------

    //deleteFromS3
    @Test
    void testDeleteFutureThrowsException() {
        CompletableFuture<DeleteObjectResponse> future = CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("Dummy exception");
        });
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .expectNext(false)
                .verifyComplete();
    }


    @Test
    void testDeleteS3ClientThrowsException() {
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenThrow(new RuntimeException("Another dummy exception"));
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testDelete3ClientReturnsNullFuture() {
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(null);
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testDeleteEmpty() {
        CompletableFuture<DeleteObjectResponse> future = CompletableFuture.supplyAsync(() -> DeleteObjectResponse.builder().build());
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testDeleteNullResponse() {
        CompletableFuture<DeleteObjectResponse> future = CompletableFuture.supplyAsync(() -> null);
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .verifyComplete();
    }

    @Test
    void testDeleteNullSdkHttpResponse() {
        SdkResponse deleteObjectResponse = DeleteObjectResponse.builder()
                .versionId("ab")
                .build();
        CompletableFuture<DeleteObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (DeleteObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .expectNext(false)
                .verifyComplete();
    }


    @Test
    void testDeleteFalseResponse() {
        SdkHttpResponse httpResponse = SdkHttpResponse.builder().statusCode(400).build();
        SdkResponse deleteObjectResponse = DeleteObjectResponse.builder()
                .versionId("ab")
                .sdkHttpResponse(httpResponse)
                .build();
        CompletableFuture<DeleteObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (DeleteObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testDelete() {
        SdkHttpResponse httpResponse = SdkHttpResponse.builder().statusCode(200).build();
        SdkResponse deleteObjectResponse = DeleteObjectResponse.builder()
                .versionId("ab")
                .sdkHttpResponse(httpResponse)
                .build();
        CompletableFuture<DeleteObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (DeleteObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.deleteFromS3("fileToDelete"))
                .expectNext(true)
                .verifyComplete();
    }
    //-----------------------

    //uploadToS3
    @Test
    void testUploadFutureThrowsException() {
        CompletableFuture<PutObjectResponse> future = CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("Dummy exception");
        });
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .verifyComplete();
    }


    @Test
    void testUploadS3ClientThrowsException() {
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenThrow(new RuntimeException("Another dummy exception"));
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .verifyComplete();
    }

    @Test
    void testUpload3ClientReturnsNullFuture() {
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenReturn(null);
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .verifyComplete();
    }

    @Test
    void testUploadEmpty() {
        CompletableFuture<PutObjectResponse> future = CompletableFuture.supplyAsync(() -> PutObjectResponse.builder().build());
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .verifyComplete();
    }

    @Test
    void testUploadNullResponse() {
        CompletableFuture<PutObjectResponse> future = CompletableFuture.supplyAsync(() -> null);
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .verifyComplete();
    }

    @Test
    void testUploadNullSdkHttpResponse() {
        SdkResponse deleteObjectResponse = PutObjectResponse.builder()
                .versionId("ab")
                .build();
        CompletableFuture<PutObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (PutObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .verifyComplete();
    }


    @Test
    void testUploadFalseResponse() {
        SdkHttpResponse httpResponse = SdkHttpResponse.builder().statusCode(400).build();
        SdkResponse deleteObjectResponse = PutObjectResponse.builder()
                .versionId("ab")
                .sdkHttpResponse(httpResponse)
                .build();
        CompletableFuture<PutObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (PutObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .verifyComplete();
    }

    @Test
    void testUpload() {
        SdkHttpResponse httpResponse = SdkHttpResponse.builder().statusCode(200).build();
        SdkResponse deleteObjectResponse = PutObjectResponse.builder()
                .versionId("ab")
                .sdkHttpResponse(httpResponse)
                .build();
        CompletableFuture<PutObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (PutObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.uploadToS3(Path.of("fileToUpload"), "destination"))
                .expectNext("destination")
                .verifyComplete();
    }
    //-----------------------


    //downloadFromS3
    @Test
    void testDownloadFutureThrowsException() {
        CompletableFuture<GetObjectResponse> future = CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("Dummy exception");
        });
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("fileToDownload"), eq("Download to local file system from S3 FAILED. Dummy exception" ), Mockito.any(RuntimeException.class) );
    }


    @Test
    void testDownloadS3ClientThrowsException() {
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenThrow(new RuntimeException("Another dummy exception"));
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("fileToDownload"), eq("Download to local file system from S3 FAILED. Another dummy exception" ), Mockito.any(RuntimeException.class) );
    }

    @Test
    void testDownload3ClientReturnsNullFuture() {
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(null);
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("fileToDownload"), eq("Download to local file system from S3 FAILED. future" ), Mockito.any(NullPointerException.class) );

    }

    @Test
    void testDownloadEmpty() {
        CompletableFuture<GetObjectResponse> future = CompletableFuture.supplyAsync(() -> GetObjectResponse.builder().build());
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("fileToDownload"), eq("Download to local file system from S3 FAILED. GetObjectResponse()" ), Mockito.any(RuntimeException.class) );
    }

    @Test
    void testDownloadNullResponse() {
        CompletableFuture<GetObjectResponse> future = CompletableFuture.supplyAsync(() -> null);
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class) );

    }

    @Test
    void testDownloadNullSdkHttpResponse() {
        SdkResponse deleteObjectResponse = GetObjectResponse.builder()
                .versionId("ab")
                .build();
        CompletableFuture<GetObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (GetObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("fileToDownload"), eq("Download to local file system from S3 FAILED. GetObjectResponse(VersionId=ab)" ), Mockito.any(RuntimeException.class) );
    }


    @Test
    void testDownloadFalseResponse() {
        SdkHttpResponse httpResponse = SdkHttpResponse.builder().statusCode(400).build();
        SdkResponse deleteObjectResponse = GetObjectResponse.builder()
                .versionId("ab")
                .sdkHttpResponse(httpResponse)
                .build();
        CompletableFuture<GetObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (GetObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError(eq("fileToDownload"), eq("Download to local file system from S3 FAILED. GetObjectResponse(VersionId=ab)" ), Mockito.any(RuntimeException.class) );
    }

    @Test
    void testDownload() {
        SdkHttpResponse httpResponse = SdkHttpResponse.builder().statusCode(200).build();
        SdkResponse deleteObjectResponse = GetObjectResponse.builder()
                .versionId("ab")
                .sdkHttpResponse(httpResponse)
                .build();
        CompletableFuture<GetObjectResponse> future = CompletableFuture.supplyAsync(() ->
                (GetObjectResponse) deleteObjectResponse);
        Mockito.when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(Path.class))).thenReturn(future);
        StepVerifier.create(acrXsbS3Util.downloadFromS3("fileToDownload", "destination"))
                .expectNext(Path.of("destination/fileToDownload"))
                .verifyComplete();
        Mockito.verify(errorHandler, Mockito.never()).handleFileError(Mockito.anyString(), Mockito.anyString(), Mockito.any(Exception.class) );
    }
    //-----------------------

}