package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfigurarionProperties;
import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class AcrXsbS3Util implements XsbSource  {
    private final S3AsyncClient s3client;
    private final S3ClientConfigurarionProperties s3config;

    private final ErrorHandler errorHandler;

    public AcrXsbS3Util(S3AsyncClient s3client, S3ClientConfigurarionProperties s3config, ErrorHandler errorHandler) {
        this.s3client = s3client;
        this.s3config = s3config;
        this.errorHandler = errorHandler;
    }

    private static void checkResult(SdkResponse result) {
        if (result.sdkHttpResponse() == null || !result.sdkHttpResponse().isSuccessful()) {
            throw new RuntimeException(result.toString());
        }
    }

    public Mono<String> uploadToS3(Path source, String destination){
        final String MN = "uploadToS3: ";
        log.info(MN + " saving file {} to S3 at {}", source, destination);
        CompletableFuture<PutObjectResponse> future;
        try {
            future = s3client
                    .putObject(PutObjectRequest.builder()
                                    .bucket(s3config.getBucket())
                                    .key(s3config.getBaseDir() + destination)
                                    .build(), source);
            return Mono.fromFuture(future)
                    .map((response) -> {
                        checkResult(response);
                        return destination;
                    })
                    .onErrorResume(e -> {
                        log.error("Unable to save file to S3.", e);
                        return Mono.empty();
                    });
        } catch (Exception e) {
            log.error("Unable to save file to S3.", e);
            return Mono.empty();
        }

    }

    private Flux<String> list(String sourceFolder, String fileNamePattern){
        try {
            ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
                    .bucket(s3config.getBucket())
                    .prefix(s3config.getBaseDir() + sourceFolder + fileNamePattern)
                    .build();
            CompletableFuture<ListObjectsResponse> future = s3client.listObjects(listObjectsRequest);

            return Mono.fromFuture(future)
                    .flatMapMany(response -> Flux.fromIterable(response.contents()))
                    .map(S3Object::key)
                    .onErrorResume(e -> {
                        log.error("Error while listing files from S3", e);
                        return Mono.empty();
                    });
        } catch (Exception e) {
            log.error("Error while listing files from S3", e);
            return Flux.empty();
        }
    }

    private Mono<Path> downloadFromS3(String key, String destinationFolder){
        try {
            Path sourcePath = Path.of(key);
            Path destinationPath = Path.of(destinationFolder+"/"+sourcePath.getFileName());
            GetObjectRequest request = GetObjectRequest.builder().bucket(s3config.getBucket()).key(key).build();

            return Mono.fromFuture(s3client.getObject(request, destinationPath))
                    .map(response -> {
                        checkResult(response);
                        return destinationPath;
                    })
                    .onErrorResume(e -> {
                      log.error("Error downloading file from S3: " + key);
                      errorHandler.handleFileError(key, "Download to local file system from S3 FAILED. " + e.getMessage(), e);
                      return Mono.empty();
                    });
        } catch (Exception e) {
            log.error("Error downloading the file from S3: " + key, e);
            errorHandler.handleFileError(key, "Download to local file system from S3 FAILED. " + e.getMessage(), e);
            return Mono.empty();
        }
    }


    private Flux<Path> getXSBFiles(String sourceFolder, String fileNamePattern, String destinationFolder){
        return list(sourceFolder, fileNamePattern).flatMap(k -> downloadFromS3(k, destinationFolder));
    }

    private String getScrubbedSourceDir(String origSourceDir){
        if (origSourceDir == null || origSourceDir.isBlank()) return "";
        else if (!origSourceDir.endsWith("/")) return origSourceDir + "/";
        else return origSourceDir;
    }

    @Override
    public Flux<Path> getXSBFiles(String sourceFolder, Set<String> fileNames, String destinationFolder) {
        final String MN = "getXSBFiles: ";
        final String srcDir = getScrubbedSourceDir(sourceFolder);
        if (fileNames == null) {
            String message = "The files array must have valid file names. The files array is null.";
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return Flux.error(e);
        }
        if (fileNames.isEmpty() || fileNames.size() > 20) {
            String message = "Either too many files to download or no files provided for download. Maximum 20 files are allowed at a time.";
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return Flux.error(e);
        }
        return Flux.fromIterable(fileNames).flatMap(f -> this.getXSBFiles(srcDir, f, destinationFolder));
    }
}
