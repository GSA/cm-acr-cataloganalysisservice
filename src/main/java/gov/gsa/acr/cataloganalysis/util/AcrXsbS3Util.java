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
public class AcrXsbS3Util implements XsbSource {
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

    public Mono<String> uploadToS3(Path source, String destination) {
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

    private Flux<String> list(String sourceFolder, String fileNamePattern) {
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
                        return Flux.empty();
                    });
        } catch (Exception e) {
            log.error("Error while listing files from S3", e);
            return Flux.empty();
        }
    }

    private Mono<Path> downloadFromS3(String key, String destinationFolder) {
        try {
            Path sourcePath = Path.of(key);
            Path destinationPath = Path.of(destinationFolder + "/" + sourcePath.getFileName());
            GetObjectRequest request = GetObjectRequest.builder().bucket(s3config.getBucket()).key(key).build();

            return Mono.fromFuture(s3client.getObject(request, destinationPath))
                    .map(response -> {
                        checkResult(response);
                        return destinationPath;
                    })
                    .doOnSuccess(p -> log.info("Downloaded {} file from S3 to {}", key, p))
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


    String getScrubbedSourceDir(String origSourceDir) {
        if (origSourceDir == null || origSourceDir.isBlank()) return "";
        int beginIndex;
        for (beginIndex = 0; beginIndex < origSourceDir.length() - 1; beginIndex++)
            if (invalidCharacter(origSourceDir, beginIndex)) break;

        int endIndex;
        for (endIndex = origSourceDir.length() - 1; endIndex > 0; endIndex--)
            if (invalidCharacter(origSourceDir, endIndex)) break;

        return origSourceDir.substring(beginIndex, endIndex + 1) + '/';
    }

    private boolean invalidCharacter(String origSourceDir, int index) {
        char c = origSourceDir.charAt(index);
        return c != '"' && c != '*' && c != '<' && c != '>' && c != '?' && c != '|' && c != '\\' && c != '/';
    }


    /**
     * Download files from the S3 bucket and generate a stream of paths of the downloaded files. Since file name
     * could be a glob line pattern, there could be multiple files that may match the pattern
     *
     * @param sourceFolder      An optional source folder to search the files in S3 bucket. If provided, this folder
     *                          will be treated relative to the base folder "catalogAnalysis/". If not provided, then
     *                          the files will be searched in the base folder.
     * @param fileNamePattern   Name of the file to search. The file names are treated as prefix. Wild cards are not
     *                          allowed here.
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of downloaded XSB files
     */
    private Flux<Path> getXSBFiles(String sourceFolder, String fileNamePattern, String destinationFolder) {
        return list(sourceFolder, fileNamePattern).flatMap(k -> downloadFromS3(k, destinationFolder));
    }


    /**
     * Download files from the S3 bucket and generate a stream of paths of the downloaded files. If multiple patterns
     * are provided in the fileNames, then each pattern might match multiple files. All these files are collected
     * on the same stream for further processing (parsing, JSON conversion, storing in DB)
     *
     * @param sourceFolder      An optional source folder to search the files in S3 bucket. If provided, this folder
     *                          will be treated relative to the base folder "catalogAnalysis/". If not provided, then
     *                          the files will be searched in the base folder.
     * @param fileNamePatterns         An array of file names to be downloaded from the XSB server. The file names are treated
     *                          as prefix. Wild cards are not allowed here.
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of all XSB files downloaded for all the patterns/ file names provided as the fileNames arg. Each
     * element in the fileNames arg could be a pattern, in which case, the stream collects all the downloaded files into
     * a single stream
     */
    @Override
    public Flux<Path> getXSBFiles(String sourceFolder, Set<String> fileNamePatterns, String destinationFolder) {
        final String srcDir = getScrubbedSourceDir(sourceFolder);
        if (unexpectedFileNames(fileNamePatterns, log)) return Flux.empty();
        return Flux.fromIterable(fileNamePatterns).flatMap(f -> this.getXSBFiles(srcDir, f, destinationFolder));
    }
}
