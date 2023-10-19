package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

@Component
@Slf4j
public class AcrXsbFilesUtil implements XsbSource {

    private final ErrorHandler errorHandler;

    public AcrXsbFilesUtil(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public static String globToRegex(String string) {
        StringBuilder buffer = new StringBuilder();

        int z = 0;
        while (z < string.length()) {
            switch (string.charAt(z)) {
                case '*' -> {
                    buffer.append(".*");
                    z++;
                }
                case '?' -> {
                    buffer.append('.');
                    z++;
                }
                case '.' -> {
                    buffer.append("\\.");
                    z++;
                }
                case '\\' -> {
                    buffer.append("\\\\");
                    z++;
                }
                case '[', ']', '^', '$', '(', ')', '{', '}', '+', '|' -> {
                    buffer.append('\\');
                    buffer.append(string.charAt(z));
                    z++;
                }
                default -> {
                    buffer.append(string.charAt(z));
                    z++;
                }
            }
        }

        return buffer.toString();
    }


    /**
     * For the file name pattern proviced in the argument, create a stream with all the matching files
     *
     * @param sourceFolder      The local directory to search for the files.
     * @param fileNamePattern   File name for files to search. Could have wildcards (*), in which case all the
     *                          matching files will be downloaded
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of downloaded XSB files
     */
    private Flux<Path> getXSBFiles(String sourceFolder, String fileNamePattern, String destinationFolder) {
        return Flux.using(
                        () -> Files.list(Path.of(sourceFolder)).filter(Files::isRegularFile).filter(p -> p.getFileName().toString().matches(globToRegex(fileNamePattern))),
                        Flux::fromStream,
                        Stream::close)
                .onErrorResume(e -> {
                    log.error("Unable to get XSB files from the directory, " + sourceFolder + ", for file, " + fileNamePattern, e);
                    errorHandler.handleFileError(fileNamePattern, "Unable to get XSB files from the directory, " + sourceFolder + ", for file, " + fileNamePattern, e);
                    return Flux.empty();
                })
                .handle((source, sink) -> {
                    Path destination = Path.of(destinationFolder + "/" + source.getFileName());
                    try {
                        sink.next(Files.copy(source, destination));
                    } catch (Exception e) {
                        log.error("Unable to copy " + source + " to " + destination + ". Will ignore and continue.", e);
                        errorHandler.handleFileError(source.toString(), "Unable to copy " + source + " to " + destination, e);
                    }
                });
    }


    /**
     * Process files already existing on the local file system and generate a stream of paths for these files.
     * The files are first copied to a temporary location before their pats are returned on the stream.If multiple patterns
     * are provided in the fileNames, then each pattern might match multiple files. All these files are collected
     * on the same stream for further processing (parsing, JSON conversion, storing in DB)
     *
     * @param sourceFolder      This is a required field and provides the folder where to search for the files
     * @param fileNames         An array of file names to be processed. Could be file name
     *                          patterns, in which case each pattern might return a list of files
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of all XSB files for all the patterns/ file names provided as the fileNames arg. Each element
     * in the fileNames arg could be a pattern, in which case, the stream collects all the  files into a single stream
     */
    @Override
    public Flux<Path> getXSBFiles(String sourceFolder, Set<String> fileNames, String destinationFolder) {
        final String MN = "getXSBFiles: ";
        if (sourceFolder == null || sourceFolder.isBlank() || (Files.notExists(Path.of(sourceFolder)))) {
            String message = "Invalid Source folder: " + sourceFolder;
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return Flux.empty();
        }
        if (destinationFolder == null || destinationFolder.isBlank() || (Files.notExists(Path.of(destinationFolder)))) {
            String message = "Invalid destination folder: " + destinationFolder;
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return Flux.empty();
        }
        if (unexpectedFileNames(fileNames, log)) return Flux.empty();
        return Flux.fromIterable(fileNames).flatMap(f -> this.getXSBFiles(sourceFolder, f, destinationFolder));
    }
}
