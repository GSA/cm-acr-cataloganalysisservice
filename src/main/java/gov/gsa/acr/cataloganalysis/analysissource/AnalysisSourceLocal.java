package gov.gsa.acr.cataloganalysis.analysissource;

import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Set;
import java.util.stream.Stream;

@Component
@Slf4j
public class AnalysisSourceLocal implements AnalysisSource {
    private final ErrorHandler errHandler;

    public AnalysisSourceLocal(ErrorHandler errHandler) {
        this.errHandler = errHandler;
    }


    /**
     * For the file name pattern provided in the argument, create a stream with all the matching files
     *
     * @param sourceFolder      The local directory to search for the files.
     * @param fileNamePattern   File name for files to search. Could have wildcards (*), in which case all the
     *                          matching files will be downloaded
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of downloaded XSB files
     */
    private Flux<Path> getAnalyzedCatalogs(String sourceFolder, String fileNamePattern, String destinationFolder) {
        return Flux.using(
                        () -> Files.list(Path.of(sourceFolder)).filter(Files::isRegularFile).filter(p -> p.getFileName().toString().matches(StringUtils.globToRegex(fileNamePattern))),
                        Flux::fromStream,
                        Stream::close)
                .onErrorResume(e -> {
                    log.error("Unable to get XSB files from the directory, " + sourceFolder + ", for file, " + fileNamePattern, e);
                    errHandler.handleFileError(fileNamePattern, "Unable to get XSB files from the directory, " + sourceFolder + ", for file, " + fileNamePattern, e);
                    return Flux.empty();
                })
                .handle((source, sink) -> {
                    Path destination = Path.of(destinationFolder + "/" + source.getFileName());
                    try {
                        sink.next(Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING));
                    } catch (Exception e) {
                        log.error("Unable to copy " + source + " to " + destination + ". Will ignore and continue.", e);
                        errHandler.handleFileError(source.toString(), "Unable to copy " + source + " to " + destination, e);
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
     * @param fileNamePatterns         An array of file names to be processed. Could be file name
     *                          patterns, in which case each pattern might return a list of files
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of all XSB files for all the patterns/ file names provided as the fileNames arg. Each element
     * in the fileNames arg could be a pattern, in which case, the stream collects all the  files into a single stream
     */
    @Override
    public Flux<Path> getAnalyzedCatalogs(String sourceFolder, Set<String> fileNamePatterns, String destinationFolder) {
        final String MN = "getAnalyzedCatalogs: ";
        if (sourceFolder == null || sourceFolder.isBlank() || (Files.notExists(Path.of(sourceFolder)))) {
            String message = "Invalid Source folder: " + sourceFolder;
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return Flux.empty();
        }
        if (invalidNumberOfFiles(fileNamePatterns, log)) return Flux.empty();
        if (destinationFolder == null || destinationFolder.isBlank() || (Files.notExists(Path.of(destinationFolder)))) {
            String message = "Invalid destination folder: " + destinationFolder;
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return Flux.empty();
        }
        return Flux.fromIterable(fileNamePatterns).flatMap(f -> this.getAnalyzedCatalogs(sourceFolder, f, destinationFolder), 3);
    }
}
