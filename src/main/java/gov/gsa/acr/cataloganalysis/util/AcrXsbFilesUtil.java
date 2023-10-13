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
public class AcrXsbFilesUtil implements XsbSource{

    private final ErrorHandler errorHandler;

    public AcrXsbFilesUtil(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public static String globToRegex(String string) {
        StringBuilder buffer = new StringBuilder();

        int z = 0;
        while (z < string.length()) {
            switch (string.charAt(z)) {
                case '*':
                    buffer.append(".*");
                    z++;
                    break;
                case '?':
                    buffer.append('.');
                    z++;
                    break;
                case '.':
                    buffer.append("\\.");
                    z++;
                    break;
                case '\\':
                    buffer.append("\\\\");
                    z++;
                    break;
                case '[':
                case ']':
                case '^':
                case '$':
                case '(':
                case ')':
                case '{':
                case '}':
                case '+':
                case '|':
                    buffer.append('\\');
                    buffer.append(string.charAt(z));
                    z++;
                    break;
                default:
                    buffer.append(string.charAt(z));
                    z++;
                    break;
            }
        }

        return buffer.toString();
    }

    private Flux<Path> getXSBFiles(String sourceFolder, String fileNamePattern, String destinationFolder){
        return Flux.using(
                () ->  Files.list(Path.of(sourceFolder)).filter(Files::isRegularFile).filter(p->p.getFileName().toString().matches(globToRegex(fileNamePattern))),
                Flux::fromStream,
                Stream::close
        ).handle((source, sink) -> {
            Path destination = Path.of(destinationFolder + "/" + source.getFileName());
            try {
                sink.next(Files.copy(source, destination));
            } catch (Exception e) {
                log.error("Unable to copy " + source + " to " + destination + ". Will ignore and continue.", e);
                errorHandler.handleFileError(source.toString(), "Unable to copy " + source + " to " + destination, e);
            }
        });
    }


    @Override
    public Flux<Path> getXSBFiles(String sourceFolder, Set<String> fileNames, String destinationFolder) {
        final String MN = "getXSBFiles: ";
        if (sourceFolder == null || sourceFolder.isBlank()) {
            Exception e = new IllegalArgumentException("Invalid Source folder: null");
            log.error("Invalid Source folder: null");
            return Flux.error(e);
        }
        if (destinationFolder == null || destinationFolder.isBlank()) {
            Exception e = new IllegalArgumentException("Invalid destination folder: null");
            log.error("Invalid Destination folder: null");
            return Flux.error(e);
        }
        if (fileNames == null) {
            Exception e = new IllegalArgumentException("The array must have valid file names, not NULL");
            log.error("Error downloading files from XSB. Null argument provided. ", e);
            return Flux.error(e);
        }
        if (fileNames.isEmpty() || fileNames.size() > 20) {
            Exception e = new IllegalArgumentException("Either too many files to download or no files provided for download. Maximum 20 files are allowed at a time.");
            log.error("Error downloading files from XSB.", e);
            return Flux.error(e);
        }

        return Flux.fromIterable(fileNames)
                .flatMap(f -> this.getXSBFiles(sourceFolder, f, destinationFolder))
                .onErrorContinue((e, o) -> {
                    log.error("Error getting files from " + o, e);
                });
        
    }
}
