package gov.gsa.acr.cataloganalysis.analysissource;

import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.nio.file.Path;
import java.util.Set;

public interface AnalysisSource {
    Flux<Path> getAnalyzedCatalogs(String sourceFolder, Set<String> fileNamePatterns, String destinationFolder);
    default boolean invalidNumberOfFiles(Set<String> fileNames, Logger log) {
        final String MN = "getAnalyzedCatalogs: ";
        if (fileNames == null) {
            String message = "The files array must have valid file names. The files array is null.";
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return true;
        }
        if (fileNames.isEmpty() || fileNames.size() > 20) {
            String message = "Either too many files to download or no files provided for download. Maximum 20 files are allowed at a time.";
            Exception e = new IllegalArgumentException(message);
            log.error(MN + message, e);
            return true;
        }
        return false;
    }
}
