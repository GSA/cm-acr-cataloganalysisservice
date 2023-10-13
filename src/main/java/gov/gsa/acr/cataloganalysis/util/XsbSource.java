package gov.gsa.acr.cataloganalysis.util;

import reactor.core.publisher.Flux;

import java.nio.file.Path;
import java.util.Set;

public interface XsbSource {
    Flux<Path> getXSBFiles(String sourceFolder, Set<String> fileNames, String destinationFolder);
}
