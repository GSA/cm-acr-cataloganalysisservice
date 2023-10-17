package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.util.AcrXsbS3Util;
import gov.gsa.acr.cataloganalysis.util.AcrXsbSftpUtil;
import gov.gsa.acr.cataloganalysis.util.XsbSourceFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class XsbDataService {
    private AtomicBoolean executing = new AtomicBoolean();
    private final XsbDataRepository xsbDataRepository;
    private final ErrorHandler errorHandler;
    private final XsbSourceFactory xsbSourceFactory;

    private final AcrXsbS3Util acrXsbS3Util;

    private final String ls = System.getProperty("line.separator");

    private Flux<String> saveErrorFilesToS3() {
        return errorHandler.getErrorFiles()
                .doFirst(() -> errorHandler.close())
                .flatMap(p -> acrXsbS3Util.uploadToS3(p, "errors/" + p.getFileName()));
    }


    private Mono<Integer> saveXsbDataRecord(XsbData x, ErrorHandler errorHandler) {
        return xsbDataRepository.saveXSBDataToTemp(x.getContractNumber(), x.getManufacturer(), x.getPartNumber(), x.getXsbData())
                // TBD: Retry logic here
                //.retry(5)
                .onErrorResume(e -> {
                        log.error("Error saving record to DB. " + e.getMessage() + " " + x, e);
                        // TBD Error Handler: Possibly a recoverable Error
                        errorHandler.handleDBError (x, e.getMessage());
                        return Mono.empty();
                });

    }


    @Transactional
    private Mono<Void> moveXsbData(Integer numRecordsInStaging, Sinks.Many<String> statusNotifier) {
        return xsbDataRepository.deleteAll()
                .doFirst(() -> {
                    if (statusNotifier != null) {
                        statusNotifier.tryEmitNext("Moving data in bulk from enrichment table to the xsb_data table using transaction ID\n");
                        statusNotifier.tryEmitNext("------------------------------------------------------------------------------------\n");
                    }
                    log.info("Moving data in bulk from enrichment table to the xsb_data table using transaction ID ");
                    log.info("----------------------------------------------");
                })
                .then(xsbDataRepository.moveXsbData());
    }

    private Mono<Void> cleanXsbDataTemp(Sinks.Many<String> statusNotifier) {
        return xsbDataRepository.deleteAllXsbDataTemp().doFirst(() -> {
            if (statusNotifier != null) {
                statusNotifier.tryEmitNext("Cleaning up temporary data from xsb_data_temp\n");
                statusNotifier.tryEmitNext("---------------------------------------------\n");
            }
            log.info("Cleaning up temporary data from xsb_data_temp");
            log.info("----------------------------------------------");
        });
    }


    private Flux<XsbData> parseXsbResponseFile(Long index, Path anXsbResponseFile, ErrorHandler eh, List<String> taaCountryCodes) {
        final String MN = "parseXsbResponseFile: ";

        if (index == 0) {
            XsbReportHandler.resetHeader();
            errorHandler.init(null);
        }


        // First read the header row (First row of the File)
        try (Stream<String> rawProductsFromXSB = Files.lines(anXsbResponseFile)){
            Optional<String> mayBeHeader = rawProductsFromXSB.findFirst();
            if (mayBeHeader.isPresent()) {
                String header = mayBeHeader.get();
                XsbReportHandler.setHeader(index, String.valueOf(anXsbResponseFile), header);
                if (errorHandler.getHeader() == null ) errorHandler.setHeader(header);
            }
            else throw new Exception("Missing header row from file, " + anXsbResponseFile + ". Possibly an empty file.");
        } catch (Exception e) {
            eh.handleFileError(String.valueOf(anXsbResponseFile), "Ignoring File. " + e.getMessage(), e);
            log.error(MN + "Ignoring this file because of the following error: " + e.getMessage(), e );
            return Flux.empty();
        }

        // Now create a Flux of all the lines from the file.
        return Flux.using(
                        () -> Files.lines(anXsbResponseFile).skip(1),
                        Flux::fromStream,
                        Stream::close
                )
                .map(s -> XsbReportHandler.mapRawXsbResponseToXsbDataPojo(String.valueOf(anXsbResponseFile), s, taaCountryCodes))
                .publishOn(Schedulers.parallel())
                .onErrorContinue((e, s) -> eh.handleParsingError(String.valueOf(s), String.valueOf(anXsbResponseFile), e.getMessage())
                );
    }


    private Flux<XsbData> processXSBFiles(Flux<Path> xsbResponseFiles, ErrorHandler errorHandler, List<String> taaCountryCodes, Sinks.Many<String> statusNotifier){
        final String MN = "processXSBFiles: ";
        AtomicInteger counter = new AtomicInteger(0);
        return xsbResponseFiles
                .doOnNext(p -> {
                    log.info(MN + "Processing file: " + p);
                    if (statusNotifier != null)
                        statusNotifier.tryEmitNext("Processing file: " + p + ls);
                })
                .doOnError(e -> {
                    errorHandler.handleFileError("", e.getMessage(), e);
                    log.error(MN + "error: " + e.getMessage(), e);
                    if (statusNotifier != null)
                        statusNotifier.tryEmitNext("error: " + e.getMessage() + ls);
                })
                .doOnComplete(() -> {
                    log.info(MN + "Got all files");
                    if (statusNotifier != null)
                        statusNotifier.tryEmitNext("Got all files"+ls);
                })
                .onErrorContinue((e, o) -> log.error("Got error for a response file, " + o + ". Will Continuew"))
                .index()
                .flatMap(tuple2 -> parseXsbResponseFile(tuple2.getT1(), tuple2.getT2(), errorHandler, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .onErrorContinue((e, o) -> log.error("Got error parsing a response file, " + o + ". Will Continuew"))
                .doOnNext(xsbData -> {
                    if (counter.incrementAndGet() % 1000 == 0) {
                        log.info(MN + "{} ... {} records", xsbData.getSourceXsbDataFileName(), counter.get());
                        if (statusNotifier != null)
                            statusNotifier.tryEmitNext( xsbData.getSourceXsbDataFileName() + " ... "+ counter.get() +" records" + ls);
                    }
                })
                .doOnComplete(() -> {
                    log.info(MN + "Finished. Parsed and converted to JSON a total of {} records", counter.get());
                    if (statusNotifier != null)
                        statusNotifier.tryEmitNext("Finished. Parsed and converted to JSON a total of " +  counter.get() +" records" + ls);
                });
    }


    public void trigger(Trigger trigger, Sinks.Many<String> statusNotifier) {
        if (executing.get()) throw new ConcurrentModificationException("Process is currently running!");
        if (trigger == null) throw new IllegalArgumentException("Invalid request body in POST");
        Set<String> uniqueFileNames = trigger.getUniqueFileNames();
        if (uniqueFileNames == null || uniqueFileNames.isEmpty())  throw new IllegalArgumentException("Request body must include files attribute (an array with file names or file name patterns.");

        AtomicInteger dbCounter = new AtomicInteger(0);

        String tmpdir;
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Flux<Path> filesFromList = xsbSourceFactory.xsbSource(trigger).getXSBFiles(trigger.getSourceFolder(), uniqueFileNames, tmpdir);
        cleanXsbDataTemp(statusNotifier)
                .thenMany(
                        xsbDataRepository
                                .findTaaCompliantCountries()
                                .collectList()
                                .doOnError(e -> log.error("Unable to get a list of TAA compliant country codes. Exiting!", e))
                                .onErrorStop()
                                .flatMapMany(taaCountryCodes ->
                                        processXSBFiles(filesFromList, errorHandler, taaCountryCodes, statusNotifier)
                                )
                                .onBackpressureBuffer()
                                .flatMap(x -> saveXsbDataRecord(x, errorHandler))
                                .doFirst(() -> dbCounter.set(0))
                                .doOnNext(e -> {
                                    if (dbCounter.incrementAndGet() % 1000 == 0) {
                                        log.info("Saved {} records", dbCounter.get());
                                        if (statusNotifier != null)
                                            statusNotifier.tryEmitNext("Saved "+ dbCounter.get() +" records" + ls);
                                    }
                                })
                                .doOnComplete(() -> {
                                    log.info("Finished. Saved a total of {} records", dbCounter.get());
                                    log.info("Number of parsing errors: " + errorHandler.getNumParsingErrors().get());
                                    log.info("Number of db errors: " + errorHandler.getNumDbErrors().get());
                                    log.info("Number of file errors: " + errorHandler.getNumFileErrors().get());
                                    if (statusNotifier != null){
                                        statusNotifier.tryEmitNext("Finished. Saved a total of " + dbCounter.get() + " records." + ls);
                                        statusNotifier.tryEmitNext("Number of parsing errors: " + errorHandler.getNumParsingErrors().get()+ls);
                                        statusNotifier.tryEmitNext("Number of db errors: " + errorHandler.getNumDbErrors().get()+ls);
                                        statusNotifier.tryEmitNext("Number of file errors: " + errorHandler.getNumFileErrors().get()+ls);
                                    }
                                })
                                // TBD only move data if there is something to move and an error threshold has not reached
                                .then(moveXsbData(dbCounter.get(), statusNotifier))
                                .doOnSuccess(s -> {
                                    log.info("All Done!!");
                                    if (statusNotifier != null) {
                                        statusNotifier.tryEmitNext("All Done!!");
                                        statusNotifier.tryEmitComplete();
                                    }
                                })
                                //.doFinally(s -> errorHandler.close())
                                .thenMany(saveErrorFilesToS3())
                )
                .doFirst(() -> executing.compareAndSet(false, true))
                .doFinally(s -> executing.compareAndSet(true, false))
                .subscribe(
                       null, e -> log.error("Unexpected Error", e)
                );
    }


    public Flux<Path> downloadReports(Trigger trigger, Sinks.Many<String> statusNotifier){
        errorHandler.init(null);
        String tmpdir;
        if (trigger == null) return Flux.error(new IllegalArgumentException("Invalid request body in POST"));
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Set<String> uniqueFileNames = trigger.getUniqueFileNames();
        if (uniqueFileNames != null && !uniqueFileNames.isEmpty())
            return xsbSourceFactory.xsbSource(trigger).getXSBFiles(trigger.getSourceFolder(), uniqueFileNames, tmpdir)
                    .doOnComplete(() -> {
                        log.info("Finished downloading all files.");
                        if (statusNotifier != null) statusNotifier.tryEmitComplete();
                    })
                    .doFinally(s -> errorHandler.close());
        else
            return Flux.error(new IllegalArgumentException("Invalid request body in POST. Either an array of file names or a file pattern is requires."));
    }


}
