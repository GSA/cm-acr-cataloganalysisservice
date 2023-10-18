package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.util.AcrXsbS3Util;
import gov.gsa.acr.cataloganalysis.util.XsbSourceFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
    private final AtomicBoolean executing = new AtomicBoolean();
    private final XsbDataRepository xsbDataRepository;
    private final ErrorHandler errorHandler;
    private final XsbSourceFactory xsbSourceFactory;

    private final AcrXsbS3Util acrXsbS3Util;

    private final String ls = System.getProperty("line.separator");


    private Mono<Boolean> deleteTmpDir(Path tmpDir){
        return Flux.using(
                        () -> Files.list(tmpDir),
                        Flux::fromStream,
                        Stream::close
                )
                .doFirst(() -> log.info("Deleting temporary folder " + tmpDir))
                .handle((source, sink) -> {
                    log.info("Deleting file " + source);
                    try {
                        sink.next(Files.deleteIfExists(source));
                    } catch (Exception e) {
                        log.error("Unable to delete " + source, e);
                    }
                })
                .then(Mono.defer(() -> {
                    try {
                        return Mono.just(Files.deleteIfExists(tmpDir));
                    }
                    catch (Exception e){
                        log.error("Unable to delete the folder " + tmpDir);
                        return Mono.error(e);
                    }
                }))
                .doOnSuccess(b -> log.info("Successfully Deleted folder "+ tmpDir));
    }


    private Flux<String> uploadErrorFilesToS3() {
        return errorHandler.getErrorFiles()
                .doFirst(errorHandler::close)
                .flatMap(p -> acrXsbS3Util.uploadToS3(p, "errors/" + p.getFileName()));
    }


    private Mono<Integer> saveDataRecordToStaging(XsbData x, ErrorHandler errorHandler) {
        return xsbDataRepository.saveXSBDataToTemp(x.getContractNumber(), x.getManufacturer(), x.getPartNumber(), x.getXsbData())
                // TBD: Retry logic here
                //.retry(5)
                .onErrorResume(e -> {
                        log.error("Error saving record to DB. " + e.getMessage() + " " + x, e);
                        errorHandler.handleDBError (x, e.getMessage());
                        return Mono.empty();
                });
    }


    @Transactional
    public Mono<Void> moveDataFromStagingToFinal() {
        return Mono.just (errorHandler.proceedToMoveDataFromStagingToFinal())
                .filter(proceed -> proceed)
                .flatMap(proceed -> xsbDataRepository.deleteAll()
                        .doFirst(() -> log.info("Moving data in bulk from enrichment table to the xsb_data table."))
                        .then(Mono.defer(() -> xsbDataRepository.moveXsbData())));
    }

    private Mono<Void> deleteOldStagingData() {
        return xsbDataRepository.deleteAllXsbDataTemp().doFirst(()->log.info("Cleaning up temporary data from xsb_data_temp"));
    }


    private Flux<XsbData> parseXsbFile(Long index, Path anXsbResponseFile, ErrorHandler eh, List<String> taaCountryCodes) {
        final String MN = "parseXsbFile: ";

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
                        () -> Files.lines(anXsbResponseFile).skip(1), //Skip the header line
                        Flux::fromStream,
                        Stream::close
                )
                .map(s -> XsbReportHandler.mapRawXsbResponseToXsbDataPojo(String.valueOf(anXsbResponseFile), s, taaCountryCodes))
                .publishOn(Schedulers.parallel())
                .onErrorContinue((e, s) -> eh.handleParsingError(String.valueOf(s), String.valueOf(anXsbResponseFile), e.getMessage())
                );
    }


    private Flux<XsbData> parseXsbFiles(Flux<Path> xsbFiles, ErrorHandler errorHandler, List<String> taaCountryCodes){
        final String MN = "parseXsbFiles: ";
        AtomicInteger counter = new AtomicInteger(0);
        return xsbFiles
                .doOnNext(p -> log.info(MN + "Parsing file: " + p))
                .doOnError(e -> {
                    errorHandler.handleFileError("", e.getMessage(), e);
                    log.error(MN + "error: " + e.getMessage(), e);
                })
                .doOnComplete(() -> log.info(MN + "Got all files"))
                .onErrorContinue((e, o) -> log.error("Got error for a response file, " + o + ". Will continue..."))
                .index()
                .flatMap(tuple2 -> parseXsbFile(tuple2.getT1(), tuple2.getT2(), errorHandler, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .onErrorContinue((e, o) -> log.error("Got error parsing a response file, " + o + ". Will Continue..."))
                .doOnNext(xsbData -> {
                    if (counter.incrementAndGet() % 1000 == 0)
                        log.info(MN + "{} ... {} records", xsbData.getSourceXsbDataFileName(), counter.get());
                })
                .doOnComplete(() -> log.info(MN + "Finished. Parsed and converted to JSON a total of {} records", counter.get()));
    }


    public void trigger(Trigger trigger) {
        if (executing.get()) throw new ConcurrentModificationException("Process is currently running!");
        if (trigger == null) throw new IllegalArgumentException("Invalid request body in POST");
        errorHandler.init(null);
        Set<String> uniqueFileNames = trigger.getUniqueFileNames();
        if (uniqueFileNames == null || uniqueFileNames.isEmpty())
            throw new IllegalArgumentException("Request body must include files attribute (an array with file names or file name patterns.");

        AtomicInteger dbCounter = new AtomicInteger(0);

        String tmpdir;
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Flux<Path> filesFromList = xsbSourceFactory.xsbSource(trigger).getXSBFiles(trigger.getSourceFolder(), uniqueFileNames, tmpdir);
        deleteOldStagingData()
                .thenMany(xsbDataRepository
                        .findTaaCompliantCountries()
                        .collectList()
                        .doOnError(e -> log.error("Unable to get a list of TAA compliant country codes. Exiting!", e))
                        .onErrorStop()
                        .flatMapMany(taaCountryCodes -> parseXsbFiles(filesFromList, errorHandler, taaCountryCodes))
                        .onBackpressureBuffer()
                        .flatMap(x -> saveDataRecordToStaging(x, errorHandler))
                        .doFirst(() -> dbCounter.set(0))
                        .doOnNext(e -> {if (dbCounter.incrementAndGet() % 1000 == 0) log.info("Saved {} records", dbCounter.get());})
                        .doOnComplete(() -> errorHandler.setNumRecordsSavedInTempDB(dbCounter))
                )
                .then(Mono.defer(() -> moveDataFromStagingToFinal()))
                .thenMany(Flux.defer(() -> uploadErrorFilesToS3()))
                .then(Mono.defer(() -> deleteTmpDir(Path.of(tmpdir))))
                .doFirst(() -> executing.compareAndSet(false, true))
                .doFinally(s -> {
                    errorHandler.close();
                    log.info("Finished. Saved a total of {} records", dbCounter.get());
                    log.info("Number of parsing errors: " + errorHandler.getNumParsingErrors().get());
                    log.info("Number of db errors: " + errorHandler.getNumDbErrors().get());
                    log.info("Number of file errors: " + errorHandler.getNumFileErrors().get());
                    log.info("All Done!!");
                    executing.compareAndSet(true, false);
                })
                .subscribe(null, e -> log.error("Unexpected Error", e));
    }

}
