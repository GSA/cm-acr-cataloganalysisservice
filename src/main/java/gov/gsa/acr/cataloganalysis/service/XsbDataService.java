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


    /**
     * Main entry point, that triggers the application to download and process the bi-monthly XSB files.
     *
     * @param trigger A trigger object. See the Swagger Docs for more information about this.
     */
    public void trigger(Trigger trigger) {
        if (executing.get()) throw new ConcurrentModificationException("Process is currently running!"); // Already executing!
        if (trigger == null) throw new IllegalArgumentException("Invalid request body in POST"); // Trigger is required
        errorHandler.init(null); // Initialize the error handler, reset all previous attributes.
        Set<String> uniqueFileNames = trigger.getUniqueFileNames();
        if (uniqueFileNames == null || uniqueFileNames.isEmpty())
            throw new IllegalArgumentException("Request body must include files attribute (an array with file names or file name patterns.");
        AtomicInteger dbCounter = new AtomicInteger(0);
        String tmpdir; // Create a temporary directory for staging all XSB files that need to be processed.
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Download all XSB files from the source specified in the trigger (SFTP, S3 or Local) to the temp dir.
        Flux<Path> xsbFiles = xsbSourceFactory.xsbSource(trigger).getXSBFiles(trigger.getSourceFolder(), uniqueFileNames, tmpdir);
        deleteOldStagingData()
                .thenMany(xsbDataRepository
                        .findTaaCompliantCountries()
                        .collectList()
                        .doOnError(e -> log.error("Unable to get a list of TAA compliant country codes. Exiting!", e))
                        .flatMapMany(taaCountryCodes -> parseXsbFiles(xsbFiles, errorHandler, taaCountryCodes))
                        .onBackpressureBuffer()
                        .flatMap(x -> saveDataRecordToStaging(x, errorHandler))
                        .doFirst(() -> dbCounter.set(0))
                        .doOnNext(e -> {
                            // TBD change the frequency of reporting
                            if (dbCounter.incrementAndGet() % 1000 == 0) log.info("Saved {} records", dbCounter.get());
                        })
                        .doOnComplete(() -> {
                            errorHandler.setNumRecordsSavedInTempDB(dbCounter);
                            log.info("Finished. Saved a total of {} records to the staging table.", dbCounter.get());
                        })
                )
                .then(Mono.defer(this::moveDataFromStagingToFinal))
                .thenMany(Flux.defer(this::uploadErrorFilesToS3))
                .then(Mono.defer(() -> deleteTmpDir(Path.of(tmpdir))))
                .doFirst(() -> executing.compareAndSet(false, true))
                .doFinally(s -> {
                    errorHandler.close();
                    log.info("Finished. Moved a total of {} records to the final xsb_data table", dbCounter.get());
                    log.info("Number of parsing errors: " + errorHandler.getNumParsingErrors().get());
                    log.info("Number of db errors: " + errorHandler.getNumDbErrors().get());
                    log.info("Number of file errors: " + errorHandler.getNumFileErrors().get());
                    log.info("All Done!!");
                    executing.compareAndSet(true, false);
                })
                .subscribe(null, e -> log.error("Unexpected Error", e));
    }


    /**
     * Parse all the XSB files asynchronously, producing a list of XSBData POJOs. A helper function (parseXsbFile) is
     * used to parse a single file. XsbData objects from all the individual files are collected into a single stream.
     *
     * @param xsbFiles        A stream of Xsb Files that need to be parsed and converted to a stream of XsbData objects
     * @param errorHandler    An object for handling any errors by mainly logging the error with as much information
     *                        as possible for further analysis
     * @param taaCountryCodes Country codes for all the countries that USA has a valid Trade Agreement
     * @return A stream of XsbData POJO object created from each data line of ALL the XSB files, collected into a single
     * stream from all the files
     */
    private Flux<XsbData> parseXsbFiles(Flux<Path> xsbFiles, ErrorHandler errorHandler, List<String> taaCountryCodes) {
        final String MN = "parseXsbFiles: ";
        AtomicInteger counter = new AtomicInteger(0);
        return xsbFiles
                .doOnNext(p -> log.info(MN + "Parsing file: " + p))
                .doOnComplete(() -> log.info(MN + "Parsed ALL files!!"))
                .index()
                .flatMap(tuple2 -> parseXsbFile(tuple2.getT1(), tuple2.getT2(), errorHandler, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .doOnNext(xsbData -> {
                    // TBD adjust the frequency of reporting
                    if (counter.incrementAndGet() % 1000 == 0)
                        log.info(MN + "{} ... {} records", xsbData.getSourceXsbDataFileName(), counter.get());
                })
                .doOnComplete(() -> log.info(MN + "Finished. Parsed and converted to JSON a total of {} records", counter.get()));
    }


    /**
     * Function that parses a single XSB file and converts each line inside the file to an XsbData POJO. Any individual
     * lines that have problems are separated into an error file for further analysis.
     *
     * @param index             Index of the Xsb File that is going to be processed by this method
     * @param anXsbResponseFile The Xsb File to be processed
     * @param errorHandler      An object for handling any errors by mainly logging the error with as much information
     *                          as possible for further analysis
     * @param taaCountryCodes   Country codes for all the countries that USA has a valid Trade Agreement
     * @return A stream of XsbData POJO object created from each data line of the XSB file.
     */
    private Flux<XsbData> parseXsbFile(Long index, Path anXsbResponseFile, ErrorHandler errorHandler, List<String> taaCountryCodes) {
        final String MN = "parseXsbFile: ";

        if (index == 0) {
            XsbReportHandler.resetHeader();
            this.errorHandler.init(null);
        }

        // First read the header row (First row of the File)
        try (Stream<String> rawProductsFromXSB = Files.lines(anXsbResponseFile)) {
            Optional<String> mayBeHeader = rawProductsFromXSB.findFirst();
            if (mayBeHeader.isPresent()) {
                String header = mayBeHeader.get();
                XsbReportHandler.setHeader(index, String.valueOf(anXsbResponseFile), header);
                if (this.errorHandler.getHeader() == null) this.errorHandler.setHeader(header);
            } else
                throw new Exception("Missing header row from file, " + anXsbResponseFile + ". Possibly an empty file.");
        } catch (Exception e) {
            errorHandler.handleFileError(String.valueOf(anXsbResponseFile), "Ignoring File. " + e.getMessage(), e);
            log.error(MN + "Ignoring this file because of the following error: " + e.getMessage(), e);
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
                .onErrorContinue((e, s) -> errorHandler.handleParsingError(String.valueOf(s), String.valueOf(anXsbResponseFile), e.getMessage())
                );
    }


    /**
     * Each XsbData POJO, on the stream, is first saved in to a staging table. This is a fast operation that does not
     * work in a database transaction.
     *
     * @param xsbData      XsbData POJO that needs to be saved to the DB
     * @param errorHandler An object that can handle any errors, mainly by logging them for further analysis.
     * @return The ID of the saved record
     */
    private Mono<Integer> saveDataRecordToStaging(XsbData xsbData, ErrorHandler errorHandler) {
        return xsbDataRepository.saveXSBDataToTemp(xsbData.getContractNumber(), xsbData.getManufacturer(), xsbData.getPartNumber(), xsbData.getXsbData())
                // TBD: Retry logic here
                //.retry(5)
                .onErrorResume(e -> {
                    log.error("Error saving record to DB. " + e.getMessage() + " " + xsbData, e);
                    errorHandler.handleDBError(xsbData, e.getMessage());
                    return Mono.empty();
                });
    }


    /**
     * Once all the data from all the XSB files have been saved into the staging table, bulk move it into the final
     * table. This is an atomic operation, performed in a DB transaction and succeeds or fails as an atomic operation.
     *
     * @return Asynchronously return void once completed
     */
    @Transactional
    public Mono<Void> moveDataFromStagingToFinal() {
        return Mono.just(errorHandler.proceedToMoveDataFromStagingToFinal())
                .filter(proceed -> proceed)
                .flatMap(proceed -> xsbDataRepository.deleteAll()
                        .doFirst(() -> log.info("Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table."))
                        .then(Mono.defer(xsbDataRepository::moveXsbData)));
    }


    /**
     * Deletes old data from previous runs in staging table
     * @return Asynchronously returns a void once all data is deleted.
     */
    private Mono<Void> deleteOldStagingData() {
        return xsbDataRepository.deleteAllXsbDataTemp().doFirst(() -> log.info("Cleaning up temporary data from xsb_data_temp"));
    }


    /**
     * After the data is saved in the final tables, if there are any error files, upload them to the S3 bucket for
     * further analysis.
     *
     * @return A stream of fully qualified names of the uploaded files
     */
    private Flux<String> uploadErrorFilesToS3() {
        return errorHandler.getErrorFiles()
                .doFirst(errorHandler::close)
                .flatMap(p -> acrXsbS3Util.uploadToS3(p, "errors/" + p.getFileName()));
    }


    /**
     * After everything is done, delete the temporary directory used to download XSB files for processing.
     *
     * @param tmpDir the temporary directory where XSB files are downloaded for processing
     * @return A Mono of True or False depending on if the files in the temp directory and the directory were deleted successfully
     */
    private Mono<Boolean> deleteTmpDir(Path tmpDir) {
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
                    } catch (Exception e) {
                        log.error("Unable to delete the folder " + tmpDir);
                        return Mono.error(e);
                    }
                }))
                .doOnSuccess(b -> log.info("Successfully Deleted folder " + tmpDir));
    }

}
