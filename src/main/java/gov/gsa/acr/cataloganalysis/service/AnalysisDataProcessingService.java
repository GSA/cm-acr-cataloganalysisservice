package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceFactory;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceS3;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.DataUploadResults;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class AnalysisDataProcessingService {
    @Value("${progress.reporting.interval.seconds:30}")
    @Getter
    private int progressReportingIntervalSeconds;

    private final AtomicBoolean executing = new AtomicBoolean();
    private final XsbDataRepository xsbDataRepository;
    private final ErrorHandler errorHandler;
    private final AnalysisSourceFactory analysisSourceFactory;
    private final AnalysisSourceS3 xsbSourceS3Files;
    private final XsbDataParser xsbDataParser;
    private final TransactionalDataService transactionalDataService;

    /**
     * Main entry point, that triggers the application to download and process the bi-monthly XSB files.
     *
     * @param trigger A trigger object. See the Swagger Docs for more information about this.
     * @return Returns the results of uploading the data from XSB to DB
     */
    public Mono<DataUploadResults> triggerDataUpload(Trigger trigger) {
        // Already executing? Exit if it is already executing.
        if (executing.get()) throw new ConcurrentModificationException("Process is currently running!");

        // Variables needed for reporting Progress every so often
        Instant start = Instant.now();
        final AtomicReference<Instant> lastProgressReportTime = new AtomicReference<>(start);
        // Provide a progress report every so many minutes
        final Duration progressMonitorInterval = Duration.ofSeconds(progressReportingIntervalSeconds);
        // Counter to count the number of records saved in the database
        AtomicInteger dbCounter = new AtomicInteger(0);

        // Trigger validation throws an IllegalArgumentException if invalid.
        Trigger.validate(trigger);
        Set<String> uniqueFileNames = trigger.getUniqueFileNames();

        // A temporary directory for downloading and staging all XSB files that need to be processed.
        String tmpdir;
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error, cannot create a temporary directory. Cannot proceed without a temporary directory.", e);
        }

        // Initialize the error handler, reset all previous attributes.
        errorHandler.init(xsbDataParser.getHeaderString());

        // Download all XSB files from the source specified in the trigger (SFTP, S3 or Local) to the temp dir.
        Flux<Path> xsbFiles = analysisSourceFactory.analysisSource(trigger).getAnalyzedCatalogs(trigger.getSourceFolder(), uniqueFileNames, tmpdir);

        // Start the pipeline for parsing files and storing data in the database
        return deleteOldStagingData()
                .then(findTaaCompliantCountries())
                .flatMapMany(taaCountryCodes -> parseXsbFiles(xsbFiles, taaCountryCodes))
                .onBackpressureBuffer()
                .flatMap(this::saveDataRecordToStaging)
                .doOnNext(e -> {
                    Instant currentTime = Instant.now();
                    int numRecordsSavedSoFar = dbCounter.incrementAndGet();
                    if (Duration.between(lastProgressReportTime.get(), currentTime).compareTo(progressMonitorInterval) >= 0) {
                        log.info("Saved {} records", numRecordsSavedSoFar);
                        lastProgressReportTime.set(currentTime);
                    }
                })
                .doOnComplete(() -> {
                    errorHandler.setNumRecordsSavedInTempDB(dbCounter);
                    log.info("Finished. Saved a total of {} records to the staging table.", dbCounter.get());
                })
                .then(Mono.defer(() -> moveDataFromStagingToFinal(trigger)))
                // TBD Do proper error handling
                .then(Mono.defer(() -> deleteTmpDir(Path.of(tmpdir))))
                .then(Flux.defer(this::uploadErrorFilesToS3).collectList())
                .flatMap(errorFileNames -> getDataUploadResults(errorFileNames, errorHandler))
                .doFirst(() -> {
                    executing.compareAndSet(false, true);
                    dbCounter.set(0);
                })
                .doFinally(s -> {
                    errorHandler.close();
                    log.info("Finished. Moved a total of {} records to the final xsb_data table", dbCounter.get());
                    log.info("Number of parsing errors: " + errorHandler.getNumParsingErrors().get());
                    log.info("Number of db errors: " + errorHandler.getNumDbErrors().get());
                    log.info("Number of file errors: " + errorHandler.getNumFileErrors().get());
                    log.info("All Done!!");
                    executing.compareAndSet(true, false);
                });
    }


    /**
     * Deletes old data from previous runs in staging table
     *
     * @return Asynchronously returns a void once all data is deleted.
     */
    Mono<Void> deleteOldStagingData() {
        String msg = "Cleaning up temporary data from xsb_data_temp";
        String errMsg = "Error: " + msg;
        try {
            return xsbDataRepository.deleteAllXsbDataTemp().doFirst(() -> log.info(msg));
        } catch (Exception e) {
            log.error(errMsg, e);
            return Mono.error(e);
        }
    }


    /**
     * Gets a list of countries that have a trade agreement with the USA.
     *
     * @return Asynchronously returns a list of countries that have a trade agreement with the USA
     */
    Mono<List<String>> findTaaCompliantCountries() {
        String errorMsg = "Unable to get a list of TAA compliant country codes. Exiting!";
        try {
            return xsbDataRepository.findTaaCompliantCountries()
                    .collectList()
                    .<List<String>>handle((list, sink) -> {
                        if (list.isEmpty()) {
                            sink.error(new NoSuchElementException("Did not find a single country with trade agreement. Most likely an error!"));
                            return;
                        }
                        sink.next(list);
                    })
                    .doOnError(e -> log.error(errorMsg, e));
        } catch (Exception e) {
            log.error(errorMsg, e);
            return Mono.error(e);
        }
    }


    /**
     * Parse all the XSB files asynchronously, producing a list of XSBData POJOs. A helper function (parseXsbFile) is
     * used to parse a single file. XsbData objects from all the individual files are collected into a single stream.
     *
     * @param xsbFiles        A stream of Xsb Files that need to be parsed and converted to a stream of XsbData objects
     * @param taaCountryCodes Country codes for all the countries that USA has a valid Trade Agreement
     * @return A stream of XsbData POJO object created from each data line of ALL the XSB files, collected into a single
     * stream from all the files
     */
    Flux<XsbData> parseXsbFiles(Flux<Path> xsbFiles, List<String> taaCountryCodes) {
        // Variables needed for reporting Progress every so often
        Instant start = Instant.now();
        final AtomicReference<Instant> lastProgressReportTime = new AtomicReference<>(start);
        // Provide a progress report every so many minutes
        final Duration progressMonitorInterval = Duration.ofSeconds(progressReportingIntervalSeconds);
        AtomicInteger counter = new AtomicInteger(0);
        return xsbFiles.doOnNext(path -> log.info("Parsing file: " + path))
                .flatMap(path -> parseXsbFile(path, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .doOnNext(xsbData -> {
                    Instant currentTime = Instant.now();
                    int numRecordsParsed = counter.incrementAndGet();
                    if (Duration.between(lastProgressReportTime.get(), currentTime).compareTo(progressMonitorInterval) >= 0) {
                        log.info("{}: parsed {} records", xsbData.getSourceXsbDataFileName(), numRecordsParsed);
                        lastProgressReportTime.set(currentTime);
                    }
                })
                .doOnComplete(() -> log.info("Finished Parsing all files. Parsed and converted to JSON a total of {} records", counter.get()));
    }


    /**
     * Function that parses a single XSB file and converts each line inside the file to an XsbData POJO. Any individual
     * lines that have problems are separated into an error file for further analysis.
     *
     * @param xsbFile         The Xsb File to be processed
     * @param taaCountryCodes Country codes for all the countries that USA has a valid Trade Agreement
     * @return A stream of XsbData POJO object created from each data line of the XSB file.
     */
    Flux<XsbData> parseXsbFile(Path xsbFile, List<String> taaCountryCodes) {
        // First read the header row (First row of the File) and makes sure its valid
        try (Stream<String> rawProductsFromXSB = Files.lines(xsbFile)) {
            String header = rawProductsFromXSB.findFirst().get();
            if (!xsbDataParser.validateHeader(header))
                throw new NoSuchElementException("Header String for file " + xsbFile + ", " + header + ", is different from expected header, " + xsbDataParser.getHeaderString());
        } catch (Exception e) {
            // If the header is invalid, we cannot do much with the file. The quality of data is not reliable.
            errorHandler.handleFileError(String.valueOf(xsbFile), "Ignoring File. " + e.getMessage(), e);
            log.error("Ignoring file : " + xsbFile, e);
            return Flux.empty();
        }

        // If the file seems valid, create a Flux of all the lines from the file. These will be the raw file lines.
        return Flux.using(
                        () -> Files.lines(xsbFile).skip(1), //Skip the header line
                        Flux::fromStream,
                        Stream::close
                )
                .map(xsbData -> xsbDataParser.parseXsbData(xsbFile.toString(), xsbData, taaCountryCodes))
                .publishOn(Schedulers.parallel())
                .onErrorContinue((e, s) -> errorHandler.handleParsingError(String.valueOf(s), String.valueOf(xsbFile), e.getMessage())
                );
    }


    /**
     * Each XsbData POJO, is first saved in to a staging table. This is a fast operation that does not
     * work in a database transaction.
     *
     * @param xsbData XsbData POJO that needs to be saved to the DB
     * @return The ID of the saved record
     */
    Mono<Integer> saveDataRecordToStaging(XsbData xsbData) {
        if (xsbData == null) return Mono.empty();
        try {
            return xsbDataRepository.saveXSBDataToTemp(xsbData.getContractNumber(), xsbData.getManufacturer(), xsbData.getPartNumber(), xsbData.getXsbData())
                    // TBD: Retry logic here
                    //.retry(5)
                    .onErrorResume(e -> {
                        log.error("Error saving record to DB. " + xsbData, e);
                        errorHandler.handleDBError(xsbData, e.getMessage());
                        return Mono.empty();
                    });
        } catch (Exception e) {
            log.error("Error saving record to DB. " + xsbData, e);
            errorHandler.handleDBError(xsbData, e.getMessage());
            return Mono.empty();
        }
    }


    /**
     * Once all the data from all the XSB files have been saved into the staging table, bulk move it into the final
     * table. This is an atomic operation, performed in a DB transaction and succeeds or fails as an atomic operation.
     *
     * @return Asynchronously return void once completed, or rolls back if an exception is thrown.
     */
     Mono<Void> moveDataFromStagingToFinal(Trigger trigger) {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Mono<Void> rtrn;

         try {
             Integer forcedError = trigger.getForcedError();
             if (errorHandler.totalErrorsWithinAcceptableThreshold()){
                 log.info(msg);
                 if (trigger.getPurgeOldData()) {
                     // TBD Delete thr test rollback code
                     if (forcedError == 0) rtrn = transactionalDataService.replace();
                     else rtrn = transactionalDataService.testRollbackReplace();
                 }
                 else {
                     // TBD Delete thr test rollback code
                     if (forcedError == 0) rtrn = transactionalDataService.update();
                     else rtrn = transactionalDataService.testRollbackUpdate();
                 }

                 return rtrn
                         .doOnSuccess(s -> {
                             log.info("Successfully moved data from staging to final tables!");
                             //TBD add counts
                         })
                         .onErrorResume(e -> {
                             log.error("Mono " + errMsg, e);
                             errorHandler.handleFileError("", errMsg, e);
                             return Mono.empty();
                         });
             }
             else{
                 String reason = "Too many parsing/db errors. Stopping the process. The process will stop and not save the data to the final table!";
                 log.error(reason);
                 // TBD errorhandler add a method for this information
                 errorHandler.handleFileError("", errMsg, new RuntimeException(reason));
                 return Mono.empty();
             }
         } catch (Exception e) {
             log.error("Unexpected exception. ", e);
             errorHandler.handleFileError("", errMsg, e);
             return Mono.empty();
         }
     }



    /**
     * After everything is done, delete the temporary directory used to download XSB files for processing.
     *
     * @param tmpDir the temporary directory where XSB files are downloaded for processing
     * @return A Mono of True or False depending on if the files in the temp directory and the directory were deleted successfully
     */
    Mono<Boolean> deleteTmpDir(Path tmpDir) {
        return Flux.using(
                        () -> Files.list(tmpDir),
                        Flux::fromStream,
                        Stream::close
                )
                .doFirst(() -> log.info("Deleting temporary folder " + tmpDir))
                .handle((source, sink) -> {
                    try {
                        log.info("Deleting temp file: " + source);
                        sink.next(Files.deleteIfExists(source));
                    } catch (Exception e) {
                        log.error("Unable to delete temp file: " + source, e);
                    }
                })
                .then(Mono.defer(() -> {
                    try {
                        return Mono.just(Files.deleteIfExists(tmpDir));
                    } catch (Exception e) {
                        log.error("Unable to delete the folder " + tmpDir, e);
                        return Mono.just(false);
                    }
                }))
                .onErrorResume(e -> {
                    log.error("Error deleting tmp dir", e);
                    return Mono.just(false);
                })
                .doOnSuccess(b -> log.info("Deleted temporary folder successfully " + tmpDir));
    }


    /**
     * After the data is saved in the final tables, if there are any error files, upload them to the S3 bucket for
     * further analysis.
     *
     * @return A stream of fully qualified names of the uploaded files
     */
    Flux<String> uploadErrorFilesToS3() {
        return errorHandler.getErrorFiles()
                .doFirst(errorHandler::close)
                .flatMap(p -> xsbSourceS3Files.uploadToS3(p, "errors/" + p.getFileName()));
    }


    /**
     * Collect all the results and generate a data object with the results.
     * @param errorFileNames Names of all the error files generated during the run.
     * @param errorHandler The error handler has all the valuable information regarding what worked and what failed.
     * @return A data object holding the metrics of the data upload process execution.
     */
    Mono<DataUploadResults> getDataUploadResults(List<String> errorFileNames, ErrorHandler errorHandler) {
        if (errorHandler == null) return Mono.error(new IllegalArgumentException("Error Handler cannot be null"));

        DataUploadResults results = new DataUploadResults();
        results.setErrorFileNames(errorFileNames);
        results.setNumRecordsSavedInTempDB(errorHandler.getNumRecordsSavedInTempDB().get());
        results.setNumParsingErrors(errorHandler.getNumParsingErrors().get());
        results.setNumDbErrors(errorHandler.getNumDbErrors().get());
        results.setNumFileErrors(errorHandler.getNumFileErrors().get());
        return Mono.just(results);
    }


    // TBD: Only for the demo. Delete later
    public Flux<Path> downloadReports(Trigger trigger) {
        errorHandler.init(null);
        String tmpdir;
        Trigger.validate(trigger);
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected Error creating tmp directory", e);
        }

        Set<String> uniqueFileNames = trigger.getUniqueFileNames();
        return analysisSourceFactory.analysisSource(trigger).getAnalyzedCatalogs(trigger.getSourceFolder(), uniqueFileNames, tmpdir)
                .doOnComplete(() -> log.info("Finished downloading all files."))
                .doFinally(s -> errorHandler.close());
    }


    // TBD: Only for the demo. Delete later
    public Flux<XsbData> parseXsbFiles(Trigger trigger) {
        errorHandler.init(null);
        String tmpdir;
        Trigger.validate(trigger);
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected Error creating tmp directory", e);
        }

        Set<String> uniqueFileNames = trigger.getUniqueFileNames();

        Flux<Path> xsbFiles = analysisSourceFactory.analysisSource(trigger).getAnalyzedCatalogs(trigger.getSourceFolder(), uniqueFileNames, tmpdir);

        // Start the pipeline for parsing files and storing data in the database
        return findTaaCompliantCountries()
                .flatMapMany(taaCountryCodes -> parseXsbFiles(xsbFiles, taaCountryCodes));
    }

}
