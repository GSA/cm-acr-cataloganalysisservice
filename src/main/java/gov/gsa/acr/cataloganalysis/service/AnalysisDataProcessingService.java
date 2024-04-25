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
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
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
        // Trigger validation throws an IllegalArgumentException if invalid.
        Trigger.validate(trigger);
        // Variables needed for reporting Progress every so often
        Instant start = Instant.now();
        final AtomicReference<Instant> lastProgressReportTime = new AtomicReference<>(start);
        // Provide a progress report every so many seconds
        final Duration progressMonitorInterval = Duration.ofSeconds(progressReportingIntervalSeconds);
        // Counter to count the number of records saved in the database
        AtomicInteger dbCounter = new AtomicInteger(0);

        // A temporary directory for downloading and staging all XSB files that need to be processed.
        Path tmpDir;
        try {
            tmpDir = Files.createTempDirectory("xsbReports");
            log.info("Created temporary Directory: " + tmpDir);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected error, cannot create a temporary directory. Cannot proceed without a temporary directory.", e);
        }

        // Almost ready, now acquire the lock to run the pipeline, so only one thread can upload data at a time
        if (!executing.compareAndSet(false, true)) {
            // Could not get a lock, some other thread is running the pipeline.
            deleteDir(tmpDir);
            throw new ConcurrentModificationException("Process is currently running! Cannot run more than one data uploads at the same time.");
        }

        Mono<Void> pipeline;
        if (trigger.getOnlyMoveStagedData()){
            pipeline = numberOfStagedRecords()
                    .flatMap(count -> {
                        dbCounter.set(count);
                        return moveDataFromStagingToFinal(trigger, count);
                    });
        }
        else {
            pipeline = deleteOldStagingData()
                    .then(findTaaCompliantCountries())
                    .flatMapMany(taaCountryCodes -> {
                        // Download all XSB files from the source specified in the trigger (SFTP, S3 or Local) to the temp dir.
                        Flux<Path> xsbFiles = analysisSourceFactory
                                .analysisSource(trigger)
                                .getAnalyzedCatalogs(trigger.getSourceFolder(), trigger.getUniqueFileNames(), tmpDir.toFile().getAbsolutePath())
                                .limitRate(4, 2);
                        return parseXsbFiles(xsbFiles, taaCountryCodes, true);})
                    .onBackpressureBuffer()
                    .flatMap(this::saveDataRecordToStaging)
                    .doOnNext(e -> {
                        Instant currentTime = Instant.now();
                        int numRecordsSavedSoFar = dbCounter.incrementAndGet();
                        if (Duration.between(lastProgressReportTime.get(), currentTime).compareTo(progressMonitorInterval) >= 0) {
                            log.info("Staged {} records", numRecordsSavedSoFar);
                            lastProgressReportTime.set(currentTime);
                        }
                    })
                    .doOnComplete(() -> log.info("Finished saving a total of {} records to the staging table.", dbCounter.get()))
                    .then(Mono.defer(() -> moveDataFromStagingToFinal(trigger, dbCounter.get())));
        }

        // Start the pipeline for parsing files and storing data in the database
        return pipeline
                .then(Flux.defer(this::uploadErrorFilesToS3).collectList())
                .flatMap(errorFileNames -> getDataUploadResults(errorFileNames, errorHandler, dbCounter.get()))
                .doFirst(() -> {
                    errorHandler.init(xsbDataParser.getHeaderString());
                    dbCounter.set(0);
                })
                .doFinally(s -> {
                    executing.compareAndSet(true, false);
                    errorHandler.close();
                    generateFinalReport(dbCounter.get(), s, trigger);
                    deleteDir(tmpDir);
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
     * get number of records in the staging table
     *
     * @return Asynchronously returns count of xsb_data_temp table.
     */
    Mono<Integer> numberOfStagedRecords() {
        try {
            return xsbDataRepository.xsbDataTempCount().doFirst(() -> log.info("Getting count of xsb_data_temp table"));
        } catch (Exception e) {
            log.error("Error: getting count of staging table.", e);
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
     * @param xsbFiles           A stream of Xsb Files that need to be parsed and converted to a stream of XsbData objects
     * @param taaCountryCodes    Country codes for all the countries that USA has a valid Trade Agreement
     * @param deleteAfterParsing Cleanup the files after parsing and make minimal use of the resources.
     * @return A stream of XsbData POJO object created from each data line of ALL the XSB files, collected into a single
     * stream from all the files
     */
    Flux<XsbData> parseXsbFiles(Flux<Path> xsbFiles, List<String> taaCountryCodes, boolean deleteAfterParsing) {
        // Variables needed for reporting Progress every so often
        Instant start = Instant.now();
        final AtomicReference<Instant> lastProgressReportTime = new AtomicReference<>(start);
        // Provide a progress report every so many minutes
        final Duration progressMonitorInterval = Duration.ofSeconds(progressReportingIntervalSeconds);
        AtomicInteger counter = new AtomicInteger(0);
        return xsbFiles.doOnNext(path -> log.info("Parsing file: " + path))
                .flatMap(path -> parseXsbFile(path, taaCountryCodes, deleteAfterParsing))
                .doFirst(() -> counter.set(0))
                .doOnNext(xsbData -> {
                    Instant currentTime = Instant.now();
                    int numRecordsParsed = counter.incrementAndGet();
                    if (Duration.between(lastProgressReportTime.get(), currentTime).compareTo(progressMonitorInterval) >= 0) {
                        log.info("{}: parsed {} records", xsbData.getSourceXsbDataFileName(), numRecordsParsed);
                        lastProgressReportTime.set(currentTime);
                    }
                })
                .doOnComplete(() -> log.info("Finished parsing all files. Parsed and converted to JSON a total of {} records", counter.get()));
    }


    /**
     * Function that parses a single XSB file and converts each line inside the file to an XsbData POJO. Any individual
     * lines that have problems are separated into an error file for further analysis.
     *
     * @param xsbFile            The Xsb File to be processed
     * @param taaCountryCodes    Country codes for all the countries that USA has a valid Trade Agreement
     * @param deleteAfterParsing Cleanup the files after parsing and make minimal use of the resources. Important since
     *                           Kubernetes cachaes file in the Page Cache of the pod and that just bloats the memory.
     * @return A stream of XsbData POJO object created from each data line of the XSB file.
     */
    Flux<XsbData> parseXsbFile(Path xsbFile, List<String> taaCountryCodes, boolean deleteAfterParsing) {
        // Check if we have too many errors already. If yes, no point moving forward, bail off now.
        if (!errorHandler.totalErrorsWithinAcceptableThreshold()) {
            log.warn("Too many errors: Exceeded the number of error threshold. Bailing out, not parsing {} file", xsbFile);
            return Flux.empty();
        }

        // First read the header row (First row of the File) and makes sure its valid
        try (Stream<String> rawProductsFromXSB = Files.lines(xsbFile)) {
            String header = rawProductsFromXSB.findFirst().get();
            if (!xsbDataParser.validateHeader(header))
                throw new NoSuchElementException("Header String for file " + xsbFile + ", " + header + ", is different from expected header, " + xsbDataParser.getHeaderString());
        } catch (Exception e) {
            // If the header is invalid, we cannot do much with the file. The quality of data is not reliable.
            errorHandler.handleFileError(String.valueOf(xsbFile), "Ignoring File. " + e.getMessage(), e);
            log.error("Ignoring file : " + xsbFile, e);
            if (deleteAfterParsing) deleteFile(xsbFile);
            return Flux.empty();
        }

        // If the file seems valid, create a Flux of all the lines from the file. These will be the raw file lines.
        return Flux.using(
                        () -> Files.lines(xsbFile).skip(1), //Skip the header line
                        Flux::fromStream,
                        Stream::close
                )
                .mapNotNull(xsbData -> xsbDataParser.parseXsbData(xsbData, xsbFile.toString(), taaCountryCodes))
                .publishOn(Schedulers.parallel())
                .onErrorContinue((e, s) -> errorHandler.handleParsingError(String.valueOf(s), String.valueOf(xsbFile), e.getMessage()))
                .doFinally(s -> {if (deleteAfterParsing) deleteFile(xsbFile);});
    }


    /**
     * Each XsbData POJO, is first saved in to a staging table. This is a fast operation that does not
     * work in a database transaction.
     *
     * @param xsbData XsbData POJO that needs to be saved to the DB
     * @return The ID of the saved record
     */
    Mono<Integer> saveDataRecordToStaging(XsbData xsbData) {
        // Check if we have too many errors already. If yes, no point moving forward, bail off now.
        if (xsbData == null || !(errorHandler.totalErrorsWithinAcceptableThreshold())) return Mono.empty();
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
     Mono<Void> moveDataFromStagingToFinal(Trigger trigger, int recordCount) {
        String msg = "Moving "+recordCount+" product(s) in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        Mono<Void> rtrn;

         try {
             if (trigger.getOnlyStageData()) return Mono.empty();
             if (recordCount <= 0) {
                 log.info("There are no records to move from staging (xsb_data_temp) to the Final (xsb_data) table!");
                 return Mono.empty();
             }

             Integer forcedError = trigger.getForcedError();
             if (errorHandler.totalErrorsWithinAcceptableThreshold()){
                 log.info(msg);
                 if (trigger.getPurgeOldData()) {
                     // TBD Delete the test rollback code
                     if (forcedError == 0) rtrn = transactionalDataService.replace();
                     else rtrn = transactionalDataService.testRollbackReplace();
                 }
                 else {
                     // TBD Delete the test rollback code
                     if (forcedError == 0) rtrn = transactionalDataService.update();
                     else rtrn = transactionalDataService.testRollbackUpdate();
                 }

                 return rtrn
                         .doOnSuccess(s -> log.info("Finished moving a total of {} records to the final xsb_data table", recordCount))
                         .onErrorResume(e -> {
                             log.error(errMsg, e);
                             errorHandler.setDataUploadFailed(Boolean.TRUE);
                             errorHandler.handleFileError("", errMsg, e);
                             return Mono.empty();
                         });
             }
             else{
                 String reason = "Too many parsing/db errors. Stopping the process. The process will stop and not save the data to the final table!";
                 log.error(reason);
                 errorHandler.setDataUploadFailed(Boolean.TRUE);
                 errorHandler.handleFileError("", errMsg, new RuntimeException(reason));
                 return Mono.empty();
             }
         } catch (Exception e) {
             log.error("Unexpected exception. ", e);
             errorHandler.setDataUploadFailed(Boolean.TRUE);
             errorHandler.handleFileError("", errMsg, e);
             return Mono.empty();
         }
     }

    boolean deleteFile(Path p){
        try {
            boolean deleted = Files.deleteIfExists(p);
            if (deleted) log.info("Deleted: " + p);
            return deleted;
        } catch (Exception e) {
            log.error("Unable to delete: " + p, e);
            return false;
        }
    }

    /**
     * After everything is done, delete the temporary directory used to download XSB files for processing.
     *
     * @param dir the temporary directory where XSB files are downloaded for processing
     * @return True or False depending on if the files in the temp directory and the directory were deleted successfully
     */
    boolean deleteDir(Path dir){
        if (Files.isDirectory(dir)) {
            try (Stream<Path> stream = Files.list(dir)) {
                stream.forEach(this::deleteFile);
            } catch (Exception e) {
                log.error("Unexpected error. Unable to delete temporary directory " + dir, e);
            }
            return deleteFile(dir);
        }
        else return false;
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
     *
     * @param errorFileNames Names of all the error files generated during the run.
     * @param errorHandler   The error handler has all the valuable information regarding what worked and what failed.
     * @param recordCount
     * @return A data object holding the metrics of the data upload process execution.
     */
    Mono<DataUploadResults> getDataUploadResults(List<String> errorFileNames, ErrorHandler errorHandler, int recordCount) {
        if (errorHandler == null) return Mono.error(new IllegalArgumentException("Error Handler cannot be null"));

        errorHandler.setErrorFileNames(errorFileNames);
        DataUploadResults results = new DataUploadResults();
        results.setErrorFileNames(errorFileNames);
        results.setNumRecordsSavedInTempDB(recordCount);
        results.setNumParsingErrors(errorHandler.getNumParsingErrors().get());
        results.setNumDbErrors(errorHandler.getNumDbErrors().get());
        results.setNumFileErrors(errorHandler.getNumFileErrors().get());
        return Mono.just(results);
    }


    private void log(String msg, Level logLevel, List<String> report){
        report.add(logLevel + ":" + msg);
        switch (logLevel) {
            case INFO  -> log.info(msg);
            case WARN  -> log.warn(msg);
            case ERROR -> log.error(msg);
        }

    }


    List<String> generateFinalReport(int recordCount, SignalType signalType, Trigger trigger) {
        List<String> report = new ArrayList<>();
        List<String> errorFileNames = errorHandler.getErrorFileNames();
        log("===================== Final Report =====================", Level.INFO, report);
        if (trigger != null && trigger.getOnlyStageData()) {
            log("Saved "+ recordCount + " records in the ACR DB staging table (xsb_data_temp).", Level.INFO, report);
        }
        else if (errorHandler.getDataUploadFailed()) {
            log("Error moving data from staging to final DB table. Please see logs for error reason.", Level.ERROR, report);
        }
        else log("Saved "+ recordCount + " records in the ACR DB.", Level.INFO, report);
        if (errorHandler.getNumParsingErrors() != null && errorHandler.getNumParsingErrors().get() > 0) {
            log("Number of parsing errors: " + errorHandler.getNumParsingErrors().get(), Level.WARN, report);
            log("Please see the below file(s) saved in S3 to get a list of all records that had parsing issues.", Level.WARN, report);
            errorFileNames.stream().filter(name -> name.contains("xsb_error_parse_")).forEach(name -> log("\t"+name, Level.WARN, report));
        }
        if (errorHandler.getNumDbErrors() != null && errorHandler.getNumDbErrors().get() > 0) {
            log("Number of db errors: " + errorHandler.getNumDbErrors().get(), Level.WARN, report);
            log("Please see the below file(s) saved in S3 to get a list of all records that had DB issues.", Level.WARN, report);
            errorFileNames.stream().filter(name -> name.contains("xsb_error_db_")).forEach(name -> log("\t"+name, Level.WARN, report));
        }
        if (errorHandler.getDataUploadFailed()
            || (errorHandler.getNumFileErrors() != null && errorHandler.getNumFileErrors().get() > 0)
            || (errorHandler.getNumParsingErrors() != null && errorHandler.getNumParsingErrors().get() > 0 )
            || (errorHandler.getNumDbErrors() != null && errorHandler.getNumDbErrors().get() > 0)) {
            if (errorFileNames != null) {
                log("Please see the below file(s) saved in S3 for reasons for any of the errors.", Level.WARN, report);
                errorFileNames.stream().filter(name -> name.contains("xsb_error_msg_")).forEach(name -> log("\t" + name, Level.WARN, report));
            }
        }
        else if (signalType == SignalType.ON_ERROR) log("There were some errors while uploading the data. Please see the error logs.", Level.ERROR, report);
        else if (signalType == SignalType.CANCEL) log("The process was canceled. Please see the logs.", Level.WARN, report);
        else log("Finished without any issues!!", Level.INFO, report);
        log("========================================================", Level.INFO, report);
        return report;
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
                .doFinally(s -> {
                    errorHandler.close();
                    deleteDir(Path.of(tmpdir));
                });
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
                .flatMapMany(taaCountryCodes -> parseXsbFiles(xsbFiles, taaCountryCodes, false))
                .doFinally(s -> deleteDir(Path.of(tmpdir)));
    }

}
