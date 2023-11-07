package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.DataUploadResults;
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
import java.util.NoSuchElementException;
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
    private final XsbDataParser xsbDataParser;


    /**
     * Main entry point, that triggers the application to download and process the bi-monthly XSB files.
     *
     * @param trigger A trigger object. See the Swagger Docs for more information about this.
     * @return Returns the results of uploading the data from XSB to DB
     */
    public Mono<DataUploadResults> triggerDataUpload(Trigger trigger) {
        // Already executing? Exit if it is already executing.
        if (executing.get()) throw new ConcurrentModificationException("Process is currently running!");
        // Trigger is required
        if (trigger == null) throw new IllegalArgumentException("Illegal argument, trigger, cannot be null!");
        // Must have a valid source type
        if(trigger.getSourceType() == null) throw new IllegalArgumentException("Trigger argument must include a sourceType attribute (value of sourceType should be one of LOCAL, S3 or SFTP).");
        // Need files to download.
        Set<String> uniqueFileNames = trigger.getUniqueFileNames();
        if (uniqueFileNames == null || uniqueFileNames.isEmpty())
            throw new IllegalArgumentException("Trigger argument must include files attribute (an array with file names or file name patterns).");
        // Counter to count the number of records saved in the database
        AtomicInteger dbCounter = new AtomicInteger(0);
        // A temporary directory for downloading and staging all XSB files that need to be processed.
        String tmpdir;
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Download all XSB files from the source specified in the trigger (SFTP, S3 or Local) to the temp dir.
        Flux<Path> xsbFiles = xsbSourceFactory.xsbSource(trigger).getXSBFiles(trigger.getSourceFolder(), uniqueFileNames, tmpdir);
        // Start the pipeline for parsing files and storing data in the database
        return deleteOldStagingData()
                .then(findTaaCompliantCountries())
                .flatMapMany(taaCountryCodes -> parseXsbFiles(xsbFiles, taaCountryCodes))
                .onBackpressureBuffer()
                .flatMap(this::saveDataRecordToStaging)
                .doOnNext(e -> {
                    // TBD change the frequency of reporting
                    if (dbCounter.incrementAndGet() % 1000 == 0) log.info("Saved {} records", dbCounter.get());
                })
                .doOnComplete(() -> {
                    errorHandler.setNumRecordsSavedInTempDB(dbCounter);
                    log.info("Finished. Saved a total of {} records to the staging table.", dbCounter.get());
                })
                .then(Mono.defer(this::moveDataFromStagingToFinal))
                .then(Mono.defer(() -> deleteTmpDir(Path.of(tmpdir))))
                .then(Flux.defer(this::uploadErrorFilesToS3).collectList())
                .flatMap(errorFileNames -> getDataUploadResults(errorFileNames, errorHandler))
                .doFirst(() -> {
                    executing.compareAndSet(false, true);
                    errorHandler.init(xsbDataParser.getHeaderString()); // Initialize the error handler, reset all previous attributes.
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
        AtomicInteger counter = new AtomicInteger(0);
        return xsbFiles.doOnNext(path -> log.info("Parsing file: " + path))
                .doOnComplete(() -> log.info("Parsed ALL files!!"))
                .flatMap(path -> parseXsbFile(path, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .doOnNext(xsbData -> {
                    // TBD adjust the frequency of reporting
                    if (counter.incrementAndGet() % 1000 == 0)
                        log.info("{} ... {} records", xsbData.getSourceXsbDataFileName(), counter.get());
                })
                .doOnComplete(() -> log.info("Finished Parsing. Parsed and converted to JSON a total of {} records", counter.get()));
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
     * @return Asynchronously return void once completed
     */
    @Transactional
    public Mono<Void> moveDataFromStagingToFinal() {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        try {
            // If there are too many errors, do not move the data to the final tables.
            return Mono.just(errorHandler.totalErrorsWithinAcceptableThreshold())
                    .filter(proceed -> proceed)
                    // TBD add functionality if it's a complete replace or an incremental update
                    .flatMap(proceed -> xsbDataRepository.deleteAll()
                            .doFirst(() -> log.info(msg))
                            .then(Mono.defer(xsbDataRepository::moveXsbData)))
                    .onErrorResume(e -> {
                        log.error(errMsg, e);
                        errorHandler.handleFileError("", errMsg, e);
                        return Mono.empty();
                    });
        } catch (Exception e) {
            log.error(errMsg, e);
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
                        log.error("Unable to delete the folder " + tmpDir);
                        return Mono.error(e);
                    }
                }))
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
                .flatMap(p -> acrXsbS3Util.uploadToS3(p, "errors/" + p.getFileName()));
    }


    /**
     * Collect all the results and generate a data object with the results.
     * @param errorFileNames Names of all the error files generated during the run.
     * @param errorHandler The error handler has all the valuable information regarding what worked and what failed.
     * @return A data object holding the merics of the data upload process execution.
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
        if (trigger == null) return Flux.error(new IllegalArgumentException("Invalid request body in POST"));
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Set<String> uniqueFileNames = trigger.getUniqueFileNames();
        if (uniqueFileNames != null && !uniqueFileNames.isEmpty())
            return xsbSourceFactory.xsbSource(trigger).getXSBFiles(trigger.getSourceFolder(), uniqueFileNames, tmpdir)
                    .doOnComplete(() -> log.info("Finished downloading all files."))
                    .doFinally(s -> errorHandler.close());
        else
            return Flux.error(new IllegalArgumentException("Invalid request body in POST. Either an array of file names or a file pattern is requires."));
    }

}
