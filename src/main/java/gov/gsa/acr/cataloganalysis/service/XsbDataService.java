package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.util.AcrXsbSftpUtil;
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
import java.nio.file.Paths;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
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
    private final AcrXsbSftpUtil acrXsbSftpUtil;

    private final String ls = System.getProperty("line.separator");
    public Mono<Integer> saveXsbDataRecord(XsbData x, ErrorHandler errorHandler) {
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
    public Mono<Void> moveXsbData(Sinks.Many<String> statusNotifier) {
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

    public Mono<Void> cleanXsbDataTemp(Sinks.Many<String> statusNotifier) {
        return xsbDataRepository.deleteAllXsbDataTemp().doFirst(() -> {
            if (statusNotifier != null) {
                statusNotifier.tryEmitNext("Cleaning up temporary data from xsb_data_temp\n");
                statusNotifier.tryEmitNext("---------------------------------------------\n");
            }
            log.info("Cleaning up temporary data from xsb_data_temp");
            log.info("----------------------------------------------");
        });
    }


    public static Flux<Path> xsbResponseFiles(Path xsbResponseDir, String responseFileExtension){
        return Flux.using(
                () ->  Files.list(xsbResponseDir).filter(Files::isRegularFile).filter(p->p.toString().endsWith(responseFileExtension)),
                Flux::fromStream,
                Stream::close
        );
    }

    public static Flux<Path> xsbResponseFiles(List<String> fileNames){
        return Flux.fromIterable(fileNames).map(Paths::get);
    }


    public static Flux<XsbData> parseXsbResponseFile(Path anXsbResponseFile, ErrorHandler eh, List<String> taaCountryCodes) {
        final String MN = "parseXsbResponseFile: ";

        // First read the header row (First row of the File)
        try (Stream<String> rawProductsFromXSB = Files.lines(anXsbResponseFile)){
            Optional<String> mayBeHeader = rawProductsFromXSB.findFirst();
            if (mayBeHeader.isPresent()) {
                String header = mayBeHeader.get();
                XsbReportHandler.setHeader(String.valueOf(anXsbResponseFile), header);
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


    public Flux<XsbData> processXSBFiles(Flux<Path> xsbResponseFiles, ErrorHandler errorHandler, List<String> taaCountryCodes, Sinks.Many<String> statusNotifier){
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
                .flatMap(p -> parseXsbResponseFile(p, errorHandler, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .doOnNext(xsbData -> {
                    if (counter.incrementAndGet() % 1000 == 0) {
                        log.info(MN + "{} ... {} records", xsbData.getSourceXsbDataFileName(), counter.get());
                        if (statusNotifier != null)
                            statusNotifier.tryEmitNext( xsbData.getSourceXsbDataFileName() + " ... "+ counter.get() +" records" + ls);
                    }
                })
                .doOnComplete(() -> {
                    log.info(MN + "Finished. Processed a total of {} records", counter.get());
                    if (statusNotifier != null)
                        statusNotifier.tryEmitNext("Finished. Processed a total of " +  counter.get() +" records" + ls);
                });
    }


    public void trigger(Trigger trigger, Sinks.Many<String> statusNotifier) {
        if (executing.get()) throw new ConcurrentModificationException("Process is currently running!");
        errorHandler.init();
        AtomicInteger dbCounter = new AtomicInteger(0);

//        String[] fileNames = { "banana", "testData/fruits", "testData/47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa", "testData/47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa"
//                    , "testData/GS-06F-0052R-3008634_20230816153812_6606792615789196106_report_1.gsa"
//                    , "orange", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"};

        String[] fileNames = {"testData/testFileWithErrors.gsa", "banana", "testData/z1.gsa", "testData/z2.gsa"};


        //Flux<Path> filesFromList = xsbResponseFiles(Arrays.asList(fileNames));
        Flux<Path> filesFromList = xsbResponseFiles(Paths.get("testData"), "gsa");

        cleanXsbDataTemp(statusNotifier)
                .then(
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
                                .then(moveXsbData(null))
                                .doOnSuccess(s -> {
                                    log.info("All Done!!");
                                    if (statusNotifier != null) {
                                        statusNotifier.tryEmitNext("All Done!!");
                                        statusNotifier.tryEmitComplete();
                                    }
                                })
                                .doFinally(s -> errorHandler.close())
                )
                .doFirst(() -> executing.compareAndSet(false, true))
                .doFinally(s -> executing.compareAndSet(true, false))
                .subscribe(
                        s -> log.info("subscribe: " + s),
                        e -> log.error("Unexpected Error", e)
                );
    }


    public Flux<Path> downloadReportsFromXSB(Trigger trigger, Sinks.Many<String> statusNotifier){
        String tmpdir;
        if (trigger == null) return Flux.error(new IllegalArgumentException("Invalid request body in POST"));
        try {
            tmpdir = Files.createTempDirectory("xsbReports").toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (trigger.getFiles() != null && trigger.getFiles().length > 0)
            return acrXsbSftpUtil.downloadFilesFromXSBToLocal(trigger.getFiles(), tmpdir);
        else if (trigger.getFilePattern() != null)
            return acrXsbSftpUtil.downloadFilesFromXSBToLocal(trigger.getFilePattern(), tmpdir);
        else
            return Flux.error(new IllegalArgumentException("Invalid request body in POST. Either an array of file names or a file pattern is requires."));
    }

}
