package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.Enrichment;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.EnrichmentRepository;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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
    private final EnrichmentRepository enrichmentRepository;
    private final XsbDataRepository xsbDataRepository;
    private final ErrorHandler errorHandler;

    @Transactional
    public Mono<Integer> deleteContract(String contractNumber){
        return xsbDataRepository.deleteAllByContractNumber(contractNumber);
    }

    public Flux<Enrichment> getEnrichment(Integer transaction_id) {
        return enrichmentRepository
                .findAllByTransactionId(transaction_id, null, 0)
                .doOnNext(e -> {
                    log.info("Found - {}", e.toString());
                });
    }


    // TBD Mark for deletion
    @Transactional
    public Flux<XsbData> saveXSBData(Integer txnId, String contractNumber) {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger dbCounter = new AtomicInteger(0);
        log.info("Enrichment found with findAllByTransactionId: ");
        log.info("----------------------------------------------");
        return xsbDataRepository
                .deleteAllByContractNumber(contractNumber)
                .flatMapMany(n -> enrichmentRepository
                        .findAllByTransactionId(txnId, null, 0)
                        .doFirst(() -> counter.set(0))
                        .doOnNext(e -> {
                            log.info("Processed {} records", counter.incrementAndGet());
                            //if (counter.incrementAndGet() % 100 == 0) log.info("Processed {} records", counter.get());
                        })
                        .map(Enrichment::toXsbData)
                        .onBackpressureBuffer()
                        //.buffer(1000)
                        .concatMap(xsbDataRepository::save, 100)
                        .doFirst(() -> dbCounter.set(0))
                        //.doOnNext(xsbDataRepository::save)
                        .doOnNext(e -> {
                            /*xsbDataRepository.save(e)
                                    .retry(5)
                                    .doOnSuccess(x -> log.info("Saved {} reeords", dbCounter.incrementAndGet()))
                                    .subscribe();*/
                            log.info("Saved {} reeords", dbCounter.incrementAndGet());
                            //if (dbCounter.incrementAndGet() % 100 == 0) log.info("Saved {} records", dbCounter.get());
                        })
                        .doOnError(e -> {
                            log.error("Error while saving to DB", e);
                            throw new RuntimeException(e);
                        }) //Rethrow as a RuntimeException to make the transaction fail
                        .doOnComplete(() -> log.info("Completed saving {} records", dbCounter.get())));
    }


    public Flux<Integer> saveXSBDataTemp(Integer txnId, Sinks.Many<String> statusNotifier) {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger dbCounter = new AtomicInteger(0);
        return cleanXsbDataTemp(statusNotifier)
                .thenMany(enrichmentRepository
                        .findAllByTransactionId(txnId, null, 0)
                        .doFirst(() -> {
                            counter.set(0);
                            if (statusNotifier != null) {
                                statusNotifier.tryEmitNext("Processing XSB Data\n");
                                statusNotifier.tryEmitNext("-------------------\n");
                            }
                            log.info("Processing XSB Data");
                            log.info("-------------------");
                        })
                        .doOnNext(e -> {
                            if (counter.incrementAndGet() % 1000 == 0) {
                                log.info("Processed {} records", counter.get());
                                if (statusNotifier != null)
                                    statusNotifier.tryEmitNext("Processed "+ counter.get() +" records\n");
                            }
                        })
                        .map(Enrichment::toXsbData)
                        .onBackpressureBuffer()
                        //.buffer(1000)
                        .flatMap( x -> xsbDataRepository.saveXSBDataToTemp(x.getContractNumber(), x.getManufacturer(), x.getPartNumber(), x.getXsbData()))
                        .doFirst(() -> {
                            dbCounter.set(0);
                            if (statusNotifier != null) {
                                statusNotifier.tryEmitNext("Saving XSB Data to Temp\n");
                                statusNotifier.tryEmitNext("-----------------------\n");
                            }
                            log.info("Saving XSB Data to Temp");
                            log.info("-----------------------");
                        })
                        .doOnNext(e -> {
                            if (dbCounter.incrementAndGet() % 1000 == 0) {
                                log.info("Saved {} records", dbCounter.get());
                                if (statusNotifier != null)
                                    statusNotifier.tryEmitNext("Saved "+ dbCounter.get() +" records\n");
                            }
                        })
                        .doOnError(e -> {
                            log.error("Error while saving to DB", e);
                            throw new RuntimeException(e);
                        }) //Rethrow as a RuntimeException to make the transaction fail
                        .doOnComplete(() -> {
                            log.info("Completed saving {} records", dbCounter.get());
                            if (statusNotifier != null)
                                statusNotifier.tryEmitNext("Completed saving "+ dbCounter.get() +" records\n");
                        })
                );
    }


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
        return Flux.fromIterable(fileNames).map(f -> Paths.get(f));
    }


    public static Flux<XsbData> parseXsbResponseFile(Path anXsbResponseFile, ErrorHandler eh, List<String> taaCountryCodes) {
        final String MN = "parseXsbResponseFile: ";
        AtomicInteger counter = new AtomicInteger(0);

        // First read the header row (First row of the File)
        try (Stream<String> rawProductsFromXSB = Files.lines(anXsbResponseFile)){
            Optional<String> mayBeHeader = rawProductsFromXSB.findFirst();
            String header = mayBeHeader.get();
            XsbReportHandler.setHeader(String.valueOf(anXsbResponseFile), header);
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


    public static Flux<XsbData> processXSBFiles(Flux<Path> xsbResponseFiles, ErrorHandler errorHandler, List<String> taaCountryCodes){
        final String MN = "processXSBFiles: ";
        AtomicInteger counter = new AtomicInteger(0);
        return xsbResponseFiles
                .doOnNext(p -> log.info(MN + "Processing file: " + String.valueOf(p)))
                .doOnError(e -> {
                    errorHandler.handleFileError("", e.getMessage(), e);
                    log.error(MN + "error: " + e.getMessage(), e);
                })
                .doOnComplete(() -> {log.info(MN + "Got all files");})
                .flatMap(p -> parseXsbResponseFile(p, errorHandler, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .doOnNext(xsbData -> {
                    if (counter.incrementAndGet() % 1000 == 0) {
                        log.info(MN + "{} ... {} records", xsbData.getSourceXsbDataFileName(), counter.get());
                    }
                })
                .doOnComplete(() -> log.info(MN + "Finished. Processed a total of {} records", counter.get()));
    }


    public void trigger(Trigger trigger) {
        if (executing.get()) throw new ConcurrentModificationException("Process is currently running!");
        errorHandler.init();
        AtomicInteger dbCounter = new AtomicInteger(0);

//        String[] fileNames = { "banana", "testData/fruits", "testData/47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa", "testData/47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa"
//                    , "testData/GS-06F-0052R-3008634_20230816153812_6606792615789196106_report_1.gsa"
//                    , "orange", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"};

        String[] fileNames = {"testData/testFileWithErrors.gsa", "banana", "testData/z1.gsa", "testData/z2.gsa"};


        //Flux<Path> filesFromList = xsbResponseFiles(Arrays.asList(fileNames));
        Flux<Path> filesFromList = xsbResponseFiles(Paths.get("testData"), "gsa");


        executing.compareAndSet(false, true);

        cleanXsbDataTemp(null)
                .then(
                        xsbDataRepository
                                .findTaaCompliantCountries()
                                .collectList()
                                .doOnError(e -> log.error("Unable to get a list of TAA compliant country codes. Exiting!", e))
                                .onErrorStop()
                                .flatMapMany(taaCountryCodes ->
                                        processXSBFiles(filesFromList, errorHandler, taaCountryCodes)
                                )
                                .onBackpressureBuffer()
                                .flatMap(x -> saveXsbDataRecord(x, errorHandler))
                                .doFirst(() -> dbCounter.set(0))
                                .doOnNext(e -> {
                                    if (dbCounter.incrementAndGet() % 1000 == 0) {
                                        log.info("Saved {} records", dbCounter.get());
                                    }
                                })
                                .doOnComplete(() -> {
                                    log.info("Finished. Saved a total of {} records", dbCounter.get());
                                    log.info("Number of parsing errors: " + errorHandler.getNumParsingErrors().get());
                                    log.info("Number of db errors: " + errorHandler.getNumDbErrors().get());
                                    log.info("Number of file errors: " + errorHandler.getNumFileErrors().get());
                                })
                                .then(moveXsbData(null))
                                .doOnSuccess(s -> log.info("All Done!!"))
                                .doFinally(s -> errorHandler.close())
                )
                .doFinally(s -> executing.compareAndSet(true, false))
                .subscribe(
                        s -> log.info("subscribe: " + s.toString()),
                        e -> log.error("Unexpected Error", e)
                );

    }

}
