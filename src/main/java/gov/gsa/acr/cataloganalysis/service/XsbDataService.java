package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.Enrichment;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.EnrichmentRepository;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class XsbDataService {
    private final EnrichmentRepository enrichmentRepository;
    private final XsbDataRepository xsbDataRepository;
    private final XsbReportHandler xsbReportHandler;


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


    public Mono<Integer> saveXsbDataRecord(XsbData x, PrintWriter pw) {
        return xsbDataRepository.saveXSBDataToTemp(x.getContractNumber(), x.getManufacturer(), x.getPartNumber(), x.getXsbData())
                // TBD: Retry logic here
                //.retry(5)
                .onErrorResume(e -> {
                        log.error("Error saving record to DB. " + e.getMessage() + " " + x, e);
                        // TBD Error Handler: Possibly a recoverable Error
                        pw.println ("Error saving to DB: " + e.getMessage() + x);
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

    private static void close(Closeable closeable){
        try {
            closeable.close();
            log.info("Closed the resource");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    public static Flux<String> xsbResponseData(Path anXsbResponseFile) {
        final String MN = "xsbResponseData: ";
        AtomicInteger counter = new AtomicInteger(0);

        // First read the header row (First row of the File)
        try (Stream<String> rawProductsFromXSB = Files.lines(anXsbResponseFile)){
            Optional<String> mayBeHeader = rawProductsFromXSB.findFirst();
            String header = mayBeHeader.get();
            rawProductsFromXSB.close();
            XsbReportHandler.setHeader(String.valueOf(anXsbResponseFile), header);
        } catch (Exception e) {
            log.error(MN + "Ignoring this file because of the following error: " + e.getMessage(), e );
            return Flux.empty();
        }

        // Now create a Flux of all the lines from the file.
        return Flux.using(
                () -> Files.lines(anXsbResponseFile).skip(1),
                Flux::fromStream,
                Stream::close
                )
                .publishOn(Schedulers.parallel())
                .onErrorResume(e -> {
                    log.error(MN + "Ignoring this file because of the following error: " + e.getMessage(), e );
                    return Flux.empty();
                });
    }

    public static Flux<XsbData> parseXsbResponseFile(Path anXsbResponseFile, PrintWriter pw, List<String> taaCountryCodes) {
        final String MN = "parseXsbResponseFile: ";
        AtomicInteger counter = new AtomicInteger(0);

        // First read the header row (First row of the File)
        try (Stream<String> rawProductsFromXSB = Files.lines(anXsbResponseFile)){
            Optional<String> mayBeHeader = rawProductsFromXSB.findFirst();
            String header = mayBeHeader.get();
            XsbReportHandler.setHeader(String.valueOf(anXsbResponseFile), header);
        } catch (Exception e) {
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
                .onErrorContinue((e, s) -> {
                    pw.println (s + " has following errors: ");
                    pw.println(e.getMessage());
                    pw.println("---------------------------");
                });
    }


    public static Flux<XsbData> processXSBFiles(Flux<Path> xsbResponseFiles, PrintWriter pw, List<String> taaCountryCodes){
        final String MN = "processXSBFiles: ";
        AtomicInteger counter = new AtomicInteger(0);
        return xsbResponseFiles
                .doOnNext(p -> log.info(MN + "Processing file: " + String.valueOf(p)))
                .doOnError(e -> {log.error(MN + "error: " + e.getMessage(), e);})
                .doOnComplete(() -> {log.info(MN + "Got all files");})
                .flatMap(p -> parseXsbResponseFile(p, pw, taaCountryCodes))
                .doFirst(() -> counter.set(0))
                .doOnNext(xsbData -> {
                    if (counter.incrementAndGet() % 1000 == 0) {
                        log.info(MN + "Processing file {} ... Processed {} records", xsbData.getSourceXsbDataFileName(), counter.get());
                    }
                })
                .doOnComplete(() -> log.info(MN + "Finished. Processed a total of {} records", counter.get()));
    }

}
