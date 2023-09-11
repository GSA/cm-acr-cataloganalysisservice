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

import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class XsbDataService {
    private final EnrichmentRepository enrichmentRepository;
    private final XsbDataRepository xsbDataRepository;

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


    @Transactional
    public Flux<XsbData> saveXSBData(Integer transaction_id, String contractNumber) {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger dbCounter = new AtomicInteger(0);
        log.info("Enrichment found with findAllByTransactionId: ");
        log.info("----------------------------------------------");
        return xsbDataRepository
                .deleteAllByContractNumber(contractNumber)
                .flatMapMany(n -> enrichmentRepository
                        .findAllByTransactionId(transaction_id, null, 0)
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
}
