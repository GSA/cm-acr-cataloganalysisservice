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
                            if (counter.incrementAndGet() % 1000 == 0) log.info("Processed {} records", counter.get());
                        })
                        .map(Enrichment::toXsbData)
                        .buffer(1000)
                        .flatMap(xsbDataRepository::saveAll)
                        .doFirst(() -> dbCounter.set(0))
                        .doOnNext(e -> {if (dbCounter.incrementAndGet() % 1000 == 0) log.info("Saved {} records", dbCounter.get());})
                        .doOnError(e -> {
                            throw new RuntimeException(e);
                        }) //Rethrow as a RuntimeException to make the transaction fail
                        .doOnComplete(() -> log.info("Saved {} records", counter.get())));
    }
}
