package gov.gsa.acr.cataloganalysis;

import gov.gsa.acr.cataloganalysis.model.Enrichment;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.EnrichmentRepository;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@Slf4j
@EnableR2dbcAuditing
public class CmAcrCataloganalysisserviceApplication {
    final
    EnrichmentRepository enrichmentRepository;
    final XsbDataRepository xsbDataRepository;

    public CmAcrCataloganalysisserviceApplication(EnrichmentRepository enrichmentRepository, XsbDataRepository xsbDataRepository) {
        this.enrichmentRepository = enrichmentRepository;
        this.xsbDataRepository = xsbDataRepository;
    }

    public static void main(String[] args) {
        SpringApplication.run(CmAcrCataloganalysisserviceApplication.class, args);
    }

    @Bean
    public CommandLineRunner demo() {
        return (args) -> {
            AtomicInteger counter = new AtomicInteger(0);

            //saveXSBData(3004804,"GS-35F-0119Y").blockLast();

            log.info("XSB Data  found with findAllByContractorNumber: ");
            log.info("=================================================");
            /*xsbDataRepository.findAllByContractNumber("GS-35F-0119Y", null, 0)
                    .doFirst(() -> counter.set(0))
                    .doOnNext(x -> {if(counter.incrementAndGet() % 1000 ==0) log.info(x.toString());})
                    .doOnComplete(() -> log.info("================================================="))
                    .subscribe();

             */
        };
    }


    @Transactional
    public Flux<XsbData> saveXSBData(Integer transaction_id, String contractNumber) {
        AtomicInteger counter = new AtomicInteger(0);

        xsbDataRepository.deleteAllByContractNumber(contractNumber).block();

        log.info("Enrichment found with findAllByTransactionId: ");
        log.info("----------------------------------------------");
        return enrichmentRepository
                .findAllByTransactionId(transaction_id, null, 0)
                .doFirst(() -> counter.set(0))
                .doOnNext(e -> {
                    if(counter.incrementAndGet() % 1000 ==0) log.info("Processed {} records", counter.get());
                })
                .map(Enrichment::toXsbData)
                .flatMap(xsbDataRepository::save)
                .doOnError(e -> {throw new RuntimeException(e);}) //Rethrow as a RuntimeException to make the transaction fail
                .doOnComplete(()->log.info("Saved {} records", counter.get()));
    }

}
