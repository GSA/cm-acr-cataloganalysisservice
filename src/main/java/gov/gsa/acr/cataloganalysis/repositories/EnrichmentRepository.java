package gov.gsa.acr.cataloganalysis.repositories;

import gov.gsa.acr.cataloganalysis.model.Enrichment;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface EnrichmentRepository extends ReactiveCrudRepository<Enrichment, Integer> {

    @Query(
            value = "SELECT * FROM enrichment e WHERE e.transaction_id = :transactionId LIMIT :limit OFFSET :offset"
    )
    Flux<Enrichment> findAllByTransactionId(Integer transactionId, Integer limit, Integer offset);

}
