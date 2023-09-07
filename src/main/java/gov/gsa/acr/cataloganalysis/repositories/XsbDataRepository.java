package gov.gsa.acr.cataloganalysis.repositories;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface XsbDataRepository extends ReactiveCrudRepository<XsbData, Integer> {

    @Query(
            value = "SELECT * FROM xsb_data x WHERE x.contract_number = :contractNumber LIMIT :limit OFFSET :offset"
    )
    Flux<XsbData> findAllByContractNumber(String contractNumber, Integer limit, Integer offset);

    Mono<Integer> deleteAllByContractNumber(String contractNumber);
}
