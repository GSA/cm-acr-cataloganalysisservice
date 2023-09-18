package gov.gsa.acr.cataloganalysis.repositories;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface XsbDataRepository extends ReactiveCrudRepository<XsbData, Integer> {

    String INSERT_INTO_XSB_DATA_QUERY = """
            INSERT INTO xsb_data (contract_number, manufacturer, part_number, xsb_data) 
            SELECT contract_number, manufacturer, part_number, enrichment_record as "xsb_data"
            FROM enrichment 
            WHERE transaction_id = :txnId
            """;

    String MOVE_XSB_DATA_QUERY = """
            INSERT INTO xsb_data (contract_number, manufacturer, part_number, xsb_data, created_date, modified_date) 
            SELECT contract_number, manufacturer, part_number, xsb_data, created_date, modified_date
            FROM xsb_data_temp 
            """;

    String INSERT_INTO_XSB_DATA_TEMP = """
            INSERT INTO xsb_data_temp (contract_number, manufacturer, part_number, xsb_data) 
            VALUES (:contractNumber, :manufacturer, :partNumber, :xsbData)
            ON CONFLICT (contract_number, manufacturer, part_number) DO NOTHING
            RETURNING id
            """;




    @Query(value = "SELECT * FROM xsb_data x WHERE x.contract_number = :contractNumber LIMIT :limit OFFSET :offset")
    Flux<XsbData> findAllByContractNumber(String contractNumber, Integer limit, Integer offset);


    @Query(value = INSERT_INTO_XSB_DATA_QUERY)
    Flux<XsbData> bulkSave(Integer txnId);

    @Query(value = MOVE_XSB_DATA_QUERY)
    Mono<Void> moveXsbData();

    @Query(value = INSERT_INTO_XSB_DATA_TEMP)
    Mono<Integer> saveXSBDataToTemp(String contractNumber, String manufacturer, String partNumber, Json xsbData);

    @Modifying
    @Query(value = "DELETE FROM xsb_data_temp")
    Mono<Void> deleteAllXsbDataTemp();

    Mono<Integer> deleteAllByContractNumber(String contractNumber);
}
