package gov.gsa.acr.cataloganalysis.repositories;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface XsbDataRepository extends ReactiveCrudRepository<XsbData, Integer> {
    String MOVE_XSB_DATA_QUERY = """
            WITH moved_rows AS (
            	DELETE FROM xsb_data_temp
            	RETURNING contract_number, manufacturer, part_number, xsb_data, created_date, modified_date
            )
            INSERT INTO xsb_data (contract_number, manufacturer, part_number, xsb_data, created_date, modified_date)
            SELECT * FROM moved_rows
            ON CONFLICT (contract_number, manufacturer, part_number)
            DO UPDATE SET xsb_data=EXCLUDED.xsb_data, modified_date=EXCLUDED.modified_date
            """;

    String INSERT_INTO_XSB_DATA_TEMP = """
            INSERT INTO xsb_data_temp (contract_number, manufacturer, part_number, xsb_data)
            VALUES (:contractNumber, :manufacturer, :partNumber, :xsbData)
            ON CONFLICT (contract_number, manufacturer, part_number) DO NOTHING
            RETURNING id
            """;



    @Query(value = "SELECT count(*) FROM xsb_data_temp")
    Mono<Integer> xsbDataTempCount();

    @Query(value = "SELECT * FROM xsb_data x WHERE x.contract_number = :contractNumber LIMIT :limit OFFSET :offset")
    Flux<XsbData> findAllByContractNumber(String contractNumber, Integer limit, Integer offset);


    @Query(value = MOVE_XSB_DATA_QUERY)
    Mono<Void> moveXsbData();

    @Query(value = INSERT_INTO_XSB_DATA_TEMP)
    Mono<Integer> saveXSBDataToTemp(String contractNumber, String manufacturer, String partNumber, Json xsbData);

    @Modifying
    @Query(value = "DELETE FROM xsb_data_temp")
    Mono<Void> deleteAllXsbDataTemp();

    @Query(value = "SELECT code FROM ppoint p WHERE p.is_ppoint = 'T'")
    Flux<String> findTaaCompliantCountries();

    @Query(value = "SELECT count(*) FROM xsb_data WHERE (xsb_data -> 'ets')::boolean")
    Mono<Integer> etsCount();

    @Query(value = "SELECT * FROM xsb_data WHERE (xsb_data -> 'ets')::boolean")
    Flux<XsbData> findAllETS();

    @Query(value = "SELECT count(*) FROM xsb_data WHERE (xsb_data -> 'is_low_outlier')::boolean")
    Mono<Integer> isLowOutlierCount();

    @Query(value = "SELECT * FROM xsb_data WHERE (xsb_data -> 'is_low_outlier')::boolean")
    Flux<XsbData> findAllLowOutliers();

    @Query(value = "SELECT count(*) FROM xsb_data WHERE (xsb_data -> 'is_mia_risk')::boolean")
    Mono<Integer> isMIARiskCount();

    @Query(value = "SELECT * FROM xsb_data WHERE (xsb_data -> 'is_mia_risk')::boolean")
    Flux<XsbData> findAllMIARisk();

    @Query(value = "SELECT count(*) FROM xsb_data WHERE (xsb_data -> 'exceeds_market_threshold')::boolean")
    Mono<Integer> exceedsMarketThresholdCount();

    @Query(value = "SELECT * FROM xsb_data WHERE (xsb_data -> 'exceeds_market_threshold')::boolean")
    Flux<XsbData> findAllExceedsMarketThreshold();

    @Query(value = "SELECT count(*) FROM xsb_data WHERE (xsb_data -> 'isProhibited')::boolean")
    Mono<Integer> isProhibitedCount();

    @Query(value = "SELECT * FROM xsb_data WHERE (xsb_data -> 'isProhibited')::boolean")
    Flux<XsbData> findAllProhibitedProducts();

    @Query(value = "SELECT count(*) FROM xsb_data WHERE (xsb_data -> 'is_taa_risk')::boolean")
    Mono<Integer> isTAARiskCount();

    @Query(value = "SELECT * FROM xsb_data WHERE (xsb_data -> 'is_taa_risk')::boolean")
    Flux<XsbData> findAllTAARisk();
}
