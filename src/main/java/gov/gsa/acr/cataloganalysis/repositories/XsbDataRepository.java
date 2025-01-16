package gov.gsa.acr.cataloganalysis.repositories;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface XsbDataRepository extends ReactiveCrudRepository<XsbData, Integer> {
    /*
            The Query used to move data from the staging table to the production table. This is an upsert query.

            WITH moved_rows AS (
            	DELETE FROM xsb_data_temp
            	RETURNING contract_number, manufacturer, part_number, xsb_data, true as is_source_bimonthly_file, created_date, modified_date
            )
            INSERT INTO xsb_data (contract_number, manufacturer, part_number, xsb_data, is_source_bimonthly_file, created_date, modified_date)
            SELECT * FROM moved_rows
            ON CONFLICT (contract_number, manufacturer, part_number)
            DO UPDATE SET xsb_data=EXCLUDED.xsb_data,  is_source_bimonthly_file=EXCLUDED.is_source_bimonthly_file, modified_date=EXCLUDED.modified_date

     */

    String MOVE_XSB_DATA_QUERY_PART_1 = """
            WITH moved_rows AS (
            	DELETE FROM xsb_data_temp_""";
    String MOVE_XSB_DATA_QUERY_PART_2 ="""
            	RETURNING contract_number, manufacturer, part_number, xsb_data, true as is_source_bimonthly_file, created_date, modified_date
            )
            INSERT INTO xsb_data (contract_number, manufacturer, part_number, xsb_data, is_source_bimonthly_file, created_date, modified_date)
            SELECT * FROM moved_rows
            ON CONFLICT (contract_number, manufacturer, part_number)
            DO UPDATE SET xsb_data=EXCLUDED.xsb_data,  is_source_bimonthly_file=EXCLUDED.is_source_bimonthly_file, modified_date=EXCLUDED.modified_date
            """;


    String INSERT_INTO_XSB_DATA_TEMP = """
            INSERT INTO xsb_data_temp (contract_number, manufacturer, part_number, xsb_data)
            VALUES (:contractNumber, :manufacturer, :partNumber, :xsbData)
            ON CONFLICT (contract_number, manufacturer, part_number)
            DO UPDATE SET xsb_data=EXCLUDED.xsb_data, created_date=EXCLUDED.created_date, modified_date=EXCLUDED.modified_date
            """;

    @Query(value = "SELECT count(*) FROM xsb_data_temp")
    Mono<Integer> xsbDataTempCount();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "0 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_0();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "1 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_1();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "2 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_2();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "3 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_3();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "4 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_4();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "5 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_5();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "6 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_6();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "7 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_7();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "8 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_8();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "9 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_9();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "10 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_10();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "11 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_11();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "12 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_12();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "13 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_13();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "14 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_14();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "15 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_15();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "16 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_16();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "17 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_17();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "18 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_18();

    @Query(value = MOVE_XSB_DATA_QUERY_PART_1 + "19 " + MOVE_XSB_DATA_QUERY_PART_2)
    Mono<Void> moveXsbData_19();



    @Query(value = INSERT_INTO_XSB_DATA_TEMP)
    Mono<Void> saveXSBDataToTemp(String contractNumber, String manufacturer, String partNumber, Json xsbData);

    @Modifying
    @Query(value = "DELETE FROM xsb_data_temp")
    Mono<Void> deleteAllXsbDataTemp();

    @Query(value = "SELECT code FROM ppoint p WHERE p.is_ppoint = 'T'")
    Flux<String> findTaaCompliantCountries();

    //@Query(value = "SELECT count(*) FROM xsb_data WHERE (xsb_data -> 'ets')::boolean")
    @Query(value = "select count from (SELECT pg_sleep(3900), count(0) FROM xsb_data WHERE (xsb_data -> 'ets')::boolean) a")
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
