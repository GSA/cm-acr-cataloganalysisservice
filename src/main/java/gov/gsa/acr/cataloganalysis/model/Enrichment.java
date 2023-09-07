package gov.gsa.acr.cataloganalysis.model;

import io.r2dbc.postgresql.codec.Json;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Data
@Table("enrichment")
public class Enrichment {
    @Id
    private  Integer id;

    @Column("transaction_id")
    private Integer transactionId;

    @Column("contract_number")
    private String contractNumber;

    @Column("manufacturer")
    private String manufacturer;

    @Column("part_number")
    private String partNumber;

    @Column("enrichment_record")
    private Json enrichmentRecord;

    @Column("created_date")
    @CreatedDate
    private LocalDateTime createdDate;

    public XsbData toXsbData() {
        XsbData xsbData = new XsbData();
        xsbData.setContractNumber(contractNumber);
        xsbData.setManufacturer(manufacturer);
        xsbData.setPartNumber(partNumber);
        xsbData.setXsbData(enrichmentRecord);
        return xsbData;
    }
}
