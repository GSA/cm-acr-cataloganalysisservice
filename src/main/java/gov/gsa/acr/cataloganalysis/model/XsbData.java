package gov.gsa.acr.cataloganalysis.model;

import io.r2dbc.postgresql.codec.Json;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Data
@Table("xsb_data")
public class XsbData {
    @Id
    private  Integer id;

    @Column("contract_number")
    private String contractNumber;

    @Column("manufacturer")
    private String manufacturer;

    @Column("part_number")
    private String partNumber;

    @Column("xsb_data")
    private Json xsbData;

    @Column("created_date")
    @CreatedDate
    private LocalDateTime createdDate;
}
