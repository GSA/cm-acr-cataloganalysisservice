package gov.gsa.acr.cataloganalysis.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.postgresql.codec.Json;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.Map;

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

    @Transient
    private String sourceXsbDataString;

    @Transient
    private String sourceXsbDataFileName;


    public XsbData(Map<String, String> xsbDataAsAMap) {
        // Refer: See https://docs.google.com/spreadsheets/d/1YuZpJOBl9jkHgciPDsEkNmGiG5NBcuauSDU76lQvbEU/view#gid=173420408
        if (xsbDataAsAMap == null) throw new NullPointerException("Cannot convert a NULL Map to XSB Data");
        String ls = System.getProperty("line.separator");
        StringBuffer sb = new StringBuffer();

        String contractNumber = xsbDataAsAMap.get("contractNumber");
        if (contractNumber == null || contractNumber.isBlank() ) {
            sb.append("Invalid data, contract number cannot be NULL or Blank.");
            sb.append(ls);
        }
        else this.setContractNumber(contractNumber);

        String manufacturerName = xsbDataAsAMap.get("manufacturerName");
        if ( manufacturerName == null || manufacturerName.isBlank()) {
            sb.append("Invalid data, Manufacturer Name cannot be NULL or Blank.");
            sb.append(ls);
        }
        else this.setManufacturer(manufacturerName);

        String manufacturerPartNumber = xsbDataAsAMap.get("manufacturerPartNumber");
        if (manufacturerPartNumber == null || manufacturerPartNumber.isBlank()) {
            sb.append("Invalid data, manufacturer Part Number cannot be NULL or Blank.");
            sb.append(ls);
        }
        else this.setPartNumber(manufacturerPartNumber);

        try {
            XsbDataJsonRecord xsbDataJsonRecord = new XsbDataJsonRecord(xsbDataAsAMap);
            ObjectMapper objectMapper = new ObjectMapper();
            this.setXsbData(Json.of(objectMapper.writeValueAsString(xsbDataJsonRecord)));
        } catch (JsonProcessingException e) {
            sb.append("Could not convert XSB response data to JSON. " + e.getMessage());
            sb.append(ls);
        }
        catch (Exception e){
            sb.append(e.getMessage());
            sb.append(ls);
        }

        if (sb.length() > 0) throw new IllegalArgumentException(sb.toString());
    }

    public XsbData() {
    }
}
