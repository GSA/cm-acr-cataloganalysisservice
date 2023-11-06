package gov.gsa.acr.cataloganalysis.model;

import lombok.Data;

import java.util.List;

@Data
public class DataUploadResults {
    private List<String> errorFileNames;
    private Integer numParsingErrors;
    private Integer numDbErrors;
    private Integer numFileErrors;
    private Integer numRecordsSavedInTempDB;
}
