package gov.gsa.acr.cataloganalysis.model;

import lombok.Data;

@Data
public class Trigger {
    private String sourceType;
    private String filePattern;
    private String[] files;
    private Boolean monitor = Boolean.FALSE;
}
