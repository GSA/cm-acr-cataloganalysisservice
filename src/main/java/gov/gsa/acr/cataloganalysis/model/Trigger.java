package gov.gsa.acr.cataloganalysis.model;

import lombok.Data;

import java.util.List;

@Data
public class Trigger {
    private String sourceType;
    private String filePattern;
    private List<String> files;
    private Boolean monitor = Boolean.FALSE;
}
