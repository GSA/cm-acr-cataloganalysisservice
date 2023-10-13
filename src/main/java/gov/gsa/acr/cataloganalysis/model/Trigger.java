package gov.gsa.acr.cataloganalysis.model;

import gov.gsa.acr.cataloganalysis.util.XsbSourceType;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
public class Trigger {
    private XsbSourceType sourceType;
    private String sourceFolder;
    private String[] files;
    private Boolean monitor = Boolean.FALSE;
    private Set<String> uniqueFileNames;

    public Set<String> getUniqueFileNames(){
        if (uniqueFileNames == null) {
            if (files != null && files.length > 0) {
                uniqueFileNames = new HashSet<>();
                for (String fn : files) uniqueFileNames.add(fn);
            }
        }
        return uniqueFileNames;
    }
}
