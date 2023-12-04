package gov.gsa.acr.cataloganalysis.analysissource;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import org.springframework.stereotype.Component;

@Component
public class AnalysisSourceFactory {
    private final AnalysisSourceXsb analysisSourceXsb;
    private final AnalysisSourceLocal analysisSourceLocal;
    private final AnalysisSourceS3 analysisSourceS3;

    public AnalysisSourceFactory(AnalysisSourceXsb analysisSourceXsb, AnalysisSourceLocal analysisSourceLocal, AnalysisSourceS3 analysisSourceS3) {
        this.analysisSourceXsb = analysisSourceXsb;
        this.analysisSourceLocal = analysisSourceLocal;
        this.analysisSourceS3 = analysisSourceS3;
    }

    public AnalysisSource xsbSource(Trigger trigger){
        return switch (trigger.getSourceType()) {
            case SFTP  -> analysisSourceXsb;
            case S3    -> analysisSourceS3;
            case LOCAL -> analysisSourceLocal;
        };
    }
}
