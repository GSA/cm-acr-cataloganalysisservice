package gov.gsa.acr.cataloganalysis.analysissource;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import org.springframework.stereotype.Component;

@Component
public class AnalysisSourceFactory {
    private final AnalysisSourceSftp analysisSourceSftp;
    private final AnalysisSourceLocal analysisSourceLocal;
    private final AnalysisSourceS3 analysisSourceS3;

    public AnalysisSourceFactory(AnalysisSourceSftp analysisSourceSftp, AnalysisSourceLocal analysisSourceLocal, AnalysisSourceS3 analysisSourceS3) {
        this.analysisSourceSftp = analysisSourceSftp;
        this.analysisSourceLocal = analysisSourceLocal;
        this.analysisSourceS3 = analysisSourceS3;
    }

    public AnalysisSource xsbSource(Trigger trigger){
        return switch (trigger.getSourceType()) {
            case SFTP  -> analysisSourceSftp;
            case S3    -> analysisSourceS3;
            case LOCAL -> analysisSourceLocal;
        };
    }
}
