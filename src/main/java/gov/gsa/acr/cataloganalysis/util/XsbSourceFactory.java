package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import org.springframework.stereotype.Component;

@Component
public class XsbSourceFactory {
    private final AcrXsbSftpUtil acrXsbSftpUtil;

    private final AcrXsbFilesUtil acrXsbFilesUtil;

    public XsbSourceFactory(AcrXsbSftpUtil acrXsbSftpUtil, AcrXsbFilesUtil acrXsbFilesUtil) {
        this.acrXsbSftpUtil = acrXsbSftpUtil;
        this.acrXsbFilesUtil = acrXsbFilesUtil;
    }

    public XsbSource xsbSource(Trigger trigger){
        XsbSourceType sourceType = trigger.getSourceType();
        if (sourceType == null) throw new IllegalArgumentException("Invalid Source type: null");

        switch (sourceType) {
            case SFTP -> {
                return acrXsbSftpUtil;
            }
            case S3 -> {
                // TBD implement this soon
                throw new UnsupportedOperationException("S3 support is still not implemented. Coming Soon!");
            }
            case LOCAL -> {
                return acrXsbFilesUtil;
            }
            default -> throw new IllegalStateException("Invalid source type: " + sourceType);
        }
    }
}
