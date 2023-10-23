package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import org.springframework.stereotype.Component;

@Component
public class XsbSourceFactory {
    private final AcrXsbSftpUtil acrXsbSftpUtil;

    private final AcrXsbFilesUtil acrXsbFilesUtil;
    private final AcrXsbS3Util acrXsbS3Util;

    public XsbSourceFactory(AcrXsbSftpUtil acrXsbSftpUtil, AcrXsbFilesUtil acrXsbFilesUtil, AcrXsbS3Util acrXsbS3Util) {
        this.acrXsbSftpUtil = acrXsbSftpUtil;
        this.acrXsbFilesUtil = acrXsbFilesUtil;
        this.acrXsbS3Util = acrXsbS3Util;
    }

    public XsbSource xsbSource(Trigger trigger){
        Trigger.XsbSourceType sourceType = trigger.getSourceType();
        if (sourceType == null) throw new IllegalArgumentException("Invalid Source type: null");

        switch (sourceType) {
            case SFTP -> {
                return acrXsbSftpUtil;
            }
            case S3 -> {
                return acrXsbS3Util;
            }
            case LOCAL -> {
                return acrXsbFilesUtil;
            }
            default -> throw new IllegalStateException("Invalid source type: " + sourceType);
        }
    }
}
