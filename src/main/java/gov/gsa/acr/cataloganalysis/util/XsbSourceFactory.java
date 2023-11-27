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
        return switch (trigger.getSourceType()) {
            case SFTP  -> acrXsbSftpUtil;
            case S3    -> acrXsbS3Util;
            case LOCAL -> acrXsbFilesUtil;
        };
    }
}
