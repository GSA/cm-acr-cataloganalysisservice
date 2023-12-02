package gov.gsa.acr.cataloganalysis.xsbsource;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import org.springframework.stereotype.Component;

@Component
public class XsbSourceFactory {
    private final XsbSourceSftpFiles xsbSourceSftpFiles;
    private final XsbSourceLocalFiles xsbSourceLocalFiles;
    private final XsbSourceS3Files xsbSourceS3Files;

    public XsbSourceFactory(XsbSourceSftpFiles xsbSourceSftpFiles, XsbSourceLocalFiles xsbSourceLocalFiles, XsbSourceS3Files xsbSourceS3Files) {
        this.xsbSourceSftpFiles = xsbSourceSftpFiles;
        this.xsbSourceLocalFiles = xsbSourceLocalFiles;
        this.xsbSourceS3Files = xsbSourceS3Files;
    }

    public XsbSource xsbSource(Trigger trigger){
        return switch (trigger.getSourceType()) {
            case SFTP  -> xsbSourceSftpFiles;
            case S3    -> xsbSourceS3Files;
            case LOCAL -> xsbSourceLocalFiles;
        };
    }
}
