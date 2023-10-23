package gov.gsa.acr.cataloganalysis.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
public class Trigger {
    @Schema(description = """
    Source for XSB Files. Has to be one of "SFTP", or "LOCAL", or "S3".
    "SFTP": will look for files on the XSB's SFTP server,
    "S3": Will look for file in the ACR's S3 bucket,
    "LOCAL": Will look for files on the PODs local file system.
    """)
    private XsbSourceType sourceType;
    @Schema(description = """
            The folder relative to the base of the source. For example, for the SFTP source the base is the root folder "/".
            For the S3 bucket the base is the folder "catalogAnalysis"  and for the local file system the base is the
            root folder "/".
            """,
            requiredMode = Schema.RequiredMode.NOT_REQUIRED
    )
    private String sourceFolder;
    @Schema(description = """
            A list of either full file names or glob like patterns. e.g.
            "files" : [ "47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa", "47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa" ]
            or\s
            "files" : ['myTestSftp*.gsa', "47QSWA19D0073-3003521**"]
            
            Except in case where the source is S3 bucket. The glob pattern does not work in case of S3, the the values in the files array
            are used as prefix only for looking for files in the S3 bucket.
            """)
    private String[] files;
    @Schema(hidden = true)
    private Set<String> uniqueFileNames;

    public Set<String> getUniqueFileNames(){
        if (uniqueFileNames == null) {
            if (files != null && files.length > 0) {
                uniqueFileNames = new HashSet<>();
                Collections.addAll(uniqueFileNames, files);
            }
        }
        return uniqueFileNames;
    }

    public enum XsbSourceType {
        SFTP, LOCAL, S3
    }
}
