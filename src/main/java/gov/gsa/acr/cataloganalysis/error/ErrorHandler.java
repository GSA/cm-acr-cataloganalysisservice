package gov.gsa.acr.cataloganalysis.error;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.util.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Component
@Slf4j
public class ErrorHandler {
    private final String ls = System.lineSeparator();
    @Value("${error.file.size.max.bytes.per.file}")
    @Getter
    private long maxErrorFileSizeBytes;

    @Value("${error.file.directory}")
    @Getter
    @Setter
    private String errorDirectory;

    @Value("${error.threshold}")
    @Getter
    private Integer errorThreshold;

    private BoundedPrintWriter errorMsgWriter;
    private BoundedPrintWriter parseErrorWriter;
    private BoundedPrintWriter dbErrorWriter;
    private int errorMsgChunk;
    private int parseErrorChunk;
    private int dbErrorChunk;
    private String timeStamp;
    private boolean totalErrorsWithinAcceptableThreshnold;

    @Getter
    @Setter
    private Boolean forceQuit;

    @Getter
    private String header;
    @Getter
    private AtomicInteger numParsingErrors;
    @Getter
    private AtomicInteger numDbErrors;
    @Getter
    private AtomicInteger numFileErrors;
    @Getter
    @Setter
    private Boolean dataUploadFailed;
    @Getter
    @Setter
    private List<String> errorFileNames;

    private static final String PARSE = "PARSE";
    private static final String ERROR_MSG = "Error initializing the errorHandler. Header string is null";

    private void deleteOldErrorFiles() {
        try (Stream<Path> stream = Files.list(Path.of(errorDirectory))
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().matches(StringUtils.globToRegex("xsb_error_*")))) {
            stream.forEach(p -> {
                try {
                    log.info("Cleaning up error directory, deleting old error file, " + p + ", from a previous execution.");
                    Files.deleteIfExists(p);
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected error. Unable to delete old error file from a previous execution: " + p, e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error. Unable to delete old error files from previous executions.", e);
        }
    }

    public void init(String header) {
        totalErrorsWithinAcceptableThreshnold = true;
        numParsingErrors = new AtomicInteger(0);
        numDbErrors = new AtomicInteger(0);
        numFileErrors = new AtomicInteger(0);
        dataUploadFailed = Boolean.FALSE;
        errorMsgChunk = 0;
        parseErrorChunk = 0;
        dbErrorChunk = 0;
        errorMsgWriter = null;
        parseErrorWriter = null;
        dbErrorWriter = null;
        errorFileNames = null;
        this.header = header;
        forceQuit = Boolean.FALSE;
        timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());

        try {
            Files.createDirectories(Path.of(errorDirectory));
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error. Unable to create a new directory for storing error files.", e);
        }

        deleteOldErrorFiles();
    }

    public void close() {
        if (errorMsgWriter != null) {
            errorMsgWriter.close();
            errorMsgWriter = null;
        }
        if (parseErrorWriter != null) {
            parseErrorWriter.close();
            parseErrorWriter = null;
        }
        if (dbErrorWriter != null) {
            dbErrorWriter.close();
            dbErrorWriter = null;
        }
        forceQuit = Boolean.FALSE;
    }

    public Flux<Path> getErrorFiles() {
        return Flux.using(
                        () -> Files.list(Path.of(errorDirectory))
                                .filter(Files::isRegularFile)
                                .filter(p -> p.getFileName().toString().matches(StringUtils.globToRegex("xsb_error_*_" + timeStamp + "_*"))),
                        Flux::fromStream,
                        Stream::close
                )
                .onErrorResume(e -> {
                    log.error("Unable to get the error files.", e);
                    return Flux.empty();
                });
    }

    public void handleParsingError(String xsbRecord, String srcFileName, String error) {
        numParsingErrors.incrementAndGet();
        handleError(xsbRecord, srcFileName, error, PARSE);
    }

    public void handleDBError(XsbData xsbRecord, String error) {
        numDbErrors.incrementAndGet();
        handleError(xsbRecord.getSourceXsbDataString(), xsbRecord.getSourceXsbDataFileName(), error, "DB");
    }

    public void handleFileError(String srcFileName, String error, Throwable t) {
        numFileErrors.incrementAndGet();
        handleError(error, srcFileName, t.toString(), "FILE");
    }

    private BoundedPrintWriter createBoundedPrintWriter(String errorFileName) throws IOException {
        return new BoundedPrintWriter(Files.newBufferedWriter(Path.of(errorFileName), StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING), maxErrorFileSizeBytes);
    }

    private void handleError(String xsbRecord, String srcFileName, String error, String errorType) {
        if (totalErrorsWithinAcceptableThreshnold) {
            totalErrorsWithinAcceptableThreshnold = ((numDbErrors.get() + numParsingErrors.get()) < errorThreshold);
        }
        boolean tryAgain = false;
        try {
            if (errorMsgWriter == null) errorMsgWriter = createBoundedPrintWriter(getErrorMessageFileName());
            if (errorType.equals("DB") && dbErrorWriter == null) {
                dbErrorWriter = createBoundedPrintWriter(getDBErrorFileName());
                if (header != null && !header.isBlank()) dbErrorWriter.println(header);
            } else if (errorType.equals(PARSE) && parseErrorWriter == null) {
                parseErrorWriter = createBoundedPrintWriter(getParseErrorFileName());
                if (header != null && !header.isBlank()) parseErrorWriter.println(header);
            }
            if (header == null || header.isBlank()) log.error(ERROR_MSG);

            StringBuilder sb = new StringBuilder();
            sb.append(xsbRecord).append(ls)
                    .append("Source File: ").append(srcFileName).append(ls)
                    .append("Error (s):").append(ls)
                    .append(error).append(ls)
                    .append("------------------------------");

            // Check if this file has reached its max limit, if so then the number of allowed bytes will be zero.
            // Create a new chunk in that case.
            if (errorMsgWriter.numBytesAllowed(sb.toString()) == 0) {
                errorMsgWriter.close();
                errorMsgWriter = null;
                tryAgain = true;
            }

            if (errorType.equals("DB") && (dbErrorWriter.numBytesAllowed(xsbRecord) == 0)) {
                dbErrorWriter.close();
                dbErrorWriter = null;
                tryAgain = true;
            } else if (errorType.equals(PARSE) && (parseErrorWriter.numBytesAllowed(xsbRecord) == 0)) {
                parseErrorWriter.close();
                parseErrorWriter = null;
                tryAgain = true;
            }

            if (tryAgain) {
                handleError(xsbRecord, srcFileName, error, errorType);
            } else {
                errorMsgWriter.println(sb.toString());
                if (errorType.equals("DB")) dbErrorWriter.println(xsbRecord);
                else if (errorType.equals(PARSE))
                    parseErrorWriter.println(xsbRecord);
            }

        } catch (Exception e) {
            log.error("Error while handling " + errorType + " error messages. " + xsbRecord + " " + error, e);
        }

    }

    public boolean totalErrorsWithinAcceptableThreshold() {
        return totalErrorsWithinAcceptableThreshnold;
    }

    private String getErrorMessageFileName() {
        String errorMsgSuffix = ".txt";
        return errorDirectory + "/xsb_error_msg_" + timeStamp + "_" + errorMsgChunk++ + errorMsgSuffix;
    }

    private String getParseErrorFileName() {
        String parseErrorSuffix = ".gsa";
        return errorDirectory + "/xsb_error_parse_" + timeStamp + "_" + parseErrorChunk++ + parseErrorSuffix;
    }

    private String getDBErrorFileName() {
        String dbErrorSuffix = ".gsa";
        return errorDirectory + "/xsb_error_db_" + timeStamp + "_" + dbErrorChunk++ + dbErrorSuffix;
    }

    PrintWriter testBoundedPrintWriter(int maxAllowedBytes) {
        return new BoundedPrintWriter(new StringWriter(maxAllowedBytes), maxAllowedBytes);
    }

    /**
     * A BoundedPrintWriter bounds the file it is writing in to a given size. This is needed since these error files
     * will have to be stored in S3 bucket and there is a 5GB transfer limit on the files. So each error file will be
     * limited to a 5 GB size limit. A new chunk will be created if a file reaches its size limit.
     */
    private static final class BoundedPrintWriter extends PrintWriter {
        private final long maxBytes;
        private final int lsBytes = System.lineSeparator().getBytes().length;
        private long currentFileSizeInBytes;

        /**
         * Creates a new PrintWriter, without automatic line flushing.
         *
         * @param out A character-output stream
         */
        public BoundedPrintWriter(Writer out, long maxBytes) {
            super(out);
            this.maxBytes = maxBytes;
            this.currentFileSizeInBytes = 0;
        }

        @Override
        public void println(String x) {
            long bytesAllowed = numBytesAllowed(x);
            if (bytesAllowed > 0) {
                this.currentFileSizeInBytes = this.currentFileSizeInBytes + bytesAllowed + lsBytes;
                super.println(x);
            } else {
                long numBytesRequested = this.currentFileSizeInBytes + x.getBytes().length + lsBytes;
                throw new IllegalArgumentException("File size exceeded: " + numBytesRequested + " > " + this.maxBytes);
            }
        }

        public long numBytesAllowed(String x) {
            long numBytesRequested = x.getBytes().length;
            if (numBytesRequested > maxBytes)
                throw new IllegalArgumentException("Error message is too long (" + numBytesRequested + " bytes) and exceeds the maximum allowed size for the error file (" + maxBytes + " bytes)");
            if ((this.currentFileSizeInBytes + numBytesRequested + lsBytes) < maxBytes) return numBytesRequested;
            return 0;
        }

    }

}
