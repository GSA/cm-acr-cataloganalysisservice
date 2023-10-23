package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.util.AcrXsbFilesUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Service
@Slf4j
public class ErrorHandler {
    /**
     * A BoundedPrintWriter bounds the file it is writing in to a given size. This is needed since these error files
     * will have to be stored in S3 bucket and there is a 5GB transfer limit on the files. So each error file will be
     * limited to a 5 GB size limit. A new chunk will be created if a file reaches its size limit.
     */
    private static final class BoundedPrintWriter extends PrintWriter {
        private final long maxBytes;
        private long currentFileSizeInBytes;
        private final int lsBytes = System.getProperty("line.separator").getBytes().length;
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

        public void println(String x, long len) {
            ensureFileSizeWithinBounds(len);
            super.println(x);
        }

        public long numBytesAllowed(String x){
            long numBytesRequested = x.getBytes().length;
            if ( (this.currentFileSizeInBytes + numBytesRequested + lsBytes) < this.maxBytes) return numBytesRequested;
            return 0;
        }

        private void ensureFileSizeWithinBounds(long len) {
            long newFileSizeInBytes = this.currentFileSizeInBytes + len + lsBytes;
            if (newFileSizeInBytes >= this.maxBytes)
                throw new IllegalArgumentException("File size exceeded: " + newFileSizeInBytes + " > " + this.maxBytes);
            this.currentFileSizeInBytes = newFileSizeInBytes;
        }

    }

    @Value("${error.file.size.max.bytes.per.file}")
    private long maxErrorFileSizeBytes;
    @Value("${error.file.directory}")
    @Getter
    private String errorDirectory;

    @Value("${error.threshold}")
    private Integer errorThreshold;

    private BoundedPrintWriter errorMsgWriter;
    private BoundedPrintWriter parseErrorWriter;
    private BoundedPrintWriter dbErrorWriter;
    private int errorMsgChunk;
    private int parseErrorChunk;
    private int dbErrorChunk;
    private String timeStamp;

    @Getter
    @Setter
    private String header;
    private final String ls = System.getProperty("line.separator");
    @Getter
    private AtomicInteger numParsingErrors;
    @Getter
    private AtomicInteger numDbErrors;
    @Getter
    private AtomicInteger numFileErrors;

    @Getter
    @Setter
    private AtomicInteger numRecordsSavedInTempDB;

    private void deleteOldErrorFiles(){
        try (Stream<Path> stream = Files.list(Path.of(errorDirectory)).filter(Files::isRegularFile).filter(p->p.getFileName().toString().matches(AcrXsbFilesUtil.globToRegex("xsb_error_*")))) {
            stream.forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                   log.error("Unable to delete error file: " + p, e);
                }
            });
        }
        catch (IOException e){
            log.error("Unable to delete error files.", e);
        }

    }

    public void init(String header){
        numParsingErrors = new AtomicInteger(0);
        numDbErrors = new AtomicInteger(0);
        numFileErrors = new AtomicInteger(0);
        errorMsgChunk = 0;
        parseErrorChunk = 0;
        errorMsgWriter = null;
        parseErrorWriter = null;
        dbErrorWriter = null;
        this.header = header;
        timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
        deleteOldErrorFiles();
    }

    public void close(){
        if (errorMsgWriter != null) {
            errorMsgWriter.close();
            errorMsgWriter = null;
        }
        if (parseErrorWriter != null) {
            parseErrorWriter.close();
            parseErrorWriter = null;
        }
        if(dbErrorWriter != null){
            dbErrorWriter.close();
            dbErrorWriter = null;
        }
    }

    public Flux<Path> getErrorFiles(){
        return Flux.using(
                () ->  Files.list(Path.of(errorDirectory)).filter(Files::isRegularFile).filter(p->p.getFileName().toString().matches(AcrXsbFilesUtil.globToRegex("xsb_error_*_"+timeStamp+"_*"))),
                Flux::fromStream,
                Stream::close
        );
    }


    public void handleParsingError(String xsbRecord, String srcFileName, String error){
        handleError(xsbRecord, srcFileName, error, "PARSE");
    }

    public void handleDBError(XsbData xsbRecord, String error){
        handleError(xsbRecord.getSourceXsbDataString(), xsbRecord.getSourceXsbDataFileName(), error, "DB");
    }

    public void handleFileError(String srcFileName, String error, Throwable t){
        handleError(error, srcFileName, t.getClass().getSimpleName(), "FILE");
    }

    private void handleError(String xsbRecord, String srcFileName, String error, String errorType){
        boolean tryAgain = false;
        try {
            if (errorMsgWriter == null) {
                Path opPath = Path.of(getErrorMessageFileName());
                BufferedWriter bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                errorMsgWriter = new BoundedPrintWriter(bw, maxErrorFileSizeBytes);
            }
            if (errorType.equals("DB") && dbErrorWriter == null) {
                Path opPath = Path.of(getDBErrorFileName());
                BufferedWriter bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                dbErrorWriter = new BoundedPrintWriter(bw, maxErrorFileSizeBytes);
                if (header == null || header.isBlank()) log.error("Error initializing the errorHandler. Header string is null");
                else dbErrorWriter.println(header);
            }
            else if (errorType.equals("PARSE") && parseErrorWriter == null) {
                Path opPath = Path.of(getParseErrorFileName());
                BufferedWriter bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                parseErrorWriter = new BoundedPrintWriter(bw, maxErrorFileSizeBytes);
                if (header == null || header.isBlank()) log.error("Error initializing the errorHandler. Header string is null");
                else parseErrorWriter.println(header);
            }

            StringBuilder sb = new StringBuilder();
            sb.append(xsbRecord).append(ls)
                    .append("Source File: ").append(srcFileName).append(ls)
                    .append("Error (s):").append(ls)
                    .append(error).append(ls)
                    .append("------------------------------");

            // Check if this file has reached its max limit, if so then the number of allowed bytes will be zero.
            // Create a new chunk in that case.
            long numAllowedErrorMessageBytes = errorMsgWriter.numBytesAllowed(sb.toString());
            if (numAllowedErrorMessageBytes == 0) {
                errorMsgWriter.close();
                errorMsgWriter = null;
                tryAgain = true;
            }

            long numAllowedDbErrorBytes = 0;
            long numAllowedParseErrorBytes = 0;
            if (errorType.equals("DB")) {
                numAllowedDbErrorBytes = dbErrorWriter.numBytesAllowed(xsbRecord);
                if (numAllowedDbErrorBytes == 0) {
                    dbErrorWriter.close();
                    dbErrorWriter = null;
                    tryAgain = true;
                }
            }
            else if (errorType.equals("PARSE")){
                numAllowedParseErrorBytes = parseErrorWriter.numBytesAllowed(xsbRecord);
                if (numAllowedParseErrorBytes == 0) {
                    parseErrorWriter.close();
                    parseErrorWriter = null;
                    tryAgain = true;
                }
            }

            if (tryAgain) {
                handleError(xsbRecord, srcFileName, error, errorType);
            }
            else {
                errorMsgWriter.println(sb.toString(), numAllowedErrorMessageBytes);
                if (errorType.equals("DB") && numAllowedDbErrorBytes > 0) {
                    dbErrorWriter.println(xsbRecord, numAllowedDbErrorBytes);
                    numDbErrors.incrementAndGet();
                }
                else if (errorType.equals("PARSE") && numAllowedParseErrorBytes > 0) {
                    parseErrorWriter.println(xsbRecord, numAllowedParseErrorBytes);
                    numParsingErrors.incrementAndGet();
                }
                else if (errorType.equals("FILE") ) {
                    numFileErrors.incrementAndGet();
                }
            }

        } catch (Exception e) {
            log.error("Error while handling "+ errorType +" error messages. " + xsbRecord + " " + error, e);
        }

    }

    public Boolean proceedToMoveDataFromStagingToFinal(){
        return (numRecordsSavedInTempDB.get() > 0) && ((numDbErrors.get() + numParsingErrors.get()) < errorThreshold);
    }


    private String getErrorMessageFileName(){
        String errorMsgSuffix = ".txt";
        return errorDirectory + "/xsb_error_msg_" + timeStamp + "_" + errorMsgChunk++ + errorMsgSuffix;
    }

    private String getParseErrorFileName(){
        String parseErrorSuffix = ".gsa";
        return errorDirectory + "/xsb_error_parse_" + timeStamp + "_" + parseErrorChunk++ + parseErrorSuffix;
    }

    private String getDBErrorFileName(){
        String dbErrorSuffix = ".gsa";
        return errorDirectory + "/xsb_error_db_" + timeStamp + "_" + dbErrorChunk++ + dbErrorSuffix;
    }

}
