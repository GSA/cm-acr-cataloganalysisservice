package gov.gsa.acr.cataloganalysis.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class ErrorHandler {
    /**
     * A BoundedPrintWriter bounds the file it is writing in to a given size. This is needed since these error files
     * will have to be stored in S3 bucket and there is a 5GB transfer limit on the files. So each error file will be
     * limited to a 5 GB size limit. A new chunk will be created if a file reaches it's size limit.
     */
    private final class BoundedPrintWriter extends PrintWriter {
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
    private String errorDirectory;

    private BoundedPrintWriter errorMsgWriter;
    private BoundedPrintWriter parseErrorWriter;
    private int errorMsgChunk;
    private int parseErrorChunk;
    private String errorMsgSuffix = ".txt";
    private String parseErrorSuffix = ".gsa";
    private String timeStamp;
    private String ls = System.getProperty("line.separator");
    private AtomicInteger numParsingErrors;

    public void init(){
        numParsingErrors = new AtomicInteger(0);
        errorMsgChunk = 0;
        parseErrorChunk = 0;
        errorMsgWriter = null;
        parseErrorWriter = null;
        timeStamp = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
    }

    public void handleParsingError(String xsbRecord, String srcFileName, String error){
        boolean tryAgain = false;
        try {
            if (errorMsgWriter == null) {
                Path opPath = Paths.get(getFullErrorMessageFileName());
                BufferedWriter bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                errorMsgWriter = new BoundedPrintWriter(bw, maxErrorFileSizeBytes);
            }
            if (parseErrorWriter == null) {
                Path opPath = Paths.get(getFullParseErrorRecordsFileName());
                BufferedWriter bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                parseErrorWriter = new BoundedPrintWriter(bw, maxErrorFileSizeBytes);
            }

            StringBuffer sb = new StringBuffer();
            sb.append(xsbRecord)
                    .append(ls)
                    .append("Source File: ")
                    .append(srcFileName)
                    .append(ls)
                    .append("Error (s):")
                    .append(ls)
                    .append(error)
                    .append("------------------------------");

            // Check if this file has reached its max limit, if so then the number of allowed bytes will be zero.
            // Create a new chunk in that case.
            long numAllowedErrorMessageBytes = errorMsgWriter.numBytesAllowed(sb.toString());
            if (numAllowedErrorMessageBytes == 0) {
                errorMsgWriter.close();
                errorMsgWriter = null;
                tryAgain = true;
            }

            long numAllowedParseErrorBytes = parseErrorWriter.numBytesAllowed(xsbRecord);
            if (numAllowedParseErrorBytes == 0) {
                parseErrorWriter.close();
                parseErrorWriter = null;
                tryAgain = true;
            }

            if (tryAgain) {
                handleParsingError(xsbRecord, srcFileName, error);
            }
            else {
                errorMsgWriter.println(sb.toString(), numAllowedErrorMessageBytes);
                parseErrorWriter.println(xsbRecord, numAllowedParseErrorBytes);
                numParsingErrors.incrementAndGet();
            }

        } catch (Exception e) {
            log.error("Error while handling parse error messages. " + xsbRecord + " " + error, e);
        }

    }

    private String getFullErrorMessageFileName(){
        return errorDirectory + "/xsb_error_msg_" + timeStamp + "_" + errorMsgChunk++ + errorMsgSuffix;
    }

    private String getFullParseErrorRecordsFileName(){
        return errorDirectory + "/xsb_parse_error_" + timeStamp + "_" + parseErrorChunk++ + parseErrorSuffix;
    }
}
