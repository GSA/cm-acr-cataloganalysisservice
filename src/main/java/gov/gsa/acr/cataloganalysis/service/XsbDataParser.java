package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
@Slf4j
public class XsbDataParser {
    private final ErrorHandler errorHandler;
    @Value("${xsb.report.file.delimiter}")
    private String defaultDelimiter;
    @Value("${xsb.report.file.header}")
    private String defaultHeader;
    @Value("${xsb.report.file.extended.header}")
    private String extendedHeader;
    @Getter
    private String delimRegex;
    @Getter
    private String delimString;
    @Getter
    private String[] header;
    @Getter
    private String[] extendedHeaderArray;
    @Getter
    private String headerString;

    @PostConstruct
    public void init(){
        delimRegex = defaultDelimiter;
        delimString = delimRegex.replace("\\", "");
        headerString = defaultHeader;
        header = headerString.split(delimRegex, -1);
        extendedHeaderArray = extendedHeader.split(delimRegex, -1);
    }


    public boolean validateHeader(String rawHeaderString){
        if (rawHeaderString == null) return false;
        if (rawHeaderString.startsWith(extendedHeader)) return true;
        else return rawHeaderString.startsWith(headerString);
    }

    private void validateRequest(String xsbDataString){
        if (xsbDataString == null || xsbDataString.isBlank()) throw new IllegalArgumentException("A Blank XSB data row.");
        if (!xsbDataString.contains(delimString))
            throw new IllegalArgumentException("Input string is not formatted correctly. it is not delimited with expected delimiter, " + delimString);
    }

    public String[] parseXsbDataToArray(String xsbDataString){
        // Validate the string. Throw exception if not valid. Otherwise, continue parsing.
        validateRequest(xsbDataString);
        String [] xsbDataAsArray = xsbDataString.split(delimRegex, -1);
        if (header.length > xsbDataAsArray.length)
            throw new IllegalArgumentException("Invalid XSB data row. The number of fields do not match expected count. Expected " + header.length + ", found " + xsbDataAsArray.length);
        if (header.length < xsbDataAsArray.length && extendedHeaderArray.length > xsbDataAsArray.length)
            throw new IllegalArgumentException("Invalid XSB data row. The number of fields do not match expected count. Expected " + extendedHeaderArray.length + ", found " + xsbDataAsArray.length);

        return xsbDataAsArray;
    }

    public Map<String, String> parseXsbDataToMap(String xsbDataString){
        String [] xsbDataAsArray = parseXsbDataToArray(xsbDataString);
        int numberOfFields;
        if (xsbDataAsArray.length == header.length) numberOfFields = header.length;
        else numberOfFields = extendedHeaderArray.length;
        return IntStream.range(0, numberOfFields)
                .boxed()
                .collect(Collectors.toMap(k -> extendedHeaderArray[k], v -> xsbDataAsArray[v]));
    }

    public XsbData parseXsbData(String xsbDataString, String sourceFileName, List<String> taaCountryCodes, LocalDate gsaFeedDate){
        // Check if we have too many errors already. If yes, no point moving forward, bail off now.
        if (!errorHandler.totalErrorsWithinAcceptableThreshold()) throw new NullPointerException("ignore");
        // Check if we are asked to force quit.
        if (errorHandler.getForceQuit()) {
            log.info("Terminating: The process is being forced to exit!");
            throw new NullPointerException("ignore");
        }

        if (sourceFileName == null || sourceFileName.isBlank()) throw new IllegalArgumentException("A Null source file name.");
        if (taaCountryCodes == null || taaCountryCodes.isEmpty()) throw new IllegalArgumentException("invalid list of Trade agreement country codes, either null or empty.");
        Map<String, String> parsedDataAsMap = parseXsbDataToMap(xsbDataString);
        if (gsaFeedDate != null) parsedDataAsMap.put("gsaFeedDate", gsaFeedDate.toString());
        XsbData xsbData = new XsbData(parsedDataAsMap, taaCountryCodes);
        xsbData.setSourceXsbDataString(xsbDataString);
        xsbData.setSourceXsbDataFileName(sourceFileName);
        return xsbData;
    }
}
