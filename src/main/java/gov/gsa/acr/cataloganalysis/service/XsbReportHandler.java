package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
@Slf4j
public class XsbReportHandler {
    private static String DELIM_REGEX;
    private static String DELIM;

    @Value("${xsb.report.file.delimiter}")
    private String delimiter;
    private static String[] header;
    private static String headerString;

    @PostConstruct
    public void init(){
        DELIM_REGEX = delimiter;
        DELIM = DELIM_REGEX.replace("\\", "");
    }

    public static void resetHeader(){
        headerString = null;
        header = null;
    }

    public static void setHeader(Long index, String sourceFileName, String rawHeaderString) {
        if (rawHeaderString != null) {
            if (index == 0 || headerString == null) {
                headerString = rawHeaderString;
                if (!rawHeaderString.contains(DELIM))
                    throw new IllegalArgumentException("Header is not formatted correctly, it is not delimited with expected delimiter, " + DELIM);
                header = rawHeaderString.split(DELIM_REGEX, -1);
            }
            else if (!headerString.equals(rawHeaderString)) {
                throw new IllegalArgumentException("Header String for file " + sourceFileName + " is different from header strings for other files.");
            }
        }
        else {
            throw new IllegalArgumentException("Missing header string");
        }
    }

    public static XsbData mapRawXsbResponseToXsbDataPojo(String sourceFileName, String rawXsbResponseString, List<String> taaCountryCodes){
        if (sourceFileName == null) throw new IllegalArgumentException("A Null source file name.");
        if (rawXsbResponseString == null || rawXsbResponseString.isBlank()) throw new IllegalArgumentException("A Blank XSB data row.");
        if (!rawXsbResponseString.contains(DELIM))
            throw new IllegalArgumentException("Input string is not formatted correctly. it is not delimited with expected delimiter, " + DELIM);
        String [] xsbResponseAsArray = rawXsbResponseString.split(DELIM_REGEX, -1);

        if (header == null)
            throw new IllegalArgumentException("Null Header");

        if (xsbResponseAsArray.length != header.length)
            throw new IllegalArgumentException("Invalid XSB data row. The number of fields do not match expected count. Expected " + header.length + ", found " + xsbResponseAsArray.length);

        Map<String, String> xsbResponseAsAMap = IntStream.range(0, header.length)
                .boxed()
                .collect(Collectors.toMap(k -> header[k], v -> xsbResponseAsArray[v]));

        XsbData xsbData = new XsbData(xsbResponseAsAMap, taaCountryCodes);
        xsbData.setSourceXsbDataString(rawXsbResponseString);
        xsbData.setSourceXsbDataFileName(sourceFileName);
        return xsbData;
    }
}
