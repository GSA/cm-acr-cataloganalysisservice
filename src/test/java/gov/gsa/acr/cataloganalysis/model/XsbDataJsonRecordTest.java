package gov.gsa.acr.cataloganalysis.model;

import gov.gsa.acr.cataloganalysis.service.XsbDataParser;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes = {XsbDataParser.class})
@TestPropertySource(locations="classpath:application-test.properties")
class XsbDataJsonRecordTest {
    @Autowired
    XsbDataParser xsbDataParser;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");


    @Test
    void testTradeAgreementViolation() {
        //Valid Country of Origin = US
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        XsbDataJsonRecord xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertFalse(xsbData.getIsTaaRisk());
        assertEquals("US", xsbData.getCountryOriginInference());

        // Country of Origin not available from XSB -- still TAA compliant
        xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertFalse(xsbData.getIsTaaRisk());
        assertEquals ("", xsbData.getCountryOriginInference());

        final String xsbDataString2 = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~CN~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString2), taaCountryCodes);
        assertTrue(xsbData.getIsTaaRisk());
        assertTrue(xsbData.getIsMiaRisk());
        assertEquals("CN", xsbData.getCountryOriginInference());
    }


    @Test
    void testMadeInAmericaRisk() {
        //Valid Country of Origin = US
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        XsbDataJsonRecord xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertFalse(xsbData.getIsMiaRisk());
        assertEquals("US", xsbData.getCountryOriginInference());
        assertEquals("US", xsbData.getCountryOfOrigin());

        // Country of Origin not available from XSB -- still TAA compliant
        xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertFalse(xsbData.getIsTaaRisk());
        assertEquals ("", xsbData.getCountryOriginInference());
        assertEquals("US", xsbData.getCountryOfOrigin());

        // Country Origin as per XSB = AF, Country origin claimed by Vendor = US
        final String xsbDataString2 = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~AF~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString2), taaCountryCodes);
        assertEquals("AF", xsbData.getCountryOriginInference());
        assertEquals("US", xsbData.getCountryOfOrigin());
        assertTrue(xsbData.getIsMiaRisk());


        // Country Origin as per XSB = US, Country origin claimed by Vendor = AF
        final String xsbDataString3 = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~AF~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString3), taaCountryCodes);
        assertFalse(xsbData.getIsMiaRisk());
        assertEquals("US", xsbData.getCountryOriginInference());
        assertEquals("AF", xsbData.getCountryOfOrigin());

        // Country Origin as per XSB = CN, Country origin claimed by Vendor = CN
        final String xsbDataString4 = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0~|~0~|~0~|~0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0~|~0~|~0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~CN~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.5~|~~|~CN~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        xsbData = new XsbDataJsonRecord(xsbDataParser.parseXsbDataToMap(xsbDataString4), taaCountryCodes);
        assertFalse(xsbData.getIsMiaRisk());
        assertEquals("CN", xsbData.getCountryOriginInference());
        assertEquals("CN", xsbData.getCountryOfOrigin());


    }


}