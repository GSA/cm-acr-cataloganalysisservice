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
class XsbDataTest {

    @Autowired
    XsbDataParser xsbDataParser;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");


    @Test
    void testValidData(){
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData xsbData = new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertEquals("47QSWA18D000C", xsbData.getContractNumber());
        assertEquals("DUCKBACK PRODUCTS", xsbData.getManufacturer());
        assertEquals("DB0019115-20", xsbData.getPartNumber());
    }

    @Test
    void testInvalidContractNumber(){
        String xsbDataString = "~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows (IllegalArgumentException.class, () -> new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes));
    }


    @Test
    void testInvalidManufacturerName(){
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows (IllegalArgumentException.class, () -> new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes));
    }

    @Test
    void testInvalidManufacturerePartNumber(){
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~DUCKBACK PRODUCTS~|~~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows (IllegalArgumentException.class, () -> new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes));
    }

}