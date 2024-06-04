package gov.gsa.acr.cataloganalysis.model;

import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.service.XsbDataParser;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Slf4j
@MockBeans({@MockBean(ErrorHandler.class)})
@ContextConfiguration(classes = {XsbDataParser.class})
@TestPropertySource(locations="classpath:application-test.properties")
class XsbDataTest {

    @Autowired
    XsbDataParser xsbDataParser;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");

    @Test
    void testNullData(){
        assertThrows(NullPointerException.class, () -> new XsbData(null, taaCountryCodes));
    }

    @Test
    void testNullContractNumber(){
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        Map<String, String> map = xsbDataParser.parseXsbDataToMap(xsbDataString);
        map.replaceAll((k, v) -> null);
        assertThrows(IllegalArgumentException.class, () -> new XsbData(map, taaCountryCodes));
    }

    @Test
    void testValidData(){
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData xsbData = new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertEquals("47QSWA18D000C", xsbData.getContractNumber());
        assertEquals("DUCKBACK PRODUCTS", xsbData.getManufacturer());
        assertEquals("DB0019115-20", xsbData.getPartNumber());
    }

    @Test
    void testJsonData() throws JSONException {
        String xsbDataString = "47QSMA21D08R6~|~123~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~TRUE~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0~|~0~|~0~|~0~|~0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0~|~0~|~0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        XsbData xsbData = new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertEquals("47QSMA21D08R6", xsbData.getContractNumber());
        assertEquals("AMERICAN SIGNAL COMPANY", xsbData.getManufacturer());
        assertEquals("OPT30125380", xsbData.getPartNumber());
        String expectedJSON = """
                {"vendorName":"AMERICAN SIGNAL COMPANY",
                "modificationNumber": "123",
                "vendorDescription":"Verizon VPN with ITS Cloud Manager per year subscription, available for all models",
                "dunsNumber":"612764845",
                "quantityPerUnit":1,
                "unitOfIssue":"EA",
                "standardizedManufacturerName":"AMERICAN SIGNAL",
                "standardizedManufacturerPartNumber":"OPT30125380",
                "standardizedUnitOfIssue":"EA",
                "productName":"VERIZON VPN WITH ITS CLOUD MANAGER PER Y",
                "standardizedProductDescription":"Verizon VPN with ITS Cloud Manager per year subscription, available for all models",
                "uniqueItemIdentifier":"91580958",
                "hits":1,
                "selfHits":1,
                "ets":false,
                "deliveryFob":"PP",
                "finalPrice":344.58,
                "lowPriceTarget":344.58,
                "highPriceTarget":437.27,
                "catalogMinPrice":344.58,
                "catalogAvgPrice":344.58,
                "catalogMedianPrice":344.58,
                "catalogMaxPrice":344.58,
                "catalogPriceStandardDeviation":0,
                "transactionMinPrice":0,
                "transactionAvgPrice":0,
                "transactionMedianPrice":0,
                "transactionMaxPrice":0,
                "catalogMinPriceSupplier":"AMERICAN SIGNAL COMPANY 47QSMA21D08R6",
                "catalogMedianPriceSupplier":"AMERICAN SIGNAL COMPANY 47QSMA21D08R6",
                "demandWeightedIndexScore":0,
                "rankCategory":"Unknown",
                "salesLikelihood":"Unknown",
                "isAuthorizedVendor":9,
                "isProhibited":false,
                "country_of_origin":"US", "enrichment_lower_bound": 344.58,
                "enrichment_upper_bound": 344.58,
                "exceeds_market_threshold": false,
                "is_low_outlier": false,
                "is_market_research_found": true,
                "is_mia_risk": false,
                "is_taa_risk": false}
                """;
        JSONAssert.assertEquals(expectedJSON, xsbData.getXsbData().asString(), JSONCompareMode.STRICT);
    }

    @Test
    void testInvalidContractNumber(){
        String xsbDataString = "~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows (IllegalArgumentException.class, () -> new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes));
    }


    @Test
    void testContractNumberWithRefresh(){
        String xsbDataString = "47QSWA18D000C_refresh~|~~|~MONO MACHINES LLC~|~104479~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~false~|~DUCKBACK PRODUCTS~|~DB0019115-20~|~~|~1~|~EA~|~DUCKBACK PRODUCTS~|~DB001911520~|~EA~|~~|~~|~~|~~|~~|~5GAL CedarTon EXT Stain~|~~|~5GAL CedarTon EXT Stain~|~5 Gallon, Cedar-Tone, Exterior Transparent Stain, VOC Less Than 350.~|~6068624~|~1~|~8~|~0~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~156.36~|~151.79~|~173.75~|~195.7~|~151.79~|~172.46~|~176.43~|~186.91~|~11.86~|~0.0~|~0.0~|~0.0~|~0.0~|~HARDWARE ASSOCIATES, INC 47QSHA18D0027~|~GORILLA STATIONERS LLC 47QSEA20D006H~|~HARDWARE, INC. GS-21F-0104W~|~0.0~|~0.0~|~0.0~|~225.74~|~257.52~|~289.29~|~225.74~|~225.74~|~225.74~|~225.74~|~0.00~|~http://www.walmart.com~|~http://www.walmart.com~|~http://www.walmart.com~|~US~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~87.50~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData xsbData = new XsbData(xsbDataParser.parseXsbDataToMap(xsbDataString), taaCountryCodes);
        assertEquals("47QSWA18D000C", xsbData.getContractNumber());
        assertEquals("DUCKBACK PRODUCTS", xsbData.getManufacturer());
        assertEquals("DB0019115-20", xsbData.getPartNumber());
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

    @Test
    void testInvalidXsbDataJsonRecord(){
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~BZ36E287~|~BZ DEFENSE SKU# BZ36E287 -- MFG. Name: Vermont Gage -- MPN: 362158010 -- UNSPSC= 41111619 -- UOM: EA -- UOM QTY: 1 -- PKG QTY: 1 -- MOQ: 1 Description: Threaded Ring Gage: M27 x 2.00 Thread, Class 6G, Go Class 6G, Tool Steel, NIST Traceability Certification Included. FOR QUOTES, VOLUME DISCOUNTS, STOCK AVAILABILITY OR SPECIAL REQUIREMENTS, PLEASE CONTACT BZ DEFENSE @ +1 (888) 305-5357 OR EMAIL US: GSASales@BZDefense.com~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~VERMONT GAGE~|~362158010~|~~|~1~|~EA~|~VERMONT PRECISION TOOLS~|~362158010~|~EA~|~~|~~|~~|~~|~~|~Threaded Ring Gage: M27 x 2.00 Thread,~|~~|~M27x2.00 Go Single Ring Thread Gage~|~BZ DEFENSE SKU# BZ36E287 -- MFG. Name: Vermont Gage -- MPN: 362158010 -- UNSPSC= 41111619 -- UOM: EA -- UOM QTY: 1 -- PKG QTY: 1 -- MOQ: 1 Description: Threaded Ring Gage: M27 x 2.00 Thread, Class 6G, Go Class 6G, Tool Steel, NIST Traceability Certification Included. FOR QUOTES, VOLUME DISCOUNTS, STOCK AVAILABILITY OR SPECIAL REQUIREMENTS, PLEASE CONTACT BZ DEFENSE @ +1 (888) 305-5357 OR EMAIL US: GSASales@BZDefense.com~|~17585722~|~1~|~11~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~347.73~|~307.63~|~349.61~|~391.58~|~307.63~|~344.1~|~catalogMedianPrice~|~426.86~|~31.33~|~0~|~0~|~0~|~0~|~BAHFED CORP GS-35F-0431Y~|~BOTACH INC. 47QSHA19D003Y~|~THE OFFICE GROUP INC GS-21F-0054X~|~0~|~0~|~0~|~432.77~|~488.97~|~545.16~|~432.77~|~432.77~|~432.77~|~432.77~|~0~|~http://www.mscdirect.com~|~http://www.mscdirect.com~|~http://www.mscdirect.com~|~US~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        final Map<String, String> map = xsbDataParser.parseXsbDataToMap(xsbDataString);
       assertThrows(IllegalArgumentException.class, () -> new XsbData(map, taaCountryCodes) );
    }


    @Test
    void testUnParseableXsbDataJsonRecord(){
        char ctrlB = 0x7F;
        String xsbDataString = "47QSWA18D000C~|~~|~MONO MACHINES LLC~|~BZ36E287~|~BZ DEFENSE" + ctrlB + " SKU# BZ36E287 -- MFG. Name: Vermont Gage -- MPN: 362158010 -- UNSPSC= 41111619 -- UOM: EA -- UOM QTY: 1 -- PKG QTY: 1 -- MOQ: 1 Description: Threaded Ring Gage: M27 x 2.00 Thread, Class 6G, Go Class 6G, Tool Steel, NIST Traceability Certification Included. FOR QUOTES, VOLUME DISCOUNTS, STOCK AVAILABILITY OR SPECIAL REQUIREMENTS, PLEASE CONTACT BZ DEFENSE @ +1 (888) 305-5357 OR EMAIL US: GSASales@BZDefense.com~|~~|~~|~C4S6Z1ALKEP1~|~314120~|~332510C~|~FALSE~|~VERMONT GAGE~|~362158010~|~~|~1~|~EA~|~VERMONT PRECISION TOOLS~|~362158010~|~EA~|~~|~~|~~|~~|~~|~Threaded Ring Gage: M27 x 2.00 Thread,~|~~|~M27x2.00 Go Single Ring Thread Gage~|~BZ DEFENSE SKU# BZ36E287 -- MFG. Name: Vermont Gage -- MPN: 362158010 -- UNSPSC= 41111619 -- UOM: EA -- UOM QTY: 1 -- PKG QTY: 1 -- MOQ: 1 Description: Threaded Ring Gage: M27 x 2.00 Thread, Class 6G, Go Class 6G, Tool Steel, NIST Traceability Certification Included. FOR QUOTES, VOLUME DISCOUNTS, STOCK AVAILABILITY OR SPECIAL REQUIREMENTS, PLEASE CONTACT BZ DEFENSE @ +1 (888) 305-5357 OR EMAIL US: GSASales@BZDefense.com~|~17585722~|~1~|~11~|~0~|~~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~FALSE~|~PP~|~~|~1243.34~|~307.63~|~349.61~|~391.58~|~307.63~|~344.1~|~343.34~|~426.86~|~100.25~|~0~|~0~|~0~|~0~|~BAHFED CORP GS-35F-0431Y~|~BOTACH INC. 47QSHA19D003Y~|~THE OFFICE GROUP INC GS-21F-0054X~|~0~|~0~|~0~|~432.77~|~488.97~|~545.16~|~432.77~|~432.77~|~432.77~|~432.77~|~0~|~http://www.mscdirect.com~|~http://www.mscdirect.com~|~http://www.mscdirect.com~|~US~|~0~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~FALSE~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100~|~~|~US~|~FALSE~|~FALSE~|~~|~~|~~|~~|~";
        Map<String, String> map = xsbDataParser.parseXsbDataToMap(xsbDataString);
        String s = """
                ÒÄçþéåÓ╣ñ 
                �[40m�[32minfo�[39m�[22m�[49m: Microsoft.AspNetCore.DataProtection.KeyManagement.XmlKeyManager[0]
                      User profile is available. Using '/home/anvba/.aspnet/DataProtection-Keys' as key repository; keys will not be encrypted at rest.
                
                """;
        map.put("vendorDescription", s);
        assertDoesNotThrow(() -> new XsbData(map, taaCountryCodes));
    }

}