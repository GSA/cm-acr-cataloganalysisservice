package gov.gsa.acr.cataloganalysis.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({
        "catalogMedianPrice",
        "catalogMedianPriceSupplier",
        "catalogMinPrice",
        "catalogMinPriceSupplier",
        "catalogPriceStandardDeviation",
        "commercialCatalogMedianPrice",
        "commercialCatalogMedianPriceSupplier",
        "commercialCatalogMinPrice",
        "commercialCatalogMinPriceSupplier",
        "countryOriginInference",
        "demandWeightedIndexScore",
        "dunsNumber",
        "enrichment_lower_bound",
        "enrichment_upper_bound",
        "ets",
        "exceeds_market_threshold",
        "finalPrice",
        "gsaFeedDate",
        "highPriceTarget",
        "hits",
        "invalidReason",
        "is_low_outlier",
        "is_market_research_found",
        "is_mia_risk",
        "is_taa_risk",
        "isAuthorizedVendor",
        "isProhibited",
        "lowPriceTarget",
        "nsn",
        "prohibitionComment",
        "salesLikelihood",
        "selfHits",
        "standardizedManufacturerName",
        "standardizedManufacturerPartNumber",
        "standardizedProductDescription",
        "standardizedUnitOfIssue",
        "tdrMedianPrice",
        "tdrMinPrice",
        "tdrMaxPrice",
        "uniqueItemIdentifier",
        "vppIndicator",
        "prohibitionCondition",
        "prohibitionReason",
        "rankCategory",
        "country_of_origin",
        "ppapi_invalid_fields",
        "modificationNumber",
        "solicitationNumber",
        "abilityOneItem",
        "globalPackagingIdentifier",
        "vendorName",
        "vendorDescription",
        "quantityOfUnit",
        "quantityPerUnit",
        "productName",
        "unspsc",
        "unitOfIssue",
        "productType",
        "deliveryFob",
        "fsc"
})
@Data
public class XsbDataJsonRecord {
    @JsonProperty("catalogMedianPrice")
    private Double catalogMedianPrice;
    @JsonProperty("catalogMedianPriceSupplier")
    private String catalogMedianPriceSupplier;
    @JsonProperty("catalogMinPrice")
    private Double catalogMinPrice;
    @JsonProperty("catalogMinPriceSupplier")
    private String catalogMinPriceSupplier;
    @JsonProperty("catalogPriceStandardDeviation")
    private Double catalogPriceStandardDeviation;
    @JsonProperty("commercialCatalogMedianPrice")
    private Double commercialCatalogMedianPrice;
    @JsonProperty("commercialCatalogMedianPriceSupplier")
    private String commercialCatalogMedianPriceSupplier;
    @JsonProperty("commercialCatalogMinPrice")
    private Double commercialCatalogMinPrice;
    @JsonProperty("commercialCatalogMinPriceSupplier")
    private String commercialCatalogMinPriceSupplier;
    @JsonProperty("countryOriginInference")
    private String countryOriginInference;
    @JsonProperty("demandWeightedIndexScore")
    private Double demandWeightedIndexScore;
    @JsonProperty("dunsNumber")
    private String dunsNumber;
    @JsonProperty("enrichment_lower_bound")
    private Double enrichmentLowerBound;
    @JsonProperty("enrichment_upper_bound")
    private Double enrichmentUpperBound;
    @JsonProperty("ets")
    private Boolean ets;
    @JsonProperty("exceeds_market_threshold")
    private Boolean exceedsMarketThreshold;
    @JsonProperty("finalPrice")
    private Double finalPrice;
    @JsonProperty("gsaFeedDate")
    private String gsaFeedDate;
    @JsonProperty("highPriceTarget")
    private Double highPriceTarget;
    @JsonProperty("hits")
    private Integer hits;
    @JsonProperty("invalidReason")
    private String invalidReason;
    @JsonProperty("is_low_outlier")
    private Boolean isLowOutlier;
    @JsonProperty("is_market_research_found")
    private Boolean isMarketResearchFound;
    @JsonProperty("is_mia_risk")
    private Boolean isMiaRisk;
    @JsonProperty("is_taa_risk")
    private Boolean isTaaRisk;
    @JsonProperty("isAuthorizedVendor")
    private Integer isAuthorizedVendor;
    @JsonProperty("isProhibited")
    private Boolean isProhibited;
    @JsonProperty("lowPriceTarget")
    private Double lowPriceTarget;
    @JsonProperty("nsn")
    private String nsn;
    @JsonProperty("prohibitionComment")
    private String prohibitionComment;
    @JsonProperty("salesLikelihood")
    private String salesLikelihood;
    @JsonProperty("selfHits")
    private Integer selfHits;
    @JsonProperty("standardizedManufacturerName")
    private String standardizedManufacturerName;
    @JsonProperty("standardizedManufacturerPartNumber")
    private String standardizedManufacturerPartNumber;
    @JsonProperty("standardizedProductDescription")
    private String standardizedProductDescription;
    @JsonProperty("standardizedUnitOfIssue")
    private String standardizedUnitOfIssue;
    @JsonProperty("tdrMedianPrice")
    private Double tdrMedianPrice;
    @JsonProperty("tdrMinPrice")
    private Double tdrMinPrice;
    @JsonProperty("tdrMaxPrice")
    private Double tdrMaxPrice;
    @JsonProperty("uniqueItemIdentifier")
    private String uniqueItemIdentifier;
    @JsonProperty("vppIndicator")
    private Integer vppIndicator;
    @JsonProperty("prohibitionCondition")
    private String prohibitionCondition;
    @JsonProperty("prohibitionReason")
    private String prohibitionReason;
    @JsonProperty("rankCategory")
    private String rankCategory;
    @JsonProperty("country_of_origin")
    private String countryOfOrigin;
    @JsonProperty("ppapi_invalid_fields")
    private List<String> ppapiInvalidFields;
    @JsonProperty("modificationNumber")
    private String modificationNumber;
    @JsonProperty("solicitationNumber")
    private String solicitationNumber;
    @JsonProperty("abilityOneItem")
    private String abilityOneItem;
    @JsonProperty("globalPackagingIdentifier")
    private String globalPackagingIdentifier;
    @JsonProperty("vendorName")
    private String vendorName;
    @JsonProperty("vendorDescription")
    private String vendorDescription;
    @JsonProperty("quantityOfUnit")
    private Integer quantityOfUnit;
    @JsonProperty("quantityPerUnit")
    private Integer quantityPerUnit;
    @JsonProperty("vendorPartNumber")
    private String vendorPartNumber;
    @JsonProperty("productName")
    private String productName;
    @JsonProperty("unspsc")
    private String unspsc;
    @JsonProperty("unitOfIssue")
    private String unitOfIssue;
    @JsonProperty("productType")
    private String productType;
    @JsonProperty("deliveryFob")
    private String deliveryFob;
    @JsonProperty("fsc")
    private String fsc;
    @JsonProperty("annualDemandQuantity")
    private Integer annualDemandQuantity;
    @JsonProperty("catalogAvgPrice")
    private Double catalogAvgPrice;
    @JsonProperty("catalogMaxPrice")
    private Double catalogMaxPrice;
    @JsonProperty("transactionMinPrice")
    private Double transactionMinPrice;
    @JsonProperty("transactionAvgPrice")
    private Double transactionAvgPrice;
    @JsonProperty("transactionMedianPrice")
    private Double transactionMedianPrice;
    @JsonProperty("transactionMaxPrice")
    private Double transactionMaxPrice;


    /**
     * Creates an object that will be saved as a JSON in the database.
     *
     * @param x               XSB Data as a Hashmap
     * @param taaCountryCodes
     */
    public XsbDataJsonRecord(Map<String, String> x, List<String> taaCountryCodes) {
        // Refer: See https://docs.google.com/spreadsheets/d/1YuZpJOBl9jkHgciPDsEkNmGiG5NBcuauSDU76lQvbEU/view#gid=173420408
        if (x == null) throw new NullPointerException("Cannot convert a NULL Map to XSB Data");
        String v = null; // temporary value holder from the map passed in as the argument
        String v1 = null; // temporary value holder from the map passed in as the argument
        String ls = System.getProperty("line.separator");
        StringBuffer sb = new StringBuffer();

        this.setModificationNumber(x.get("modificationNumber"));
        this.setSolicitationNumber(x.get("solicitationNumber"));
        this.setDunsNumber(x.get("dunsNumber"));
        try {
            v = x.get("quantityPerUnit");
            if (x.get("quantityPerUnit") != null)
                this.setQuantityPerUnit(Integer.valueOf(x.get("quantityPerUnit")));
        } catch (Exception e) {
            sb.append("Invalid data, Quantity Per Unit. Must be a valid number. Value encountered: " + x.get("quantityPerUnit"));
            sb.append(ls);
        }
        this.setAbilityOneItem(x.get("abilityOneItem"));
        this.setGlobalPackagingIdentifier(x.get("globalPackagingIdentifier"));
        this.setVendorName(x.get("vendorName"));
        this.setVendorDescription(x.get("vendorDescription"));
        this.setProductName(x.get("productName"));
        this.setUnspsc(x.get("unspsc"));
        this.setUnitOfIssue(x.get("unitOfIssue"));
        this.setProductType(x.get("productType"));
        this.setDeliveryFob(x.get("deliveryFob"));
        try {
            v = x.get("finalPrice");
            if (x.get("finalPrice") != null)
                this.setFinalPrice(Double.valueOf(x.get("finalPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Final Price. Must be a valid number. Value encountered: " + x.get("finalPrice"));
            sb.append(ls);
        }
        this.setFsc(x.get("fsc"));
        try {
            v = x.get("catalogPriceStandardDeviation");
            if (x.get("catalogPriceStandardDeviation") != null)
                this.setCatalogPriceStandardDeviation(Double.valueOf(x.get("catalogPriceStandardDeviation")));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Price Standard Deviation. Must be a valid number. Value encountered: " + x.get("catalogPriceStandardDeviation"));
            sb.append(ls);
        }
        this.setUniqueItemIdentifier(x.get("uniqueItemIdentifier"));
        this.setEts(Boolean.valueOf(x.get("ets")));
        this.setIsProhibited(Boolean.valueOf(x.get("isProhibited")));
        this.setStandardizedManufacturerName(x.get("standardizedManufacturerName"));
        this.setStandardizedManufacturerPartNumber(x.get("standardizedManufacturerPartNumber"));
        this.setStandardizedUnitOfIssue(x.get("standardizedUnitOfIssue"));

        // Begin ACREPO-2432
        try {
            v = x.get("catalogMedianPrice");
            if (x.get("catalogMedianPrice") != null)
                this.setCatalogMedianPrice(Double.valueOf(x.get("catalogMedianPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Median Price. Must be a valid number. Value encountered: " + x.get("catalogMedianPrice"));
            sb.append(ls);
        }
        this.setCatalogMedianPriceSupplier(x.get("catalogMedianPriceSupplier"));
        try {
            v = x.get("catalogMedianPrice");
            if (x.get("catalogMinPrice") != null) this.setCatalogMinPrice(Double.valueOf(x.get("catalogMinPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Min Price. Must be a valid number. Value encountered: " + x.get("catalogMinPrice"));
            sb.append(ls);
        }
        this.setCatalogMinPriceSupplier(x.get("catalogMinPriceSupplier"));
        try {
            v = x.get("commercialCatalogMedianPrice");
            if (v != null && !v.isBlank()) this.setCommercialCatalogMedianPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Commercial catalog median price. Must be a valid number. Value encountered: " + v);
            sb.append(ls);
        }
        this.setCommercialCatalogMedianPriceSupplier(x.get("commercialCatalogMedianPriceSupplier"));
        try {
            v = x.get("commercialCatalogMinPrice");
            if (v != null && !v.isBlank()) this.setCommercialCatalogMinPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Commercial catalog min price. Must be a valid number. Value encountered: " + v);
            sb.append(ls);
        }
        this.setCommercialCatalogMinPriceSupplier(x.get("commercialCatalogMinPriceSupplier"));
        this.setCountryOriginInference(x.get("countryOriginInference"));
        try {
            v = x.get("catalogMedianPrice");
            if (x.get("highPriceTarget") != null) this.setHighPriceTarget(Double.valueOf(x.get("highPriceTarget")));
        } catch (Exception e) {
            sb.append("Invalid data, High Price Target. Must be a valid number. Value encountered: " + x.get("highPriceTarget"));
            sb.append(ls);
        }
        this.setInvalidReason(x.get("invalidReason"));
        try {
            v = x.get("catalogMedianPrice");
            if (x.get("highPriceTarget") != null)
                this.setIsMarketResearchFound((Double.valueOf(x.get("highPriceTarget"))) > 0.0);
            else this.setIsMarketResearchFound(false);
        } catch (Exception e) {
            sb.append("Invalid data, High Price Target. Must be a valid number. Value encountered: " + x.get("highPriceTarget"));
            sb.append(ls);
        }
        try {
            v = x.get("lowPriceTarget");
            if (x.get("lowPriceTarget") != null) this.setLowPriceTarget(Double.valueOf(x.get("lowPriceTarget")));
        } catch (Exception e) {
            sb.append("Invalid data, Low Price Target. Must be a valid number. Value encountered: " + x.get("lowPriceTarget"));
            sb.append(ls);
        }
        this.setNsn(x.get("nsn"));
        this.setStandardizedProductDescription(x.get("standardizedProductDescription"));
        try {
            v = x.get("tdrMedianPrice");
            if (x.get("tdrMedianPrice") != null) this.setTdrMedianPrice(Double.valueOf(x.get("tdrMedianPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Median Price. Must be a valid number. Value encountered: " + x.get("tdrMedianPrice"));
            sb.append(ls);
        }
        try {
            v = x.get("tdrMinPrice");
            if (x.get("tdrMinPrice") != null) this.setTdrMinPrice(Double.valueOf(x.get("tdrMinPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Min Price. Must be a valid number. Value encountered: " + x.get("tdrMinPrice"));
            sb.append(ls);
        }
        try {
            v = x.get("tdrMaxPrice");
            if (x.get("tdrMaxPrice") != null) this.setTdrMaxPrice(Double.valueOf(x.get("tdrMaxPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Min Price. Must be a valid number. Value encountered: " + x.get("tdrMaxPrice"));
            sb.append(ls);
        }
        try {
            v = x.get("vppIndicator");
            if (x.get("vppIndicator") != null) this.setVppIndicator(Integer.valueOf(x.get("vppIndicator")));
        } catch (Exception e) {
            sb.append("Invalid data, VPP Indicator. Must be a valid number. Value encountered: " + x.get("vppIndicator"));
            sb.append(ls);
        }
        this.setProhibitionComment(x.get("prohibitionComment"));
        // End ACREPO-2432

        // Begin ACREPO-3215
        try {
            v = x.get("annualDemandQuantity");
            if (x.get("annualDemandQuantity") != null)
                this.setAnnualDemandQuantity(Integer.valueOf(x.get("annualDemandQuantity")));
        } catch (Exception e) {
            sb.append("Invalid data, Annual Demand Quantity. Must be a valid integer. Value encountered: " + x.get("annualDemandQuantity"));
            sb.append(ls);
        }

        try {
            v = x.get("catalogAvgPrice");
            if (x.get("catalogAvgPrice") != null) this.setCatalogAvgPrice(Double.valueOf(x.get("catalogAvgPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Average Price. Must be a valid number. Value encountered: " + x.get("catalogAvgPrice"));
            sb.append(ls);
        }

        try {
            v = x.get("catalogMedianPrice");
            if (x.get("catalogMedianPrice") != null)
                this.setCatalogMedianPrice(Double.valueOf(x.get("catalogMedianPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Median Price. Must be a valid number. Value encountered: " + x.get("catalogMedianPrice"));
            sb.append(ls);
        }

        try {
            v = x.get("catalogMaxPrice");
            if (x.get("catalogMaxPrice") != null) this.setCatalogMaxPrice(Double.valueOf(x.get("catalogMaxPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Max Price. Must be a valid number. Value encountered: " + x.get("catalogMaxPrice"));
            sb.append(ls);
        }

        try {
            v = x.get("catalogPriceStandardDeviation");
            if (x.get("catalogPriceStandardDeviation") != null)
                this.setCatalogPriceStandardDeviation(Double.valueOf(x.get("catalogPriceStandardDeviation")));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Price Standard Deviation. Must be a valid number. Value encountered: " + x.get("catalogPriceStandardDeviation"));
            sb.append(ls);
        }

        try {
            v = x.get("transactionMinPrice");
            if (x.get("transactionMinPrice") != null)
                this.setTransactionMinPrice(Double.valueOf(x.get("transactionMinPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Minimum Price. Must be a valid number. Value encountered: " + x.get("transactionMinPrice"));
            sb.append(ls);
        }

        try {
            v = x.get("transactionAvgPrice");
            if (x.get("transactionAvgPrice") != null)
                this.setTransactionAvgPrice(Double.valueOf(x.get("transactionAvgPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Average Price. Must be a valid number. Value encountered: " + x.get("transactionAvgPrice"));
            sb.append(ls);
        }

        try {
            v = x.get("transactionMedianPrice");
            if (x.get("transactionMedianPrice") != null)
                this.setTransactionMedianPrice(Double.valueOf(x.get("transactionMedianPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Median Price. Must be a valid number. Value encountered: " + x.get("transactionMedianPrice"));
            sb.append(ls);
        }

        try {
            v = x.get("transactionMaxPrice");
            if (x.get("transactionMaxPrice") != null)
                this.setTransactionMaxPrice(Double.valueOf(x.get("transactionMaxPrice")));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Maximum Price. Must be a valid number. Value encountered: " + x.get("transactionMaxPrice"));
            sb.append(ls);
        }
        // End ACREPO-3215

        try {
            v = x.get("isAuthorizedVendor");
            if (x.get("isAuthorizedVendor") != null)
                this.setIsAuthorizedVendor(Integer.valueOf(x.get("isAuthorizedVendor")));
        } catch (Exception e) {
            sb.append("Invalid data, Authorized vendor. Must be a valid integer. Value encountered: " + x.get("isAuthorizedVendor"));
            sb.append(ls);
        }
        try {
            v = x.get("hits");
            if (x.get("hits") != null) this.setHits(Integer.valueOf(x.get("hits")));
        } catch (Exception e) {
            sb.append("Invalid data, for hits. Must be a valid integer. Value encountered: " + x.get("hits"));
            sb.append(ls);
        }
        try {
            v = x.get("selfHits");
            if (x.get("selfHits") != null) this.setSelfHits(Integer.valueOf(x.get("selfHits")));
        } catch (Exception e) {
            sb.append("Invalid data, for Self hits. Must be a valid integer. Value encountered: " + x.get("selfHits"));
            sb.append(ls);
        }
        this.setRankCategory(x.get("rankCategory"));
        this.setSalesLikelihood(x.get("salesLikelihood"));
        try {
            v = x.get("demandWeightedIndexScore");
            if (x.get("demandWeightedIndexScore") != null)
                this.setDemandWeightedIndexScore(Double.valueOf(x.get("demandWeightedIndexScore")));
        } catch (Exception e) {
            sb.append("Invalid data, Demand Weighted Index Score. Must be a valid number. Value encountered: " + x.get("demandWeightedIndexScore"));
            sb.append(ls);
        }

        //Calculated fields.
        v = x.get("finalPrice");
        try {
            v1 = x.get("catalogPriceStandardDeviation");
            this.setEnrichmentLowerBound(0.0);
            if (x.get("finalPrice") != null && x.get("catalogPriceStandardDeviation") != null)
                this.setEnrichmentLowerBound(Double.parseDouble(x.get("finalPrice")) - Double.parseDouble(x.get("catalogPriceStandardDeviation")));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or Catalog price Standard Deviation. Must be a valid number. Value encountered: " + x.get("finalPrice") + ", " + x.get("catalogPriceStandardDeviation"));
            sb.append(ls);
        }
        try {
            v1 = x.get("catalogPriceStandardDeviation");
            this.setEnrichmentUpperBound(0.0);
            if (x.get("finalPrice") != null && x.get("catalogPriceStandardDeviation") != null)
                this.setEnrichmentUpperBound(Double.parseDouble(x.get("finalPrice")) + Double.parseDouble(x.get("catalogPriceStandardDeviation")));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or Catalog price Standard Deviation. Must be a valid number. Value encountered: " + x.get("finalPrice") + ", " + x.get("catalogPriceStandardDeviation"));
            sb.append(ls);
        }
        try {
            v1 = x.get("highPriceTarget");
            this.setExceedsMarketThreshold(false);
            if (x.get("finalPrice") != null && x.get("highPriceTarget") != null)
                this.setExceedsMarketThreshold(Double.parseDouble(x.get("finalPrice")) > Double.parseDouble(x.get("highPriceTarget")));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or high price target. Must be a valid number. Value encountered: " + x.get("finalPrice") + ", " + x.get("highPriceTarget"));
            sb.append(ls);
        }
        this.setIsTaaRisk(isTradeAgreementViolated(x.get("countryOriginInference"), taaCountryCodes)); //ACREPO-2143
        this.setIsMiaRisk(isMiaMisrepresented(x.get("countryOriginInference"), x.get("countryOfOrigin")));

        try {
            v1 = x.get("catalogMedianPrice");
            boolean isLowOutlier = false;
            if (x.get("catalogMedianPrice") != null && x.get("finalPrice") != null) {
                if (Double.parseDouble(x.get("catalogMedianPrice")) > 0 && (((Double.parseDouble(x.get("finalPrice")) / Double.parseDouble(x.get("catalogMedianPrice"))) - 1) < (-0.5))) {
                    //true if ([catalogMedianPrice] > 0) && ((([finalPrice]/[catalogMedianPrice])-1)<(-0.5))
                    isLowOutlier = true;
                }
            }
            this.setIsLowOutlier(isLowOutlier);
        } catch (Exception e) {
            sb.append("Invalid data, for Catalog Median Price or Final Price. Must be a valid number. Value encountered: " + x.get("catalogMedianPrice") + ", " + x.get("finalPrice"));
            sb.append(ls);
        }

        if (sb.length() > 0) throw new IllegalArgumentException(sb.toString());
    }

    private Boolean isTradeAgreementViolated(String countryOriginInference, List<String> taaCompliantCountryCodes){
        if (countryOriginInference != null && !countryOriginInference.isEmpty()) return !taaCompliantCountryCodes.contains(countryOriginInference);
        return Boolean.FALSE;
    }

    private Boolean isMiaMisrepresented(String countryOriginInference, String countryOfOrigin){
        if (countryOriginInference == null || countryOriginInference.isEmpty()) return Boolean.FALSE;
        else return countryOfOrigin != null && countryOfOrigin.equals("US") && !countryOriginInference.equals("US");
    }
}
