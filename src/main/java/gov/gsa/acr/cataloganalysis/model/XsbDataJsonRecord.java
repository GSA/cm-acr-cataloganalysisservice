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
     * @param xsbData               XSB Data as a Hashmap
     * @param taaCountryCodes List of countries that do not violate trade agreement
     */
    public XsbDataJsonRecord(Map<String, String> xsbData, List<String> taaCountryCodes) {
        // Refer:https://docs.google.com/spreadsheets/d/1YuZpJOBl9jkHgciPDsEkNmGiG5NBcuauSDU76lQvbEU/view#gid=173420408
        if (xsbData == null) throw new NullPointerException("Cannot convert a NULL Map to XSB Data");
        String val = null;
        String ls = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();

        this.setModificationNumber(xsbData.get("modificationNumber"));
        this.setVendorName(xsbData.get("vendorName"));
        this.setVendorPartNumber(xsbData.get("vendorPartNumber"));
        this.setVendorDescription(xsbData.get("vendorDescription"));
        this.setSolicitationNumber(xsbData.get("solicitationNumber"));
        this.setDunsNumber(xsbData.get("dunsNumber"));
        try {
            val = xsbData.get("quantityOfUnit");
            if (val != null && !val.isBlank())
                this.setQuantityOfUnit(Integer.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Quantity Of Unit. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        try {
            val = xsbData.get("quantityPerUnit");
            if (val != null && !val.isBlank())
                this.setQuantityPerUnit(Integer.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Quantity Per Unit. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setAbilityOneItem(xsbData.get("abilityOneItem"));
        this.setGlobalPackagingIdentifier(xsbData.get("globalPackagingIdentifier"));
        this.setProductName(xsbData.get("productName"));
        this.setUnspsc(xsbData.get("unspsc"));
        this.setUnitOfIssue(xsbData.get("unitOfIssue"));
        this.setProductType(xsbData.get("productType"));
        this.setDeliveryFob(xsbData.get("deliveryFob"));
        try {
            val = xsbData.get("finalPrice");
            if (val != null && !val.isBlank())
                this.setFinalPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Final Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setFsc(xsbData.get("fsc"));
        try {
            val = xsbData.get("catalogPriceStandardDeviation");
            if (val != null && !val.isBlank())
                this.setCatalogPriceStandardDeviation(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Price Standard Deviation. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setUniqueItemIdentifier(xsbData.get("uniqueItemIdentifier"));
        this.setEts(Boolean.valueOf(xsbData.get("ets")));
        this.setIsProhibited(Boolean.valueOf(xsbData.get("isProhibited")));
        this.setStandardizedManufacturerName(xsbData.get("standardizedManufacturerName"));
        this.setStandardizedManufacturerPartNumber(xsbData.get("standardizedManufacturerPartNumber"));
        this.setStandardizedUnitOfIssue(xsbData.get("standardizedUnitOfIssue"));

        // Begin ACREPO-2432
        try {
            val = xsbData.get("catalogMedianPrice");
            if (val != null && !val.isBlank()) this.setCatalogMedianPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Median Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setCatalogMedianPriceSupplier(xsbData.get("catalogMedianPriceSupplier"));
        try {
            val = xsbData.get("catalogMinPrice");
            if (val != null && !val.isBlank()) this.setCatalogMinPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Min Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setCatalogMinPriceSupplier(xsbData.get("catalogMinPriceSupplier"));
        try {
            val = xsbData.get("commercialCatalogMedianPrice");
            if (val != null && !val.isBlank()) this.setCommercialCatalogMedianPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Commercial catalog median price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setCommercialCatalogMedianPriceSupplier(xsbData.get("commercialCatalogMedianPriceSupplier"));
        try {
            val = xsbData.get("commercialCatalogMinPrice");
            if (val != null && !val.isBlank()) this.setCommercialCatalogMinPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Commercial catalog min price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setCommercialCatalogMinPriceSupplier(xsbData.get("commercialCatalogMinPriceSupplier"));
        this.setCountryOriginInference(xsbData.get("countryOriginInference"));
        this.setCountryOfOrigin(xsbData.get("countryOrigin"));
        try {
            val = xsbData.get("highPriceTarget");
            if (val != null && !val.isBlank()) this.setHighPriceTarget(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, High Price Target. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setInvalidReason(xsbData.get("invalidReason"));
        try {
            val = xsbData.get("catalogMedianPrice");
            if (val != null && !val.isBlank())
                this.setIsMarketResearchFound((Double.parseDouble(val)) > 0.0);
            else this.setIsMarketResearchFound(false);
        } catch (Exception e) {
            sb.append("Invalid data, High Price Target. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        try {
            val = xsbData.get("lowPriceTarget");
            if (val != null && !val.isBlank()) this.setLowPriceTarget(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Low Price Target. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setNsn(xsbData.get("nsn"));
        this.setStandardizedProductDescription(xsbData.get("standardizedProductDescription"));
        try {
            val = xsbData.get("tdrMedianPrice");
            if (val != null && !val.isBlank()) this.setTdrMedianPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Median Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        try {
            val = xsbData.get("tdrMinPrice");
            if (val != null && !val.isBlank()) this.setTdrMinPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Min Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        try {
            val = xsbData.get("tdrMaxPrice");
            if (val != null && !val.isBlank()) this.setTdrMaxPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Min Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        try {
            val = xsbData.get("vppIndicator");
            if (val != null && !val.isBlank()) this.setVppIndicator(Integer.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, VPP Indicator. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        this.setProhibitionCondition(xsbData.get("prohibitionCondition"));
        this.setProhibitionReason(xsbData.get("prohibitionReason"));
        this.setProhibitionComment(xsbData.get("prohibitionComment"));
        // End ACREPO-2432

        // Begin ACREPO-3215
        try {
            val = xsbData.get("annualDemandQuantity");
            if (val != null && !val.isBlank())
                this.setAnnualDemandQuantity(Integer.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Annual Demand Quantity. Must be a valid integer. Value encountered: ").append(val).append(ls);
        }

        try {
            val = xsbData.get("catalogAvgPrice");
            if (val != null && !val.isBlank()) this.setCatalogAvgPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Average Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }

        try {
            val = xsbData.get("catalogMaxPrice");
            if (val != null && !val.isBlank()) this.setCatalogMaxPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Max Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }

        try {
            val = xsbData.get("catalogPriceStandardDeviation");
            if (val != null && !val.isBlank())
                this.setCatalogPriceStandardDeviation(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Price Standard Deviation. Must be a valid number. Value encountered: ").append(val).append(ls);
        }

        try {
            val = xsbData.get("transactionMinPrice");
            if (val != null && !val.isBlank())
                this.setTransactionMinPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Minimum Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }

        try {
            val = xsbData.get("transactionAvgPrice");
            if (val != null && !val.isBlank())
                this.setTransactionAvgPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Average Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }

        try {
            val = xsbData.get("transactionMedianPrice");
            if (val != null && !val.isBlank())
                this.setTransactionMedianPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Median Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }

        try {
            val = xsbData.get("transactionMaxPrice");
            if (val != null && !val.isBlank())
                this.setTransactionMaxPrice(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Maximum Price. Must be a valid number. Value encountered: ").append(val).append(ls);
        }
        // End ACREPO-3215

        try {
            val = xsbData.get("isAuthorizedVendor");
            if (val != null && !val.isBlank())
                this.setIsAuthorizedVendor(Integer.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Authorized vendor. Must be a valid integer. Value encountered: ").append(val).append(ls);
        }
        try {
            val = xsbData.get("hits");
            if (val != null && !val.isBlank()) this.setHits(Integer.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, for hits. Must be a valid integer. Value encountered: ").append(val).append(ls);
        }
        try {
            val = xsbData.get("selfHits");
            if (val != null && !val.isBlank()) this.setSelfHits(Integer.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, for Self hits. Must be a valid integer. Value encountered: ").append(val).append(ls);
        }
        this.setRankCategory(xsbData.get("rankCategory"));
        this.setSalesLikelihood(xsbData.get("salesLikelihood"));
        try {
            val = xsbData.get("demandWeightedIndexScore");
            if (val != null && !val.isBlank())
                this.setDemandWeightedIndexScore(Double.valueOf(val));
        } catch (Exception e) {
            sb.append("Invalid data, Demand Weighted Index Score. Must be a valid number. Value encountered: ").append(val).append(ls);
        }

        //Calculated fields.
        calculatedFields(xsbData, taaCountryCodes, sb, ls);

        if (!sb.isEmpty()) throw new IllegalArgumentException(sb.toString());
    }

    private void calculatedFields(Map<String, String> xsbData, List<String> taaCountryCodes, StringBuilder sb, String ls) {
        String val = xsbData.get("finalPrice") ; // temporary value holder from the map passed in as the argument
        String tmpVal = null; // temporary value holder from the map passed in as the argument
        try {
            tmpVal = xsbData.get("catalogPriceStandardDeviation");
            this.setEnrichmentLowerBound(0.0);
            if (val != null && !val.isBlank() && tmpVal != null && !tmpVal.isBlank())
                this.setEnrichmentLowerBound(Double.parseDouble(val) - Double.parseDouble(tmpVal));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or Catalog price Standard Deviation. Must be a valid number. Value encountered: ").append(val).append(", ").append(tmpVal).append(ls);
        }
        try {
            tmpVal = xsbData.get("catalogPriceStandardDeviation");
            this.setEnrichmentUpperBound(0.0);
            if (val != null && !val.isBlank() && tmpVal != null && !tmpVal.isBlank())
                this.setEnrichmentUpperBound(Double.parseDouble(val) + Double.parseDouble(tmpVal));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or Catalog price Standard Deviation. Must be a valid number. Value encountered: ").append(val).append(", ").append(tmpVal).append(ls);
        }
        try {
            tmpVal = xsbData.get("highPriceTarget");
            this.setExceedsMarketThreshold(false);
            if (val != null && !val.isBlank() && tmpVal != null && !tmpVal.isBlank())
                this.setExceedsMarketThreshold(Double.parseDouble(val) > Double.parseDouble(tmpVal));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or high price target. Must be a valid number. Value encountered: ").append(val).append(", ").append(tmpVal).append(ls);
        }
        this.setIsTaaRisk(isTradeAgreementViolated(xsbData.get("countryOriginInference"), taaCountryCodes)); //ACREPO-2143
        this.setIsMiaRisk(isMiaMisrepresented(xsbData.get("countryOriginInference"), xsbData.get("countryOrigin")));

        try {
            tmpVal = xsbData.get("catalogMedianPrice");
            boolean isLowOutlier = false;
            if (val != null && !val.isBlank() && tmpVal != null && !tmpVal.isBlank()) {
                if (Double.parseDouble(tmpVal) > 0 && (((Double.parseDouble(val) / Double.parseDouble(tmpVal)) - 1) < (-0.5))) {
                    //true if ([catalogMedianPrice] > 0) && ((([finalPrice]/[catalogMedianPrice])-1)<(-0.5))
                    isLowOutlier = true;
                }
            }
            this.setIsLowOutlier(isLowOutlier);
        } catch (Exception e) {
            sb.append("Invalid data, for Catalog Median Price or Final Price. Must be a valid number. Value encountered: ").append(tmpVal).append(", ").append(val).append(ls);
        }
    }

    private Boolean isTradeAgreementViolated(String countryOriginInference, List<String> taaCompliantCountryCodes){
        if (countryOriginInference != null && !countryOriginInference.isEmpty())
            return !taaCompliantCountryCodes.contains(countryOriginInference);
        return Boolean.FALSE;
    }

    private Boolean isMiaMisrepresented(String countryOriginInference, String countryOfOrigin){
        if (countryOriginInference == null || countryOriginInference.isEmpty()) return Boolean.FALSE;
        else return countryOfOrigin != null && countryOfOrigin.equals("US") && !countryOriginInference.equals("US");
    }
}
