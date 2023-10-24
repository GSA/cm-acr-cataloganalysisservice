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
     * @param taaCountryCodes List of countries that do not violate trade agreement
     */
    public XsbDataJsonRecord(Map<String, String> x, List<String> taaCountryCodes) {
        // Refer:https://docs.google.com/spreadsheets/d/1YuZpJOBl9jkHgciPDsEkNmGiG5NBcuauSDU76lQvbEU/view#gid=173420408
        if (x == null) throw new NullPointerException("Cannot convert a NULL Map to XSB Data");
        String v = null; // temporary value holder from the map passed in as the argument
        String v1 = null; // temporary value holder from the map passed in as the argument
        String ls = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();

        this.setModificationNumber(x.get("modificationNumber"));
        this.setSolicitationNumber(x.get("solicitationNumber"));
        this.setDunsNumber(x.get("dunsNumber"));
        try {
            v = x.get("quantityPerUnit");
            if (v != null && !v.isBlank())
                this.setQuantityPerUnit(Integer.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Quantity Per Unit. Must be a valid number. Value encountered: ").append(v).append(ls);
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
            if (v != null && !v.isBlank())
                this.setFinalPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Final Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setFsc(x.get("fsc"));
        try {
            v = x.get("catalogPriceStandardDeviation");
            if (v != null && !v.isBlank())
                this.setCatalogPriceStandardDeviation(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Price Standard Deviation. Must be a valid number. Value encountered: ").append(v).append(ls);
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
            if (v != null && !v.isBlank())
                this.setCatalogMedianPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Median Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setCatalogMedianPriceSupplier(x.get("catalogMedianPriceSupplier"));
        try {
            v = x.get("catalogMedianPrice");
            if (v != null && !v.isBlank()) this.setCatalogMinPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Min Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setCatalogMinPriceSupplier(x.get("catalogMinPriceSupplier"));
        try {
            v = x.get("commercialCatalogMedianPrice");
            if (v != null && !v.isBlank()) this.setCommercialCatalogMedianPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Commercial catalog median price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setCommercialCatalogMedianPriceSupplier(x.get("commercialCatalogMedianPriceSupplier"));
        try {
            v = x.get("commercialCatalogMinPrice");
            if (v != null && !v.isBlank()) this.setCommercialCatalogMinPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Commercial catalog min price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setCommercialCatalogMinPriceSupplier(x.get("commercialCatalogMinPriceSupplier"));
        this.setCountryOriginInference(x.get("countryOriginInference"));
        this.setCountryOfOrigin(x.get("countryOrigin"));
        try {
            v = x.get("catalogMedianPrice");
            if (v != null && !v.isBlank()) this.setHighPriceTarget(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, High Price Target. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setInvalidReason(x.get("invalidReason"));
        try {
            v = x.get("catalogMedianPrice");
            if (v != null && !v.isBlank())
                this.setIsMarketResearchFound((Double.parseDouble(v)) > 0.0);
            else this.setIsMarketResearchFound(false);
        } catch (Exception e) {
            sb.append("Invalid data, High Price Target. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        try {
            v = x.get("lowPriceTarget");
            if (v != null && !v.isBlank()) this.setLowPriceTarget(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Low Price Target. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setNsn(x.get("nsn"));
        this.setStandardizedProductDescription(x.get("standardizedProductDescription"));
        try {
            v = x.get("tdrMedianPrice");
            if (v != null && !v.isBlank()) this.setTdrMedianPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Median Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        try {
            v = x.get("tdrMinPrice");
            if (v != null && !v.isBlank()) this.setTdrMinPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Min Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        try {
            v = x.get("tdrMaxPrice");
            if (v != null && !v.isBlank()) this.setTdrMaxPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Tdr Min Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        try {
            v = x.get("vppIndicator");
            if (v != null && !v.isBlank()) this.setVppIndicator(Integer.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, VPP Indicator. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        this.setProhibitionComment(x.get("prohibitionComment"));
        // End ACREPO-2432

        // Begin ACREPO-3215
        try {
            v = x.get("annualDemandQuantity");
            if (v != null && !v.isBlank())
                this.setAnnualDemandQuantity(Integer.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Annual Demand Quantity. Must be a valid integer. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("catalogAvgPrice");
            if (v != null && !v.isBlank()) this.setCatalogAvgPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Average Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("catalogMedianPrice");
            if (v != null && !v.isBlank())
                this.setCatalogMedianPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Median Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("catalogMaxPrice");
            if (v != null && !v.isBlank()) this.setCatalogMaxPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Max Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("catalogPriceStandardDeviation");
            if (v != null && !v.isBlank())
                this.setCatalogPriceStandardDeviation(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Catalog Price Standard Deviation. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("transactionMinPrice");
            if (v != null && !v.isBlank())
                this.setTransactionMinPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Minimum Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("transactionAvgPrice");
            if (v != null && !v.isBlank())
                this.setTransactionAvgPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Average Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("transactionMedianPrice");
            if (v != null && !v.isBlank())
                this.setTransactionMedianPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Median Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        try {
            v = x.get("transactionMaxPrice");
            if (v != null && !v.isBlank())
                this.setTransactionMaxPrice(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Transaction Maximum Price. Must be a valid number. Value encountered: ").append(v).append(ls);
        }
        // End ACREPO-3215

        try {
            v = x.get("isAuthorizedVendor");
            if (v != null && !v.isBlank())
                this.setIsAuthorizedVendor(Integer.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Authorized vendor. Must be a valid integer. Value encountered: ").append(v).append(ls);
        }
        try {
            v = x.get("hits");
            if (v != null && !v.isBlank()) this.setHits(Integer.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, for hits. Must be a valid integer. Value encountered: ").append(v).append(ls);
        }
        try {
            v = x.get("selfHits");
            if (v != null && !v.isBlank()) this.setSelfHits(Integer.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, for Self hits. Must be a valid integer. Value encountered: ").append(v).append(ls);
        }
        this.setRankCategory(x.get("rankCategory"));
        this.setSalesLikelihood(x.get("salesLikelihood"));
        try {
            v = x.get("demandWeightedIndexScore");
            if (v != null && !v.isBlank())
                this.setDemandWeightedIndexScore(Double.valueOf(v));
        } catch (Exception e) {
            sb.append("Invalid data, Demand Weighted Index Score. Must be a valid number. Value encountered: ").append(v).append(ls);
        }

        //Calculated fields.
        v = x.get("finalPrice");
        try {
            v1 = x.get("catalogPriceStandardDeviation");
            this.setEnrichmentLowerBound(0.0);
            if (v != null && !v.isBlank() && v1 != null && !v1.isBlank())
                this.setEnrichmentLowerBound(Double.parseDouble(v) - Double.parseDouble(v1));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or Catalog price Standard Deviation. Must be a valid number. Value encountered: ").append(v).append(", ").append(v1).append(ls);
        }
        try {
            v1 = x.get("catalogPriceStandardDeviation");
            this.setEnrichmentUpperBound(0.0);
            if (v != null && !v.isBlank() && v1 != null && !v1.isBlank())
                this.setEnrichmentUpperBound(Double.parseDouble(v) + Double.parseDouble(v1));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or Catalog price Standard Deviation. Must be a valid number. Value encountered: ").append(v).append(", ").append(v1).append(ls);
        }
        try {
            v1 = x.get("highPriceTarget");
            this.setExceedsMarketThreshold(false);
            if (v != null && !v.isBlank() && v1 != null && !v1.isBlank())
                this.setExceedsMarketThreshold(Double.parseDouble(v) > Double.parseDouble(v1));
        } catch (Exception e) {
            sb.append("Invalid data, for Final Price or high price target. Must be a valid number. Value encountered: ").append(v).append(", ").append(v1).append(ls);
        }
        this.setIsTaaRisk(isTradeAgreementViolated(x.get("countryOriginInference"), taaCountryCodes)); //ACREPO-2143
        this.setIsMiaRisk(isMiaMisrepresented(x.get("countryOriginInference"), x.get("countryOrigin")));

        try {
            v1 = x.get("catalogMedianPrice");
            boolean isLowOutlier = false;
            if (v != null && !v.isBlank() && v1 != null && !v1.isBlank()) {
                if (Double.parseDouble(v1) > 0 && (((Double.parseDouble(v) / Double.parseDouble(v1)) - 1) < (-0.5))) {
                    //true if ([catalogMedianPrice] > 0) && ((([finalPrice]/[catalogMedianPrice])-1)<(-0.5))
                    isLowOutlier = true;
                }
            }
            this.setIsLowOutlier(isLowOutlier);
        } catch (Exception e) {
            sb.append("Invalid data, for Catalog Median Price or Final Price. Must be a valid number. Value encountered: ").append(v1).append(", ").append(v).append(ls);
        }

        if (!sb.isEmpty()) throw new IllegalArgumentException(sb.toString());
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
