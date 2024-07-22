package gov.gsa.acr.cataloganalysis.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JwtToken {
    private String token;

    public JwtToken(String jwtToken) {
        this.token = jwtToken;
    }

    public String getJwtToken() {
        return token;
    }

    public void setJwtToken(String jwtToken) {
        this.token = jwtToken;
    }
}
