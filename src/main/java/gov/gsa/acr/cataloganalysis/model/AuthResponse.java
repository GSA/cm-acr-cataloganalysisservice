package gov.gsa.acr.cataloganalysis.model;

import lombok.Data;

@Data
public class AuthResponse {
    private String access_token;
    private String expires_in;
    private String refresh_token_expires_in;
    private String refresh_token;
    private String token_type;
}
