package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.model.JwtToken;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;

@Slf4j
@Service
public class TokenService {
    @Value("${authservice.url}")
    private String authServiceURL;

    private final WebClient.Builder builder;
    private WebClient webClient;
    private static final String VALID = "valid";

    @Autowired
    public TokenService(WebClient.Builder builder) {
        this.builder = builder;
    }

    @PostConstruct
    public void init() {
        this.webClient = this.builder
                .baseUrl(authServiceURL)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.TEXT_PLAIN_VALUE)
                .build();
    }

    public boolean validate(String token){
        if (StringUtils.hasText(token)) {
            return this.webClient
                    .post()
                    .uri("/validation")
                    .bodyValue(new JwtToken(token))
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(VALID::equals)
                    .onErrorResume(e -> Mono.just(false))
                    .block();
        }
        return false;
    }
}
