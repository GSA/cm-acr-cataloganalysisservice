package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.PpApiConfig;
import gov.gsa.acr.cataloganalysis.model.AuthRequest;
import gov.gsa.acr.cataloganalysis.model.AuthResponse;
import gov.gsa.acr.cataloganalysis.model.Stats;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.function.Predicate;

@Slf4j
@Service
public class XsbPpApiService {
    private WebClient webClient;
    private final PpApiConfig apiConfig;

    @Value("${xsb.ppapi.retry.maxattempts:6}")
    private Long maxAttempts;

    @Value("${xsb.ppapi.retry.minbackoff.seconds:300}")
    private Long minBackOffSeconds;

    @Value("${xsb.ppapi.retry.maxbackoff.seconds:600}")
    private Long maxBackOffSeconds;

    public XsbPpApiService(PpApiConfig apiConfig) {
        this.apiConfig = apiConfig;
    }

    /**
     * Gets a JWT token from the auth endpoint.
     * This method will be called before making any API requests.
     *
     * @return Mono<String> containing the JWT token
     */
    public Mono<String> getAuthToken() {
        return getWebClient().post()
                .uri(apiConfig.getAuthUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new AuthRequest(apiConfig.getUsername(), apiConfig.getPassword()))
                .retrieve()
                .bodyToMono(AuthResponse.class)
                .map(AuthResponse::getAccess_token)
                .retryWhen(createRetrySpec())
                .doOnError(error -> {
                    log.error("Failed to get auth token: {}", error.getMessage());
                });
    }

    /**
     * Fetches latest Stats from the API with a date filter.
     * This method uses WebClient to make reactive HTTP requests and filters results
     * based on the provided ACR feed date.
     *
     * @param acrFeedDate The ACR feed date to use as a filter (format: yyyy-MM-dd)
     * @return A Flux of Post objects representing the posts from the API
     */
    public Flux<Stats> getLatestXsbStats(String acrFeedDate) {
        if (acrFeedDate == null) throw new IllegalArgumentException("ACR Feed date cannot be null");
        return getAuthToken()
                .flatMapMany(token -> getWebClient().get()
                        .uri(uriBuilder -> uriBuilder
                                .path("/api/catalog/stats")
                                .queryParam("sort-by", "gsaFeedDate")
                                .queryParam("sort-order", "DESC")
                                .queryParam("filters", "{filters}")
                                .queryParam("start", "0")
                                .queryParam("limit", "20")
                                .build(String.format("[{\"column\":\"gsaFeedDate\",\"condition\":\"greaterThan\",\"conditionType\":\"Date\",\"value\":\"%s\"}]", acrFeedDate)))
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .retrieve()
                        .bodyToFlux(Stats.class))
                .retryWhen(createRetrySpec())
                .doOnError(error -> log.error("Error fetching stats: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Completed fetching stats"));
    }


    /**
     * Retrieves the latest post date from the API response.
     * This method calls getLatestXsbStats and finds the post with the most recent date.
     *
     * @param acrFeedDate The ACR feed date to use as a filter (format: yyyy-MM-dd)
     * @return A Mono containing the latest post date formatted as yyyy-MM-dd, or empty if no posts found
     */
    public Mono<String> getGsaFeedDate(String acrFeedDate) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        if (acrFeedDate == null || acrFeedDate.isEmpty()) throw new IllegalArgumentException("Invalid argument, acrFeedDate: " + acrFeedDate);

        return getLatestXsbStats(acrFeedDate)
                .reduce((stat1, stat2) -> {
                  if (stat1.getGsaFeedDate().compareTo(stat2.getGsaFeedDate()) > 0) return stat1;
                  else return stat2;})
                .mapNotNull(stat -> {
                    LocalDate localDate = stat.getGsaFeedDate().toInstant()
                            .atZone(java.time.ZoneId.systemDefault())
                            .toLocalDate();
                    return localDate.format(formatter);
                });

        /*return getLatestXsbStats(acrFeedDate)
                .collectList()
                .map(stats -> stats.stream()
                        .filter(stat -> stat.getGsaFeedDate() != null)
                        .max((p1, p2) -> p1.getGsaFeedDate().compareTo(p2.getGsaFeedDate()))
                        .map(stat -> {
                            LocalDate localDate = stat.getGsaFeedDate().toInstant()
                                    .atZone(java.time.ZoneId.systemDefault())
                                    .toLocalDate();
                            return localDate.format(formatter);
                        })
                        .orElse(null)
                );

         */
    }


    /**
     * Creates a retry specification for authentication token requests.
     * Retries on server errors (5xx) and connection errors, with 10-minute intervals for up to 6 attempts.
     *
     * @return RetryBackoffSpec configured for auth token retries
     */
    private RetryBackoffSpec createRetrySpec() {
        return Retry.backoff(maxAttempts, Duration.ofSeconds(minBackOffSeconds))
                .maxBackoff(Duration.ofSeconds(maxBackOffSeconds))
                .filter(isRetryableError())
                .doBeforeRetry(retrySignal -> {
                    log.warn("Retrying auth token request (attempt {}/6) due to: {}",
                            retrySignal.totalRetries() + 1,
                            retrySignal.failure().getMessage());
                })
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    log.error("Auth token request failed after {} attempts", retrySignal.totalRetries());
                    return retrySignal.failure();
                });
    }

    /**
     * Determines if an error should trigger a retry.
     * Retries on server errors (5xx HTTP status codes) and connection-related exceptions.
     *
     * @return Predicate that returns true for retryable errors
     */
    private Predicate<Throwable> isRetryableError() {
        return throwable -> {
            if (throwable instanceof WebClientResponseException webClientException) {
                int statusCode = webClientException.getStatusCode().value();
                boolean isServerError = statusCode >= 500 && statusCode < 600;
                log.debug("HTTP error {} - retryable: {}", statusCode, isServerError);
                return isServerError;
            }

            // Retry on connection-related errors
            boolean isConnectionError = throwable instanceof java.net.ConnectException ||
                                        throwable instanceof java.net.SocketTimeoutException ||
                                        throwable instanceof java.io.IOException ||
                                        throwable.getMessage() != null &&
                                        (throwable.getMessage().contains("Connection refused") ||
                                         throwable.getMessage().contains("timeout") ||
                                         throwable.getMessage().contains("connection"));

            log.debug("Connection error - retryable: {}", isConnectionError);
            return isConnectionError;
        };
    }

    private WebClient getWebClient(){
        if (this.webClient == null)
            this.webClient = WebClient.builder()
                .baseUrl(apiConfig.getHostport())
                .build();
        return this.webClient;
    }
}
