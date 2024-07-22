package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.util.TokenService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TokenServiceTest {

    @Mock
    private WebClient.Builder builder;

    @Mock
    private WebClient webClient;

    @InjectMocks
    private TokenService tokenService;

    @Test
    void testTokenValid() {
        final var requestBodyUriSpec = mock(WebClient.RequestBodyUriSpec.class);
        when(webClient.post()).thenReturn(requestBodyUriSpec);
        final var requestBodySpec = mock(WebClient.RequestBodySpec.class);
        when(requestBodyUriSpec.uri(any(String.class))).thenReturn(requestBodySpec);
        final var requestHeadersSpec = mock(WebClient.RequestHeadersSpec.class);
        when(requestBodySpec.bodyValue(any())).thenReturn(requestHeadersSpec);
        var responseSpec = mock(WebClient.ResponseSpec.class);
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        final var validMono = Mono.just("valid");
        when(responseSpec.bodyToMono(any(Class.class))).thenReturn(validMono);

        when(builder.baseUrl(any())).thenReturn(builder);
        when(builder.defaultHeader(any(), any())).thenReturn(builder);
        when(builder.build()).thenReturn(webClient);

        tokenService.init();

        assertEquals(tokenService.validate("abc"), true);
    }

    @Test
    void testTokenNotValid() {
        assertEquals(tokenService.validate(null), false);
    }
}
