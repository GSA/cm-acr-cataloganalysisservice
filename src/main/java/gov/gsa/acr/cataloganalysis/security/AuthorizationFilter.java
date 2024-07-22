package gov.gsa.acr.cataloganalysis.security;

import gov.gsa.acr.cataloganalysis.util.TokenService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.regex.Pattern;

@Slf4j
@Component
public class AuthorizationFilter extends OncePerRequestFilter {
    @Value("${security.disabled:false}")
    private boolean securityDisabled;

    @Value("${secured.path.pattern:/api/}")
    private String securedPathPatternString;

    private final TokenService tokenService;
    private Pattern patternForSecuredPath;
    private static final String HEADER_PREFIX = "Bearer ";

    public AuthorizationFilter(TokenService tokenService) {
        this.tokenService = tokenService;
    }

    @PostConstruct
    public void init(){
        patternForSecuredPath = Pattern.compile(securedPathPatternString);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        if (tokenService.validate(getTokenFromRequest(request))) filterChain.doFilter(request, response);
        else response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) throws ServletException {
        String path = request.getRequestURI();
        return (Boolean.TRUE.equals(securityDisabled) || !patternForSecuredPath.matcher(path).find());
    }

    private String getTokenFromRequest(HttpServletRequest request){
        String bearerToken = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith(HEADER_PREFIX)) return bearerToken.substring(7);
        else return null;
    }
}
