package gov.gsa.acr.cataloganalysis.model;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class JwtTokenTest {
    @Test
    void testSettersAndGetters() {
        JwtToken jwtToken = new JwtToken("b");
        String newVal = "a";
        jwtToken.setJwtToken(newVal);
        assertEquals(jwtToken.getJwtToken(), newVal);
    }
}
