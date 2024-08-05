package gov.gsa.acr.cataloganalysis.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.r2dbc.postgresql.codec.Json;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
@SpringBootTest
@Slf4j
@TestPropertySource(locations="classpath:application-test.properties")
class JsonSerializerDeserializerTest {

    @Test
    void testSerialize() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module =
                new SimpleModule("CustomJsonSerializer", new Version(1, 0, 0, null, null, null));
        module.addSerializer(Json.class, new JsonSerializerDeserializer.Serializer());
        mapper.registerModule(module);
        Json json = Json.of("{\"vendorName\":\"AMERICAN SIGNAL COMPANY\"}");
        String jsonStr = mapper.writeValueAsString(json);

        assertEquals("{\"vendorName\":\"AMERICAN SIGNAL COMPANY\"}", jsonStr);

    }

    @Test
    void testDeserialize() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module =
                new SimpleModule("CustomJsonSerializer", new Version(1, 0, 0, null, null, null));
        module.addDeserializer(Json.class, new JsonSerializerDeserializer.Deserializer());
        mapper.registerModule(module);

        Json json = mapper.readValue("{\"vendorName\":\"AMERICAN SIGNAL COMPANY\"}", Json.class);

        assertEquals("{\"vendorName\":\"AMERICAN SIGNAL COMPANY\"}", json.asString());

    }
}