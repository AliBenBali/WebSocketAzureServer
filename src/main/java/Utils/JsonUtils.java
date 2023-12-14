package Utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class JsonUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String SYSTEM = "SYSTEM";
    private static final String SATURN = "saturn";

    private JsonUtils() {
    }

    public static String convertToJson(Map<String, Object> data) {
        try {
            return OBJECT_MAPPER.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return "";
    }

    public static Map<String, Object> convertToMap(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return Collections.emptyMap();
    }

    public static String convertToJson(Object o) {
        try {
            String className = o.getClass().getName().replace(SATURN, SYSTEM);
            String data = OBJECT_MAPPER.writeValueAsString(o);
            return className + ";" + data;
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return "";
    }

    public static Optional<Object> convertToObject(String json) {
        try {
            int divider = json.indexOf(";");
            String className = json.substring(0, divider).replace(SYSTEM, SATURN);
            Class<?> clazz = Class.forName(className);
            String data = json.substring(divider + 1);
            return Optional.of(OBJECT_MAPPER.readValue(data, clazz));
        } catch (JsonProcessingException | ClassNotFoundException e) {
            LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return Optional.empty();
    }
}
