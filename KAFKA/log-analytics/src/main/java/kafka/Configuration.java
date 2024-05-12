package kafka;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Configuration {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class.getName());
    public final Map<String, String> map;

    public Configuration(final String pathToJson) {
        Map<String, String> dataMap = new HashMap<>();
        try (JsonReader reader = Json.createReader(new FileReader(pathToJson))) {
            JsonObject jsonObject = reader.readObject();

            jsonObject.entrySet().forEach(entry -> dataMap.put(entry.getKey(), entry.getValue().toString()));

        } catch (IOException e) {
            LOGGER.error("Error while reading json file under path {}. Exception caught: {}", 
            pathToJson, e.toString());
        }

        this.map = dataMap;
    }

    public String get(final String key) {
        String value;
        if (key == "userName") {
            value = this.map.getOrDefault("userName", "root");
        } else {
            value = this.map.get(key);
        }
        return value.replace("\"", "");
    }
}
