package kafka;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class.getName());
    public static void main(String[] args) {

        Configuration configuration = new Configuration("configuration/config.json");
        
        Consumer consumer = new Consumer(
                configuration.get("topic"), 
                configuration.get("server"), 
                configuration.get("keyDeserilizer"), 
                configuration.get("valueDeserilizer"), 
                configuration.get("groupId"),
                configuration.get("offsetReset")
                );
    
        CompletableFuture<Set<String>> future = CompletableFuture.supplyAsync(() -> consumer.consume());

        Set<String> kafkaRecords = Collections.emptySet();
        try {
            kafkaRecords = future.get(); 
        } catch (Exception e) {
            LOGGER.error("Kafka poll process raised an exception :", e);
        }
        
        Set<Record> records = kafkaRecords.stream()
            .map(kr -> new Record(kr))
            .collect(Collectors.toSet());
        
        if (!records.isEmpty()) {
            List<String> columns = List.of(
                "date", "ip_address", "request", "endpoint", "status", 
                "referrers", "byte", "user_agent", "response_time"
                );
    
            String dbName = configuration.get("databaseName");
            String tableName = configuration.get("table");
            String userName = configuration.get("userName");

            Jdbc jdbc = new Jdbc(records, userName, dbName, tableName, columns);
            jdbc.pushToDb();
        }
    }


}
