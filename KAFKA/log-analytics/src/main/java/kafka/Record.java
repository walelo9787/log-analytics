package kafka;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Record {

    public final String ip;
    public final String date;
    public final String request;
    public final String endpoint;
    public final int status;
    public final double byte_;
    public final String referres;
    public final String userAgent;
    public final double responseTime;

    public Record(final String recordValue) {
        String[] split = recordValue.split(",");
        this.ip = split[0];
        this.date = toDate(split[1]);
        this.request = split[2];
        this.endpoint = split[3];
        this.status = Integer.parseInt(split[4]);
        this.byte_ = Double.parseDouble(split[5]);
        this.referres = split[6];
        this.userAgent = split[7];
        this.responseTime = Double.parseDouble(split[8]);
    }

    private String toDate(final String unixDate) {
        
        Long longDate = Long.parseLong(unixDate);
        long timestampSec = longDate / 1000;
        Instant instant = Instant.ofEpochSecond(timestampSec);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return dateTime.format(formatter);
    }
}
