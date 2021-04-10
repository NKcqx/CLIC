package fdu.daslab.executable.udf;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author 李姜辛
 * @version 1.0
 * @since 2021/4/6 17:02
 */
public class TestTimeFormatter {
    public static void main(String[] args) {
        String pattern = "yyyy-MM-dd HH:mm:ss 'UTC'";
        String timestampAsString = "2019-12-01 00:00:01 UTC";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(timestampAsString));
        Timestamp timestamp = Timestamp.valueOf(localDateTime);
        System.out.println(timestamp.getTime());
        System.out.println(timestamp.toString());
    }
}
