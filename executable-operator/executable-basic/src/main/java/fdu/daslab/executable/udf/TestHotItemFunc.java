package fdu.daslab.executable.udf;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * flink 热门商品topN的udf函数
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/30 15:40
 */
public class TestHotItemFunc {

    // 为数据分配时间戳
    public long assignTimestampFunc(List<String> record){
        String pattern = "yyyy-MM-dd HH:mm:ss 'UTC'";
        String timestampAsString = record.get(0);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(timestampAsString));
        Timestamp timestamp = Timestamp.valueOf(localDateTime);
        return timestamp.getTime();
    }

    // filter筛选出用户行为为"pv"的行为记录
    public boolean filterFunc(List<String> record) {
        boolean flag = false;
        try {
//            String type = record.get(1);
//            System.out.println(type);
            if ("view".equals(record.get(1))) {
                flag = true;
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }

        return flag;
    }

    // map2: 将用户行为映射成int值 (每条数据只代表一次点击)
    public List<String> mapCateFunc(List<String> record) {
        try {
            record.set(1, "1");
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return record;
    }

    // reduce的Key 商品ID
    public String reduceKey(List<String> record) {
        return record.get(2);
    }

    // reduce的func: 以商品ID为key，聚合每个商品ID的点击次数
    // QUESTION: reduce的两条List数据长度可以不同吗？
    public List<String> reduceFunc(List<String> record1, List<String> record2) {
        List<String> res = new ArrayList<>(record1);
        res.set(1, String.valueOf(new Integer(record1.get(1)) + new Integer(record2.get(1))));
        return res;
    }

    // reduce的winFunc: 对每一个滑动窗口，输出不同商品ID的点击数和窗口末端
    public void windowFunc(String key, TimeWindow window, Iterable<List<String>> input, Collector<List<String>> out){
        long windowEnd = window.getEnd();
        String count = input.iterator().next().get(1);
        List<String> res = Arrays.asList(key, String.valueOf(windowEnd), count);
        out.collect(res);
    }

    // TopN区别每个商品的ID
    public String topNID(List<String> record) {
        return record.get(0);
    }

    // TopN的WindowEnd
    public String topNWindow(List<String> record) {
        return record.get(1);
    }

    // TopN的Key
    public String topNKey(List<String> record) {
        return record.get(2);
    }

}
