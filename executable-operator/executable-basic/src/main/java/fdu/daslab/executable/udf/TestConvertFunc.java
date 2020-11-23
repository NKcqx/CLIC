package fdu.daslab.executable.udf;

import org.javatuples.Triplet;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/3 5:45 pm
 */
public class TestConvertFunc {

    // filter
    // 检查每个网站是否是完整的域名，将非法域名的网站剔除
    public boolean filterFunc(List<String> record) {
        // 只保留网址符合以下正则表达式的网站
        String pattern = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(record.get(1));
        return m.find();
    }

    // map
    public List<String> mapFunc(List<String> record) {
        String url = record.get(1); // 合法网址
        String[] urlSegments = url.split("/");
        String primaryDomainName = urlSegments[2]; // 一级域名（此处索引取2是因为https:后面的//）
        return Arrays.asList(primaryDomainName, "1");
    }

    // reduce的Key
    public String reduceKey(List<String> record) {
        return record.get(0);
    }

    // reduce的func
    public List<String> reduceFunc(List<String> record1, List<String> record2) {
        return Arrays.asList(record1.get(0),
                String.valueOf(new Integer(record1.get(1)) + new Integer(record2.get(1))));
    }

    // sort
    // 按照网站点击量从大到小排序
    public int sortFunc(List<String> record1, List<String> record2) {
        return new Integer(record2.get(1)) - new Integer(record1.get(1));
    }

    // rdd-to-table
    // 用户在这里提供schema信息
    public Map<String, Triplet<Class, Boolean, String>> toTableFunc() {
        Map<String, Triplet<Class, Boolean, String>> schemaMap =
                new LinkedHashMap<String, Triplet<Class, Boolean, String>>();
        schemaMap.put("url", new Triplet<>(String.class, true, null));
        schemaMap.put("clickTimes", new Triplet<>(String.class, true, null));
        return schemaMap;
    }
}
