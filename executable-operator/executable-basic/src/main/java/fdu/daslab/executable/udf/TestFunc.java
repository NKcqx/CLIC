package fdu.daslab.executable.udf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/16/21 4:17 PM
 */
public class TestFunc implements Serializable {

    public List<String> mapFunc(List<String> record) {
        // 先输出
        for (String s : record) System.out.print(s + ":");
        System.out.println();
        try {
            String url = record.get(1); // 合法网址
            String[] urlSegments = url.split("/");
            String primaryDomainName = urlSegments[2]; // 一级域名（此处索引取2是因为https:后面的//）
            return Arrays.asList("https://" + primaryDomainName, "1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Arrays.asList("dummy", "1");
    }

    public boolean filterFunc(List<String> record) {
        for (String s : record) System.out.print(s + ",");
        System.out.println();
        // 只保留网址符合以下正则表达式的网站
        try {
            String pattern = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(record.get(0));
            return m.find();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

}
