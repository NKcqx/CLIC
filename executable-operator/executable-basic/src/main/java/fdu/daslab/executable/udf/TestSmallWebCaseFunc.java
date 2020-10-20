package fdu.daslab.executable.udf;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 唐志伟，刘丰艺
 * @version 1.0
 * @since 2020/7/6 14:05
 */
public class TestSmallWebCaseFunc {

    // 网站信息表webClick.csv（点击者id，网站url）

    // groupby
    // 将网站按一级域名分类并记录总点击量（目前这个算子功能用下面的mapFunc实现了）
    public List<String> groupByFunc(List<String> record) {
        return record;
    }

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
}
