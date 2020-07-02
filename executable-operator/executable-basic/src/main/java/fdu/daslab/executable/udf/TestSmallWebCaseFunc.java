package fdu.daslab.executable.udf;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 测试一个关于web的简单case的函数
 */
public class TestSmallWebCaseFunc {

    // 网站信息表webClick.csv（点击者id，网站url）
    // 黑名单表blackList.csv（网站一级域名，是否是黑名单）

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

    // join
    // 网站公司表webCompany.csv（网站一级域名，公司id），公司国家表companyCountry.csv（公司id，国家id）
    // join这两个表，将网站所属国家列出
    // key是公司id
    // join的key
    public String rightTableKey(List<String> record) {
        return record.get(0);
    }
    public String leftTableKey(List<String> record) {
        return record.get(1);
    }
    // join的func
    // 自定义join之后右表中要保留的属性
    public List<String> rightTableFunc(List<String> record) {
        return record;
    }
    // 自定义join之后左表中要保留的属性
    public List<String> leftTableFunc(List<String> record) {
        return record;
    }
}
