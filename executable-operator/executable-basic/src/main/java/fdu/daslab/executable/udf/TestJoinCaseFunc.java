package fdu.daslab.executable.udf;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 测试 Java Join Operator的udf
 */
public class TestJoinCaseFunc {

    // 网站公司表webCompany.csv（网站地址，网站中文名，公司id）
    // 公司信息表companyInfo.csv（公司id，公司名称，公司法人，公司领域）

    // filter
    // webCompany.csv中将“网站地址”非法的网站过滤掉
    public boolean filterWebCompanyFunc(List<String> record) {
        // 只保留网址符合以下正则表达式的网站
        String pattern = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(record.get(0));
        //System.out.println("左表过滤函数运行");
        return m.find();
    }
    // companyInfo.csv中将“公司领域”是“US”的过滤掉
    public boolean filterCompanyInfoFunc(List<String> record) {
        //System.out.println("右表过滤函数运行");
        return (!record.get(3).equals("US"));
    }

    // join
    // join这两个表，key为公司id，case仅测试所用请无视范式约束
    // 本case设定leftTable是webCompany.csv，rightTable是companyInfo.csv
    public String leftTableKey(List<String> record) {
        return record.get(2);
    }
    public String rightTableKey(List<String> record) {
        return record.get(0);
    }

    // 自定义join之后左表中要select的属性
    public List<String> leftTableFunc(List<String> record) {
        //System.out.println("左表新表函数运行");
        return Arrays.asList(record.get(0), record.get(1));
    }
    // 自定义join之后右表中要select的属性
    public List<String> rightTableFunc(List<String> record) {
        //System.out.println("右表新表函数运行");
        return Arrays.asList(record.get(1), record.get(2), record.get(3));
    }

}
