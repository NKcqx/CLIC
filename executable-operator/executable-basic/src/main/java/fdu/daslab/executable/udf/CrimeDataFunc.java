package fdu.daslab.executable.udf;

import java.util.*;




/**
 *
 * map -> filter(0 and 1) -> sort -> map -> reduceByKey
 *
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/17 15:08
 */
public class CrimeDataFunc {

    // 1.map1:将犯罪类型映射成int值
    public List<String> mapCateFunc(List<String> record) {
        Map<String, String> categoryDic = new HashMap<>();
        categoryDic.put("Theft and Handling", "1");
        categoryDic.put("Violence Against the Person", "2");
        categoryDic.put("Criminal Damage", "3");
        categoryDic.put("Drugs", "4");
        categoryDic.put("Burglary", "5");
        categoryDic.put("Robbery", "6");
        categoryDic.put("Other Notifiable Offences", "7");
        categoryDic.put("Fraud or Forgery", "8");
        categoryDic.put("Sexual Offences", "9");
        try {
            //如果满足categoryDic，将值改为映射值
            if (!categoryDic.get(record.get(2)).isEmpty()) {
                record.set(2, categoryDic.get(record.get(2)));
            } else {
                record.set(2, "0"); //否则设为0，避免以为string匹配问题出错
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return record;
    }

    // 2.filter删除犯罪值为0 and 1的行
    public boolean filterFunc(List<String> record) {
        boolean flag = true;
        try {
            if (record.get(4).equals("0") || record.get(4).equals("1") || record.size() != 7) {
                flag = false;
            }
        } catch (Exception e) {
            flag = false;
            //e.printStackTrace();
        }

        return flag;
    }

    // 4, map2: month映射并添加相应的季度
    public List<String> mapMonthFunc(List<String> record) {
        Map<String, String> seasonyDic = new HashMap<>();
        seasonyDic.put("1", "1");
        seasonyDic.put("2", "1");
        seasonyDic.put("3", "1");
        seasonyDic.put("4", "2");
        seasonyDic.put("5", "2");
        seasonyDic.put("6", "2");
        seasonyDic.put("7", "3");
        seasonyDic.put("8", "3");
        seasonyDic.put("9", "3");
        seasonyDic.put("10", "4");
        seasonyDic.put("11", "4");
        seasonyDic.put("12", "4");
        List<String> res = new ArrayList<>(record);
        res.add("0");

        try {
            //如果满足categoryDic，添加映射值
            if (!seasonyDic.get(record.get(6)).isEmpty()) {
                res.set(7, seasonyDic.get(record.get(6)));
            }
        } catch (Exception e) {
            res = new ArrayList<String>();
            res.add("E01001116");
            res.add("Croydon");
            res.add("Burglary");
            res.add("Burglary in Other Buildings");
            res.add("0");
            res.add("2016");
            res.add("11");
            res.add("4");
        }

        return res;
    }

    // 5, reduce的Key 季度
    public String reduceKey(List<String> record) {
        return record.get(7);
    }

    // reduce的func: 以季度为key，获取每个季度的犯罪量总值以及该季度犯罪数量最大的区
    public List<String> reduceFunc(List<String> record1, List<String> record2) {

        List<String> res = new ArrayList<>();
        if (Integer.parseInt(record1.get(4)) >= Integer.parseInt(record2.get(4))) {
            res.addAll(record1);
            res.set(4, String.valueOf(new Integer(record1.get(4)) + new Integer(record2.get(4))));

        } else {
            res.addAll(record2);
            res.set(4, String.valueOf(new Integer(record1.get(4)) + new Integer(record2.get(4))));
        }
        return res;


    }

    // 6, sort：按照犯罪数量的季度总和值进行排序
    public int sortFunc(List<String> record1, List<String> record2) {

        return new Integer(record2.get(4)) - new Integer(record1.get(4));

    }

}
