package fdu.daslab.executable.udf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/12/01 15:00
 */
public class TestGraphCaseFunc {
    // map1:移去id
    public List<String> mapDidFunc(List<String> record) {
        List<String> res = new ArrayList<>();
        res.add(record.get(1));
        res.add(record.get(2));
        res.add(record.get(3));
        res.add(record.get(4));
        res.add(record.get(5));

        return res;
    }

    // sort1：按照粉丝数的进行初步排序
    public int sortCountFunc(List<String> record1, List<String> record2) {

        return new Integer(record2.get(2)) - new Integer(record1.get(2));

    }

    // filter过滤调粉丝数小于1000的，count<1000
    public boolean filterFunc(List<String> record) {
        boolean flag = true;
        try {
            if (Integer.parseInt(record.get(2)) < 1000) {
                flag = false;
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }

        return flag;
    }

    // map2: 将用户url特征化（直接使用url的code替代）
    public List<String> mapCodeFunc(List<String> record) {
        List<String> res = new ArrayList<>();
        res.add(record.get(3));
        res.add(record.get(4));
        return res;
    }

    // map3: 为了方便排序（使用int，因此每个rank值乘以了1000）
    public List<String> mapMulFunc(List<String> record) {
        record.set(1, String.valueOf(Float.parseFloat(record.get(1)) * 1000));
        return record;
    }

    // sort：按照rank值进行排序
    public int sortFunc(List<String> record1, List<String> record2) {
        Float f1 = new Float(record1.get(1));
        Float f2 = new Float(record2.get(1));
        return f2.intValue() - f1.intValue();
    }
}
