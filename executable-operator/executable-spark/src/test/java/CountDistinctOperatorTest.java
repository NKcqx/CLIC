import fdu.daslab.executable.spark.operators.CountByValueOperator;
import fdu.daslab.executable.spark.operators.CountOperator;
import fdu.daslab.executable.spark.operators.DistinctOperator;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/28 16:44
 */
public class CountDistinctOperatorTest {
    List<String> a = Arrays.asList("www.baidu.com,5".split(","));
    List<String> b = Arrays.asList("www.sina.com,1".split(","));
    List<String> c = Arrays.asList("www.google.com,10".split(","));
    List<String> d = Arrays.asList("www.cctv.com,8".split(","));
    List<String> e = Arrays.asList("www.cctv.com,8".split(","));
    List<List<String>> mydata = Arrays.asList(a,b,c,d,e);
    JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();
    JavaRDD<List<String>> listJavaRDD = javaSparkContext.parallelize(mydata);
    @Test
    public  void countOperatorTest(){
        Map<String,String> params = new HashMap<String, String>();
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        CountOperator countOperator = new CountOperator("1",in,out, params);
        countOperator.setInputData("data",listJavaRDD);
        countOperator.execute(null,null);
        long res = countOperator.getOutputData("result");
        Assert.assertEquals(res,(long)5);

    }
    @Test
    public  void countByValueOperatorTest(){
        Map<List<String>, Long> myRes = new HashMap<List<String>, Long>();
        myRes.put(a,(long)1);
        myRes.put(b,(long)1);
        myRes.put(c,(long)1);
        myRes.put(d,(long)2);

        Map<String,String> params = new HashMap<String, String>();
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        CountByValueOperator countByValue = new CountByValueOperator("1",in,out, params);
        countByValue.setInputData("data",listJavaRDD);
        countByValue.execute(null,null);
        Map<List<String>, Long> res= countByValue.getOutputData("result");

        Assert.assertEquals(res,myRes);

    }
    @Test
    public  void distinctOperatorTest(){
        List<List<String>> distinctData = Arrays.asList(a,c,b,d);

        Map<String,String> params = new HashMap<String, String>();
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        DistinctOperator distinctOperator = new DistinctOperator("1",in,out, params);
        distinctOperator.setInputData("data",listJavaRDD);
        distinctOperator.execute(null,null);
        JavaRDD<List<String>> res = distinctOperator.getOutputData("result");

        Assert.assertEquals(res.collect(),distinctData);

    }

}
