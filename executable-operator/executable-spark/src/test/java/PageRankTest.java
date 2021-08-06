import fdu.daslab.executable.spark.operators.FileSink;
import fdu.daslab.executable.spark.operators.FileSource;
import fdu.daslab.executable.spark.operators.PageRankOperator;
import fdu.daslab.executable.spark.operators.RddToGraphOperator;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/26 15:18
 */
public class PageRankTest {
    private JavaSparkContext javaSparkContext;
    String filePath1 = PageRankTest.class.getClassLoader().
            getResource("0.edges").getPath();//读取
    String filePath2 = PageRankTest.class.getClassLoader().
            getResource("").getPath() + "0.csv";//写入

    @Before
    public void before() {
        SparkInitUtil.setSparkContext(new SparkConf().setMaster("local[*]").setAppName("PageRankTest"));
        javaSparkContext = SparkInitUtil.getDefaultSparkContext();
    }

    /**
     * 测试RddToGraphOperator以及PageRankOperator
     *
     * @throws Exception
     */
    @Test
    public void pageRankTest() throws Exception {
//        List<String> a = Arrays.asList("1", "2");
//        List<String> b = Arrays.asList("1", "3");
//        List<String> c = Arrays.asList("1", "4");
//        List<String> d = Arrays.asList("2", "3");
//        List<String> e = Arrays.asList("3", "4");
//        List<List<String>> mydata = Arrays.asList(a, b, c, d, e);
//        SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
//        JavaRDD<List<String>> listJavaRDD = javaSparkContext.parallelize(mydata);
        Map<String, String> params0 = new HashMap<String, String>();
        params0.put("inputPath", filePath1);
        params0.put("partitionNum", "1");
        params0.put("separator", " ");

        List<String> in0 = new LinkedList<String>();
        in0.add("data");
        List<String> out0 = new LinkedList<String>();
        out0.add("result");
        FileSource fileSource = new FileSource("1", in0, out0, params0);
        fileSource.execute(null, null);

        JavaRDD<List<String>> listJavaRDD = fileSource.getOutputData("result");

        Map<String, String> params = new HashMap<String, String>();
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        RddToGraphOperator rg = new RddToGraphOperator("1", in, out, params);
        rg.setInputData("data", listJavaRDD);
        rg.execute(null, null);

        Graph<String, String> graph = rg.getOutputData("result");

        Map<String, String> params1 = new HashMap<String, String>();
        params1.put("iterNum", "9");
        List<String> in1 = new LinkedList<String>();
        in.add("data");
        List<String> out1 = new LinkedList<String>();
        out.add("result");
        PageRankOperator pr = new PageRankOperator("1", in1, out1, params1);
        pr.setInputData("data", graph);
        pr.execute(null, null);

//        JavaRDD<List<String>> respr = pr.getOutputData("result");
//
//        Map<String, String> params2 = new HashMap<String, String>();
//        params2.put("isCombined", "true");
//        params2.put("outputPath", filePath2);
//        params2.put("separator", ",");
//
//        List<String> in2 = new LinkedList<String>();
//        in.add("data");
//        List<String> out2= new LinkedList<String>();
//        out.add("result");
//        FileSink fileSink = new FileSink("1", in2, out2, params2);
//        fileSink.setInputData("data", respr);
//        fileSink.execute(null, null);
//        List<List<String>> res = pr.getOutputData("result").collect();
//
//        res.forEach(System.out::println);
    }


    @After
    public void after() {
        SparkInitUtil.getDefaultSparkContext().close();
    }
}
