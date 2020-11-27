import fdu.daslab.executable.graphchi.operators.PageRankOperator;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 跑测试时需要设置禁用断言assert，即不用-ea运行。
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/24 16:05
 */
public class PageRankTest {
    String filePath1= PageRankTest.class.getClassLoader().
            getResource("0.edges").getPath();//读取
    //需要在运行的时候忽略断点assert
    @Test
    public void testPageRank() throws Exception {
//        File file = new File(filePath1);
//        final FileInputStream inputStream = new FileInputStream(file);
//
//        Map<String,String> params = new HashMap<String, String>();
//        params.put("graphName","testGraph");
//        params.put("shardNum","1");
////        params.put("fileType","edgelist"); // "edgelist" / "adjacency"
//        params.put("iterNum","9");
//
//
//        List<String> in = new LinkedList<String>();
//        in.add("data");
//        List<String> out = new LinkedList<String>();
//        out.add("result");
//        PageRankOperator pg = new PageRankOperator("1",in,out, params);
//        pg.setInputData("data",inputStream);
//        pg.execute(null, null);
//        Stream<List<String>> res = pg.getOutputData("result");
//        res.forEach(r->{
//            r.forEach(System.out::println);
//        });

    }
}
