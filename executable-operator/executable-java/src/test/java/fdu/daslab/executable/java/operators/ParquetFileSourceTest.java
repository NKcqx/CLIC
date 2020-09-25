package fdu.daslab.executable.java.operators;

import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/25 16:23
 */
public class ParquetFileSourceTest {

    String filePath="C:\\Users\\huawei\\Downloads\\myusers.parquet";
    @Test
    public void  readParquetFileTest() throws Exception {
        Map<String,String> params = new HashMap<String, String>();
        params.put("inputPath", filePath);//parquet文件路径
        List<String> in = new LinkedList<String>();
        List<String> out = new LinkedList<String>();
        ParquetFileSource parquetFileSource=new ParquetFileSource("id",in,out, params);

        parquetFileSource.execute(null,null);

        //输出读入的结果
        Stream<List<String>> s = parquetFileSource.getOutputData("result");
        s.forEach(r -> {
            System.out.println(r.toString());
        });

        //打印schema信息
        System.out.println(parquetFileSource.getSchema("schema"));
        /*
        [bob0, blue, 2]
        [bob1, blue, 2]
        [bob2, blue, 2]
        [bob3, null, 2]
        message example.avro.User {
            required binary name (UTF8);
            optional binary favorite_color (UTF8);
             optional int32 favorite_number;
         }
         */
    }

}
