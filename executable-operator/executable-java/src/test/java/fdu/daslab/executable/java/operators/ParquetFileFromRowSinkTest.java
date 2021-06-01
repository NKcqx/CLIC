package fdu.daslab.executable.java.operators;

import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/25 18:45
 */
public class ParquetFileFromRowSinkTest {

    String filePath1 = ParquetFileFromRowSinkTest.class.getClassLoader().
            getResource("myusers.parquet").getPath();//读取
    String filePath2 = ParquetFileFromRowSinkTest.class.getClassLoader().
            getResource("").getPath() + "myusers2.parquet";//写入
//    String filePath1="hdfs://ip:9000/myusers.parquet";   //hdfs测试
//    String filePath2="hdfs://ip:9000/myusers.parquet2";

    @Test
    public void writeParquetFileTest() throws Exception {
        //从已有文件中读取，获得Stream
        String schemaStr = "message example.avro.User {" +
                "required binary name (UTF8);" +
                "optional binary favorite_color (UTF8);" +
                "optional int32 favorite_number;" +
                "}";
        Stream<List<String>> data = fileRead(filePath1);
        Map<String, String> schema = new HashMap<String, String>();
        Map<String, String> params = new HashMap<String, String>();
        params.put("outputPath", filePath2);//parquet文件路径

        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        ParquetFileFromRowSink parquetFileFromRowSink = new ParquetFileFromRowSink("1", in, out, params);
        parquetFileFromRowSink.setInputData("data", data);
        parquetFileFromRowSink.setSchema(schemaStr);

        //执行
        parquetFileFromRowSink.execute(null, null);

        //再读写入的文件，并打印
        Stream<List<String>> res = fileRead(filePath2);

        List<String> k = new LinkedList<String>();
        res.forEach(r -> {
            k.add(r.toString());
        });

        assertEquals(k.get(0), "[bob0, blue, 2]");
        assertEquals(k.get(1), "[bob1, blue, 2]");
        assertEquals(k.get(2), "[bob2, blue, 2]");
        assertEquals(k.get(3), "[bob3, null, 2]");
        /*
           [bob0, blue, 2]
           [bob1, blue, 2]
           [bob2, blue, 2]
           [bob3, null, 2]
         */

    }

    public Stream<List<String>> fileRead(String path) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("inputPath", path);//parquet文件路径
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        ParquetFileToRowSource parquetFileToRowSource = new ParquetFileToRowSource("id", in, out, params);

        parquetFileToRowSource.execute(null, null);

        //获得读入的结果
        Stream<List<String>> s = parquetFileToRowSource.getOutputData("result");
        return s;
    }
}
