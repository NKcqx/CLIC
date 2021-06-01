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
 * @since 2020/10/12 14:54
 */

public class ParquetFileFromColumnSinkTest {

    String filePath1 = ParquetFileFromColumnSinkTest.class.getClassLoader().
            getResource("myusersColumn.parquet").getPath();//读取
    String filePath2 = ParquetFileFromColumnSinkTest.class.getClassLoader().
            getResource("").getPath() + "myusersColumn2.parquet";//写入

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
        ParquetFileFromColumnSink parquetFileFromColumnSink = new ParquetFileFromColumnSink("1", in, out, params);
        parquetFileFromColumnSink.setInputData("data", data);
        parquetFileFromColumnSink.setSchema(schemaStr);

        //执行
        parquetFileFromColumnSink.execute(null, null);

        //再读写入的文件，并打印
        Stream<List<String>> res = fileRead(filePath2);

        List<String> k = new LinkedList<String>();
        res.forEach(r -> {
            k.add(r.toString());
        });

        assertEquals(k.get(0), "[bob0, bob1, bob2, bob3, bob4]");
        assertEquals(k.get(1), "[blue, blue, blue, yellow, red]");
        assertEquals(k.get(2), "[2, 2, 2, 8, 6]");


    }

    public Stream<List<String>> fileRead(String path) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("inputPath", path);//parquet文件路径
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        ParquetFileToColumnSource parquetFileToColumnSource = new ParquetFileToColumnSource("id", in, out, params);

        parquetFileToColumnSource.execute(null, null);

        //获得读入的结果
        Stream<List<String>> s = parquetFileToColumnSource.getOutputData("result");
        return s;
    }

}
