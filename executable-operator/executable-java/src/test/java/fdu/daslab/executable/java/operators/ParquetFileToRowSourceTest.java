package fdu.daslab.executable.java.operators;



import fdu.daslab.executable.basic.model.OperatorBase;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import static org.junit.Assert.assertEquals;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/25 16:23
 */
public class ParquetFileToRowSourceTest {

    String filePath=ParquetFileToRowSourceTest.class.getClassLoader().
            getResource("myusers.parquet").toString();
//    String filePath="hdfs://ip:9000/myusers.parquet"; //hdfs测试
    @Test
    public void  readParquetFileTest() throws Exception {

        String schemaStr ="message example.avro.User {"+
                "required binary name (UTF8);"+
                "optional binary favorite_color (UTF8);"+
                "optional int32 favorite_number;"+
                "}";

        MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

        Map<String,String> params = new HashMap<String, String>();
        params.put("inputPath", filePath);//parquet文件路径
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");

        ParquetFileToRowSource parquetFileToRowSource=new ParquetFileToRowSource("1",in,out, params);

        parquetFileToRowSource.execute(null,null);

        //输出读入的结果
        Stream<List<String>> s = parquetFileToRowSource.getOutputData("result");
        List<String> k = new LinkedList<String>();
        s.forEach(r -> {
            k.add(r.toString());
        });

        assertEquals(k.get(0), "[bob0, blue, 2]");
        assertEquals(k.get(1), "[bob1, blue, 2]");
        assertEquals(k.get(2), "[bob2, blue, 2]");
        assertEquals(k.get(3), "[bob3, null, 2]");

        assertEquals(parquetFileToRowSource.getSchema(), schema.toString());
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
