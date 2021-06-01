package fdu.daslab.executable.java.operators;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
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
 * @since 2020/10/09 19:07
 */
public class ParquetFileToColumnSourceTest {
    String filePath = ParquetFileToColumnSourceTest.class.getClassLoader().
            getResource("myusersColumn.parquet").toString();

    @Test
    public void readParquetFileTest() throws Exception {

        String schemaStr = "message example.avro.User {" +
                "required binary name (UTF8);" +
                "optional binary favorite_color (UTF8);" +
                "optional int32 favorite_number;" +
                "}";

        MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

        Map<String, String> params = new HashMap<String, String>();
        params.put("inputPath", filePath);//parquet文件路径
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        ParquetFileToColumnSource parquetFileToColumnSource = new ParquetFileToColumnSource("1", in, out, params);

        parquetFileToColumnSource.execute(null, null);
        Stream<List<String>> s = parquetFileToColumnSource.getOutputData("result");
        List<String> k = new LinkedList<String>();
        s.forEach(r -> {
            k.add(r.toString());
        });

        assertEquals(k.get(0), "[bob0, bob1, bob2, bob3, bob4]");
        assertEquals(k.get(1), "[blue, blue, blue, yellow, red]");
        assertEquals(k.get(2), "[2, 2, 2, 8, 6]");

        assertEquals(parquetFileToColumnSource.getSchema(), schema.toString());

    }

}
