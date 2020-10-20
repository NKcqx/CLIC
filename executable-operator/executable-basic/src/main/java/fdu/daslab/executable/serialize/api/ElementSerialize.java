package fdu.daslab.executable.serialize.api;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;

import static fdu.daslab.executable.serialize.schema.SchemaUtil.buildAvroElementSchema;


/**
 * Created by Nathan on 2020/10/7.
 */
public class ElementSerialize<T> {
    /**
     * This function serialize an element to an ByteArrayOutputStream
     *
     * @param element element to be serialized, supporting String,Integer,Long, Double, Float type
     * @return ByteArrayOutputStream type, containing serialized byte array stream(in memory)
     * @throws Exception
     */
    public ByteArrayOutputStream serializeToBuffer(T element) throws Exception {
        assert element != null : "matrix can not be null";
        assert checkType(element) : "element type should be float,int,long,double, self-define class";

        Class elementClazz = element.getClass();
        Schema elementSchema = new Schema.Parser().parse(buildAvroElementSchema(elementClazz).toString());

        DatumWriter<T> datumWriter = new GenericDatumWriter<>(elementSchema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter);

        ByteArrayOutputStream res = new ByteArrayOutputStream();
        dataFileWriter.create(elementSchema, res);
        dataFileWriter.append(element);

        dataFileWriter.close();

        return res;
    }

    /**
     * This function serialize an element to an file
     * @param element element to be serialized, supporting String,Integer,Long, Double, Float type
     * @param filePath serialize element to this path
     * @throws Exception
     */
    public void serializeToFile(T element, String filePath) throws Exception {
        assert element != null : "matrix can not be null";
        assert checkType(element) : "element type should be float,int,long,double, self-define class";

        Class elementClazz = element.getClass();
        Schema elementSchema = new Schema.Parser().parse(buildAvroElementSchema(elementClazz).toString());

        DatumWriter<T> datumWriter = new GenericDatumWriter<>(elementSchema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.create(elementSchema, new File(filePath));
        dataFileWriter.append(element);

        dataFileWriter.close();

    }

    private boolean checkType(Object t) {
        if ((!(t instanceof String)) && (!(t instanceof Integer)) && (!(t instanceof Long))
                && (!(t instanceof Float)) && (!(t instanceof Double))) {
            return false;
        }
        return true;
    }
}
