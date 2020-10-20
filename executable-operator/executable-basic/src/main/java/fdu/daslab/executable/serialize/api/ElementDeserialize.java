package fdu.daslab.executable.serialize.api;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.File;

import static fdu.daslab.executable.serialize.schema.SchemaUtil.buildAvroElementSchema;

/**
 * Created by Nathan on 2020/10/7.
 */
public class ElementDeserialize<T> {
    /**
     *
     * @param matrix
     * @param clazz
     * @return
     * @throws Exception
     */
    public T deserializeFromBuffer(ByteArrayOutputStream matrix, Class clazz) throws Exception {

        Schema matrixSchema = new Schema.Parser().parse(buildAvroElementSchema(clazz).toString());

        DatumReader<T> datumReader = new GenericDatumReader<>(matrixSchema);
        DataFileReader<T> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(
                matrix.toByteArray()), datumReader);
        T res = dataFileReader.next();
        // avro use Utf8 class instead of java.util.String class
        if (res instanceof Utf8) {
            return (T) res.toString();
        }
        return res;
    }

    public T deserializeFromFile(String filePath, Class clazz) throws Exception {
        Schema matrixSchema = new Schema.Parser().parse(buildAvroElementSchema(clazz).toString());
        DatumReader<T> datumReader = new GenericDatumReader<>(matrixSchema);
        DataFileReader<T> dataFileReader = new DataFileReader<>(new File(filePath), datumReader);
        T res = dataFileReader.next();
        return res;
    }

}
