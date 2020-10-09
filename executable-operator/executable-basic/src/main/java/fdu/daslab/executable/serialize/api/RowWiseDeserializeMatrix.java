package fdu.daslab.executable.serialize.api;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;

import static fdu.daslab.executable.serialize.schema.SchemaUtil.buildAvroMatrixSchema;


/**
 * Created by Nathan on 2020/9/24.
 */
public class RowWiseDeserializeMatrix<T extends Number> {

    public ArrayList<ArrayList<T>> deserializeFromBuffer(ByteArrayOutputStream matrix, Class clazz) throws Exception {

        Schema matrixSchema = new Schema.Parser().parse(buildAvroMatrixSchema(clazz).toString());

        DatumReader<ArrayList<T>> datumReader = new GenericDatumReader<>(matrixSchema);
        DataFileReader<ArrayList<T>> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(
                matrix.toByteArray()), datumReader);
        ArrayList<ArrayList<T>> res = new ArrayList<>();
        while (dataFileReader.hasNext()) {
            res.add(dataFileReader.next());
        }
        return res;
    }

    public ArrayList<ArrayList<T>> deserializeFromFile(String filePath, Class clazz) throws Exception {
        Schema matrixSchema = new Schema.Parser().parse(buildAvroMatrixSchema(clazz).toString());
        DatumReader<ArrayList<T>> datumReader = new GenericDatumReader<>(matrixSchema);
        DataFileReader<ArrayList<T>> dataFileReader = new DataFileReader<>(new File(filePath), datumReader);
        ArrayList<ArrayList<T>> res = new ArrayList<>();
        while (dataFileReader.hasNext()) {
            res.add(dataFileReader.next());
        }
        return res;
    }
}