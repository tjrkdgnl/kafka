package util;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

 public class AvroSerializers {
    private final Map<Schema, DatumWriter<Object>> datumWriterChache = new ConcurrentHashMap<>();
    private final Map<Schema, DatumReader<Object>> datumReaderChache = new ConcurrentHashMap<>();


    private DatumWriter<?> getDatuWriter(Schema schema){
        return new ReflectDatumWriter<>(schema);
    }

    private DatumReader<?> getDatumReader(Schema schema){
        return new ReflectDatumReader<>(schema);
    }

    public byte[] getSerialization(Object value, Schema schema) throws IOException {

        DatumWriter<Object> datumWriter = datumWriterChache.computeIfAbsent(schema,v ->(DatumWriter<Object>) getDatuWriter(schema));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Encoder encoder = EncoderFactory.get().directBinaryEncoder(out,null);

        datumWriter.write(value, encoder);
        encoder.flush();

        return out.toByteArray();
    }

    public Object getDeserialization(byte[] value,Schema schema) throws IOException {
        DatumReader<Object> reader =  datumReaderChache.computeIfAbsent(schema,v->(DatumReader<Object>)getDatumReader(schema));

        return reader.read(null, DecoderFactory.get().binaryDecoder(value,null));
    }



}
