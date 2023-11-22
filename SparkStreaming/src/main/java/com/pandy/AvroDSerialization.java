package com.pandy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroDSerialization {

    public static void main(String[] args) throws IOException {
        // Define Avro schema
        String schemaString = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
        Schema schema = new Schema.Parser().parse(schemaString);

        // Create Avro record
        GenericRecord user = new GenericData.Record(schema);
        user.put("name", new Utf8("John Doe"));
        user.put("age", 25);

        // Serialize Avro record to byte array
        byte[] serializedData = serializeAvroRecord(user, schema);

        // Deserialize byte array back to Avro record
        GenericRecord deserializedUser = deserializeAvroRecord(serializedData, schema);

        // Print the deserialized Avro record
        System.out.println("Deserialized User: " + deserializedUser);
    }

    public static byte[] serializeAvroRecord(GenericRecord record, Schema schema) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        outputStream.close();
        return outputStream.toByteArray();
    }

    public static GenericRecord deserializeAvroRecord(byte[] data, Schema schema) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        return datumReader.read(null, decoder);
    }
}
