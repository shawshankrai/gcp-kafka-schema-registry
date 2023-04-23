package org.example.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExample {
    public static void main(String[] args) {
        // Step 0: Define schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"com.example\",\n" +
                "  \"name\": \"Customer\",\n" +
                "  \"doc\": \"Customer avro Schema\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of the customer\" },\n" +
                "    { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of the Customer\", \"default\": \"null\" },\n" +
                "    { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age of the Customer\" },\n" +
                "    { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height in cms\" },\n" +
                "    { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight in kgs\", \"default\": 0},\n" +
                "    { \"name\": \"automated_email\", \"type\": \"boolean\", \"doc\": \"true if user want promotional email\", \"default\": true } \n" +
                "  ]\n" +
                " }");

        // Step 1: Create a generic record
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        recordBuilder.set("first_name", "FName");
        recordBuilder.set("last_name", "LName");
        recordBuilder.set("age", 10);
        recordBuilder.set("height", 180f);
        recordBuilder.set("weight", 85.0f);
        recordBuilder.set("automated_email", true);

        GenericData.Record genericRecord = recordBuilder.build();
        System.out.println(genericRecord);

        // Step 2: Write that generic record in a file - Serialization
        // writing to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(genericRecord.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(genericRecord);
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
        // Step 3: Read the generic record from a file - Deserialization
        // Step 4: Interpret as generic record

    }
}
