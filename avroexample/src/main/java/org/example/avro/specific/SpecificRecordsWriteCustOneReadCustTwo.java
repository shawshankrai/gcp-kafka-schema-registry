package org.example.avro.specific;

import com.example.CustomerV1;
import com.example.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordsWriteCustOneReadCustTwo {
    private static final String SRC_MAIN_RESOURCES_GENERATED = "src/main/resources/generated/v1/customer-specific.avro";

    public static void main(String[] args) {
        // Step 1: Create a specific record
        CustomerV1.Builder builder = CustomerV1.newBuilder();
        builder.setFirstName("FName");
        builder.setLastName("LName");
        builder.setAge(10);
        builder.setHeight(5.11f);
        builder.setWeight(80.0f);
        builder.setAutomatedEmail(false);

        CustomerV1 customer = builder.build();
        System.out.println(customer);

        // Step 2: Write V1 record in a file - Serialization
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        try (DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(SRC_MAIN_RESOURCES_GENERATED));
            dataFileWriter.append(customer);
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // Step 3: Read V1 record as V2 from a file - Deserialization
        final File file = new File(SRC_MAIN_RESOURCES_GENERATED);
        final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
        try (DataFileReader<CustomerV2> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                CustomerV2 customerRead = dataFileReader.next();
                System.out.println("Successfully read avro file");
                System.out.println(customerRead.toString());

                // Step 4: Interpret as specific record
                System.out.println("First name: " + customerRead.get("first_name"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
