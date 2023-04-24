package org.example.avro.specific;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordsExample {
    private static final String SRC_MAIN_RESOURCES_GENERATED = "src/main/resources/generated/customer-specific.avro";

    public static void main(String[] args) {
        // Step 1: Create a specific record
        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName("FName");
        builder.setLastName("LName");
        builder.setAge(10);
        builder.setHeight(5.11f);
        builder.setWeight(80.0f);
        builder.setAutomatedEmail(false);

        Customer customer = builder.build();
        System.out.println(customer);

        // Step 2: Write that generic record in a file - Serialization
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>();
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(SRC_MAIN_RESOURCES_GENERATED));
            dataFileWriter.append(customer);
            System.out.println("Written customer-generic.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // Step 3: Read the generic record from a file - Deserialization
        final File file = new File(SRC_MAIN_RESOURCES_GENERATED);
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>();
        try (DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                Customer customerRead = dataFileReader.next();
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
