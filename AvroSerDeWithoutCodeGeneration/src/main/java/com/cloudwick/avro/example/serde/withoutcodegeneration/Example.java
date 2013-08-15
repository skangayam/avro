package com.cloudwick.avro.example.serde.withoutcodegeneration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class Example {

	public static void main(String[] args) throws IOException {

		String avroSchemaFilePath = "src/main/resources/schema/departments.avsc";
		String textFilePath = "src/main/resources/data/departments.txt";
		String avroFilePath = "src/main/resources/data/departments.avro";

		Schema deptSchema = Schema.parse(new File(avroSchemaFilePath));

		GenericRecord dept = new GenericData.Record(deptSchema);
		File avroFile = new File(avroFilePath);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				deptSchema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(deptSchema, avroFile);

		// Serializing
		BufferedReader br = new BufferedReader(new FileReader(textFilePath),
				100);
		String s = null;
		String[] arr = new String[3];
		try {
			while ((s = br.readLine()) != null) {
				arr = s.split(",");
				dept.put("id", new Integer(arr[0])); // we can explicitly
														// mention the column
				// names
				dept.put("name", arr[1]);
				dept.put(2, arr[2]); // we can also use the zero based column
										// index

				dataFileWriter.append(dept);

			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			dataFileWriter.close();
			br.close();
			System.out.println("Done");
		}

		// Deserializing

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				deptSchema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				avroFile, datumReader);
		while (dataFileReader.hasNext()) {

			dept = dataFileReader.next(dept);
			System.out.println(dept);

		}
	}
}
