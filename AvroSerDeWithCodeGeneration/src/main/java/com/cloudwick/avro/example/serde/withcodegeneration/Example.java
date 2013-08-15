package com.cloudwick.avro.example.serde.withcodegeneration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class Example {

	public static void main(String[] args) throws IOException {		
		String textFilePath = "src/main/resources/data/employees.txt";
		String avroFilePath = "src/main/resources/data/employees.avro";

		BufferedReader br = new BufferedReader(new FileReader(textFilePath),100);

		System.out.println("Reading the text file from " + textFilePath);
		System.out.println();

		String s = null;
		Employee emp = new Employee();
		String[] arr = new String[8];

		File avroFile = new File(avroFilePath);
		DatumWriter<Employee> empDatumWriter = new SpecificDatumWriter<Employee>(
				Employee.class);
		DataFileWriter<Employee> dataFileWriter = new DataFileWriter<Employee>(
				empDatumWriter);
		dataFileWriter.create(emp.getSchema(), avroFile);
		try {

			while ((s = br.readLine()) != null) {
				arr = s.split(",");

				emp.setId(new Integer(arr[0]));
				emp.setName(arr[1]);
				emp.setDesignation(arr[2]);
				if (!arr[3].isEmpty()) {
					emp.setMgrid(new Integer(arr[3]));
				}

				emp.setHiredate(arr[4]);
				emp.setSalary(new Double(arr[5]));
				if (!arr[6].isEmpty()) {
					emp.setCommission(new Double(arr[6]));
				}

				emp.setDeptid(new Integer(arr[7]));

				dataFileWriter.append(emp);

			}

		} catch (Exception e) { 
			dataFileWriter.close();
			br.close();
		}

		finally {
			dataFileWriter.close();

			br.close();
		}

		System.out.println("Avro file created at " + avroFilePath);
		System.out.println();
		System.out.println("Reading data from " + avroFilePath);
		System.out.println();
		DatumReader<Employee> empDatumReader = new SpecificDatumReader<Employee>(
				Employee.class);
		DataFileReader<Employee> dataFileReader = new DataFileReader<Employee>(
				avroFile, empDatumReader);

		while (dataFileReader.hasNext()) {
			emp = dataFileReader.next(emp);			
			System.out.println(emp);
		}

		dataFileReader.close();

	}
}
