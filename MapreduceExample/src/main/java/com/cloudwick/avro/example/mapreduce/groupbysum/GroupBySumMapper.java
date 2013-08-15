package com.cloudwick.avro.example.mapreduce.groupbysum;

import java.io.IOException;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;

import com.cloudwick.avro.example.mapreduce.common.Employee;

public class GroupBySumMapper extends
		AvroMapper<Employee, Pair<Integer, Integer>> {

	public void map(Employee emp,
			AvroCollector<Pair<Integer, Integer>> collector, Reporter reporter)
			throws IOException {

		Pair<Integer, Integer> x = new Pair<Integer, Integer>(emp.getDeptid(),
				1);
		System.out.println(x.toString());

		collector.collect(new Pair<Integer, Integer>(emp.getDeptid(), 1));

	}

}
