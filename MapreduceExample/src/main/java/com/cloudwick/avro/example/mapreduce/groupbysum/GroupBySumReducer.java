package com.cloudwick.avro.example.mapreduce.groupbysum;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;

public class GroupBySumReducer extends
		AvroReducer<Integer, Integer, Pair<Integer, Integer>> {

	@Override
	public void reduce(Integer deptId, Iterable<Integer> values,
			AvroCollector<Pair<Integer, Integer>> collector, Reporter reporter)
			throws IOException {
		int count = 0;

		for (Integer i : values) {
			count += i;
		}

		collector.collect(new Pair<Integer, Integer>(deptId, count));
	}

}
