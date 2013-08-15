package com.cloudwick.avro.example.mapreduce.groupbysum;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudwick.avro.example.mapreduce.common.Employee;

public class GroupBySumDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err
					.println("Usage: GroupBySumDriver <input path> <output path>");
			return -1;
		}

		JobConf conf = new JobConf(getConf(), GroupBySumDriver.class);
		conf.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		AvroJob.setMapperClass(conf, GroupBySumMapper.class);
		AvroJob.setReducerClass(conf, GroupBySumReducer.class);		
		
		AvroJob.setInputSchema(conf, Employee.SCHEMA$);
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), Schema.create(Type.INT)));		

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new GroupBySumDriver(), args);
		System.exit(exitCode);

	}

}
