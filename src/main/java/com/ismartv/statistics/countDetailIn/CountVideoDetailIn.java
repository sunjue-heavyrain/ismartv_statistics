package com.ismartv.statistics.countDetailIn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountVideoDetailIn extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String inputPathString = args[0];
		String outputPathString = args[1];
		// job1
		Configuration conf1 = getConf();
		conf1.set("mapred.compress.map.output", "true");

		Job job1 = new Job(conf1, "CountVideoDetailIn1");
		job1.setJarByClass(CountVideoDetailIn.class);
		job1.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job1, new Path(inputPathString));

		Path intermediatePathForJob1 = new Path(outputPathString, "stage1");
		TextOutputFormat.setOutputPath(job1, intermediatePathForJob1);

		job1.setMapperClass(CountVideoDetailInStage1.Mapper.class);
		job1.setPartitionerClass(CountVideoDetailInStage1.SnPartitionner.class);
		job1.setReducerClass(CountVideoDetailInStage1.Reducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);

		// job2
		Configuration conf2 = getConf();
		conf1.set("mapred.compress.map.output", "true");

		Job job2 = new Job(conf2, "CountVideoDetailIn1");
		job2.setJarByClass(CountVideoDetailIn.class);
		job2.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job2, intermediatePathForJob1);

		Path intermediatePathForJob2 = new Path(outputPathString, "stage2");
		TextOutputFormat.setOutputPath(job2, intermediatePathForJob2);

		job2.setMapperClass(CountVideoDetailInStage2.Mapper.class);
		job2.setReducerClass(CountVideoDetailInStage2.Reducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(IntWritable.class);

		int exitCode = -1;
		if (job1.waitForCompletion(true)) {
			if (job2.waitForCompletion(true)) {
				exitCode = 0;
			}
		}

		return exitCode;
	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 2) {
			System.out
					.println("Usage: CountVideoDetailIn <inputPath> <outputPath> ");
			System.exit(-1);
		}
		int exitCode = -1;
		CountVideoDetailIn countVideoDetailIn = new CountVideoDetailIn();
		exitCode = ToolRunner.run(countVideoDetailIn, args);
		System.exit(exitCode);
	}
}
