package com.ismartv.statistics.countDetailIn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
		String rundate = args[2];
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

		if (exitCode == 0) {
			calcPercentAndWriteLocalDir(new Path(intermediatePathForJob2,
					"part-r-00000"), FileSystem.get(conf1), rundate);
		}

		return exitCode;
	}

	private void calcPercentAndWriteLocalDir(Path hdfsFilePath,
			FileSystem hdfsFileSystem, String rundate) throws Exception {

		if (!hdfsFileSystem.exists(hdfsFilePath)) {
			throw new Exception("source hdfs file is not exists");
		}

		// 格式化double类型的输出
		NumberFormat numberFormat = NumberFormat.getInstance();
		numberFormat.setMaximumFractionDigits(2);

		String line = null;
		int[][] data = new int[12][1];
		String[] strs = null;
		BufferedReader bufferedReader = null;

		File localFile = new File("/home/deploy/sj/output/CountVideoDetailIn",
				rundate);
		if (!localFile.exists()) {
			localFile.createNewFile();
		}

		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
				localFile, false))) {

			bufferedReader = new BufferedReader(new InputStreamReader(
					hdfsFileSystem.open(hdfsFilePath)));

			while ((line = bufferedReader.readLine()) != null) {
				strs = line.split("\t");

				if (strs.length != 2) {
					return;
				}
				Double index = Double.parseDouble(strs[0]);
				int value = Integer.parseInt(strs[1]);
				data[index.intValue()][0] = value;
			}

			// 计算总数
			int sum = 0;
			for (int i = 0; i < 12; i++) {
				sum += data[i][0];
			}

			// 计算百分比 并输出
			for (int j = 0; j < 12; j++) {
				if (j == 0) {
					line = "0.5\t";
				} else {
					line = j + "\t";
				}
				line = line
						+ data[j][0]
						+ "\t"
						+ numberFormat
								.format((data[j][0] * 100) / (double) sum)
						+ "%";
				bufferedWriter.write(line);
				bufferedWriter.newLine();
			}
		} finally {
			if (bufferedReader != null) {
				bufferedReader.close();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 3) {
			System.out
					.println("Usage: CountVideoDetailIn <inputPath> <outputPath> <rundate>");
			System.exit(-1);
		}
		int exitCode = -1;
		CountVideoDetailIn countVideoDetailIn = new CountVideoDetailIn();
		exitCode = ToolRunner.run(countVideoDetailIn, args);
		System.exit(exitCode);
	}
}
