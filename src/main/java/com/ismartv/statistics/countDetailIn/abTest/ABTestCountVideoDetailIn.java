package com.ismartv.statistics.countDetailIn.abTest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.output.FileWriterWithEncoding;
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

public class ABTestCountVideoDetailIn extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String inputPathString = args[0];
		String outputPathString = args[1];
		String rundate = args[2];
		// job1
		Configuration conf1 = getConf();
		conf1.set("mapred.compress.map.output", "true");

		Job job1 = new Job(conf1, "ABTestCountVideoDetailIn1");
		job1.setJarByClass(ABTestCountVideoDetailIn.class);
		job1.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job1, new Path(inputPathString));

		Path intermediatePathForJob1 = new Path(outputPathString, "stage1");
		TextOutputFormat.setOutputPath(job1, intermediatePathForJob1);

		job1.setMapperClass(ABTestCountVideoDetailInStage1.Mapper.class);
		job1.setPartitionerClass(ABTestCountVideoDetailInStage1.SnPartitionner.class);
		job1.setReducerClass(ABTestCountVideoDetailInStage1.Reducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);

		// job2
		Configuration conf2 = getConf();
		conf1.set("mapred.compress.map.output", "true");

		Job job2 = new Job(conf2, "ABTestCountVideoDetailIn2");
		job2.setJarByClass(ABTestCountVideoDetailIn.class);
		job2.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job2, intermediatePathForJob1);

		Path intermediatePathForJob2 = new Path(outputPathString, "stage2");
		TextOutputFormat.setOutputPath(job2, intermediatePathForJob2);

		job2.setMapperClass(ABTestCountVideoDetailInStage2.Mapper.class);
		job2.setReducerClass(ABTestCountVideoDetailInStage2.Reducer.class);
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

		FileSystem fileSystem = FileSystem.get(conf1);
		fileSystem.deleteOnExit(intermediatePathForJob1);
		fileSystem.deleteOnExit(intermediatePathForJob2);

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
		List<CountVideoOutput> dataA = new ArrayList<CountVideoOutput>();
		List<CountVideoOutput> dataB = new ArrayList<CountVideoOutput>();
		String[] strs = null;
		BufferedReader bufferedReader = null;

		File localFile = new File(
				"/home/deploy/sj/output/ABTestCountVideoDetailIn", rundate);
		if (!localFile.exists()) {
			localFile.createNewFile();
		}

		try (BufferedWriter bufferedWriter = new BufferedWriter(
				new FileWriterWithEncoding(localFile, "UTF-8", false))) {

			bufferedReader = new BufferedReader(new InputStreamReader(
					hdfsFileSystem.open(hdfsFilePath)));

			String mod = null;
			int time;
			String[] othorStrs = null;
			int count;
			while ((line = bufferedReader.readLine()) != null) {
				strs = line.split("\t");

				if (strs.length != 2) {
					return;
				}
				othorStrs = strs[0].split(":");
				mod = othorStrs[0];
				time = Integer.parseInt(othorStrs[1]);
				count = Integer.parseInt(strs[1]);
				if (mod.equals("A")) {
					dataA.add(new CountVideoOutput(time, count));
				} else {
					dataB.add(new CountVideoOutput(time, count));
				}

			}

			// 排序
			Collections.sort(dataA, new Comparator<CountVideoOutput>() {
				@Override
				public int compare(CountVideoOutput o1, CountVideoOutput o2) {
					return o1.getTime().compareTo(o2.getTime());
				}
			});

			Collections.sort(dataB, new Comparator<CountVideoOutput>() {
				@Override
				public int compare(CountVideoOutput o1, CountVideoOutput o2) {
					return o1.getTime().compareTo(o2.getTime());
				}
			});

			// 计算总数
			int sumA = 0;
			int sumB = 0;
			for (int i = 0; i < dataA.size(); i++) {
				sumA += dataA.get(i).getCount();
				sumB += dataB.get(i).getCount();
			}

			// 计算百分比 并输出
			line = " \t推荐排序 \t默认排序";
			bufferedWriter.write(line);
			bufferedWriter.newLine();
			for (int j = 0; j < dataA.size(); j++) {

				line = dataA.get(j).getTime()
						+ "\t"
						+ dataA.get(j).getCount()
						+ "\t"
						+ numberFormat.format((dataA.get(j).getCount() * 100)
								/ (double) sumA)
						+ "%\t"
						+ dataB.get(j).getCount()
						+ "\t"
						+ numberFormat.format((dataB.get(j).getCount() * 100)
								/ (double) sumB) + "%";
				bufferedWriter.write(line);
				bufferedWriter.newLine();
			}

		} finally {
			if (bufferedReader != null) {
				bufferedReader.close();
			}
		}
	}

	// 输出本地时使用的对象
	static class CountVideoOutput {
		private int time;
		private int count;

		public CountVideoOutput() {
		}

		public CountVideoOutput(int time, int count) {
			this.time = time;
			this.count = count;
		}

		public Integer getTime() {
			return time;
		}

		public void setTime(int time) {
			this.time = time;
		}

		public int getCount() {
			return count;
		}

		public void setCount(int count) {
			this.count = count;
		}

	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 3) {
			System.out
					.println("Usage: CountVideoDetailIn <inputPath> <outputPath> <rundate>");
			System.exit(-1);
		}
		int exitCode = -1;
		ABTestCountVideoDetailIn countVideoDetailIn = new ABTestCountVideoDetailIn();
		exitCode = ToolRunner.run(countVideoDetailIn, args);
		System.exit(exitCode);
	}
}
