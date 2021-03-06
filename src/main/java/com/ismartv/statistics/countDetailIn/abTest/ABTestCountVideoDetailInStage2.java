package com.ismartv.statistics.countDetailIn.abTest;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class ABTestCountVideoDetailInStage2 {

	public static class Mapper
			extends
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

		private IntWritable outputValue = new IntWritable(1);

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, outputValue);
		}
	}

	public static class Reducer
			extends
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outValue = new IntWritable();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
		}
	}
}
