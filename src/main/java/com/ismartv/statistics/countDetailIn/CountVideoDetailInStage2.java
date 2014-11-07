package com.ismartv.statistics.countDetailIn;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class CountVideoDetailInStage2 {

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
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, DoubleWritable, IntWritable> {

		// private Text outputKey = new Text();
		private DoubleWritable outKey = new DoubleWritable();
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
			// outputKey.set(key.toString() + "\t" + String.valueOf(sum));
			// context.write(outputKey, NullWritable.get());

			outKey.set(Double.valueOf(key.toString()));
			outValue.set(sum);
			context.write(outKey, outValue);
		}
	}
}
