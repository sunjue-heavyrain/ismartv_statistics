package com.ismartv.statistics.countDetailIn.abTest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ABTestCountVideoDetailInStage1 {

	public static class Mapper extends
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		// ///////////////////
		// private Log log;

		// ///////////////////

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			// ////////////////////////////////
			// log = new Log(context.getConfiguration());
			// ////////////////////////////////

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
			String[] strs = value.toString().split("\t");

			if (strs.length < 5) {
				return;
			}

			String ts = strs[0];
			String sn = strs[2];
			String event = strs[4];

			// 过滤 sn
			if (sn == null || sn.equals("(Windows;")
					|| sn.equals("(compatible;")) {
				return;
			}

			// 过滤 event
			if (!event.equals("video_channel_in")) {
				if (!event.equals("video_detail_in")) {
					if (!event.equals("video_play_load")) {
						return;
					}
				}
			}

			outKey.set(sn + ":" + ts);
			outValue.set(event);
			context.write(outKey, outValue);
		}
	}

	public static class SnPartitionner<K, V> extends Partitioner<K, V> {

		@Override
		public int getPartition(K key, V value, int numPartitions) {
			String keyString = key.toString();
			if (!keyString.contains(":")) {
				return -1;
			}
			String sn = keyString.substring(0, keyString.indexOf(':'));
			return (sn.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class Reducer extends
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, NullWritable> {

		public static SimpleDateFormat dateFormat = new SimpleDateFormat(
				"HHmmss");

		private Text outputKeyValue = new Text();
		private String currentSn = null;
		private boolean currentChannelIn = false;
		private boolean currentDeailIn = false;
		private String channelInTs = null;
		private String mod = null;

		// private Log log;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// log = new Log(context.getConfiguration());
			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] strs = key.toString().split(":");
			if (strs.length != 2) {
				return;
			}

			String sn = strs[0];
			String ts = strs[1];

			if (sn == null || !sn.equals(currentSn)) {
				currentSn = sn;
				currentChannelIn = false;
				mod = (currentSn.hashCode() & 1) == 1 ? "A" : "B";
			}

			String value = null;
			int time = -1;
			for (Text t : values) {
				value = t.toString();

				// log.log(currentSn + ":" + ts + "\t" + value);

				if (value.equals("video_channel_in")) {
					currentChannelIn = true;
					channelInTs = ts;
					currentDeailIn = false;
				} else if (value.equals("video_detail_in")) {
					if (currentChannelIn) {
						time = calcTimeInterval(ts, channelInTs);
						outputKeyValue.set(mod + ":" + String.valueOf(time));
						context.write(outputKeyValue, NullWritable.get());
						currentDeailIn = true;
					}
				} else if (value.equals("video_play_load")) {
					if (currentChannelIn) {
						if (currentDeailIn) {
							currentDeailIn = false;
						} else {
							time = calcTimeInterval(ts, channelInTs);
							outputKeyValue
									.set(mod + ":" + String.valueOf(time));
							context.write(outputKeyValue, NullWritable.get());
						}
					}
				}
			}
		}

		private static int calcTimeInterval(String ts, String anothorTs)
				throws InterruptedException {
			// 后面事件对应的ts必须大于前面事件的ts
			if (ts != null && ts.compareTo(anothorTs) < 0) {
				throw new InterruptedException(
						"ts must equalt or bigger then anothorTs");
			}
			int timeInterval;
			try {
				timeInterval = (int) (dateFormat.parse(ts).getTime() - dateFormat
						.parse(anothorTs).getTime());
			} catch (ParseException e) {
				throw new InterruptedException(e.getMessage());
			}

			// 毫秒 转 秒
			timeInterval = timeInterval / 1000;

			if (timeInterval <= 60) {
				if (timeInterval == 0) {
					return 10;
				}
				return (int) (Math.ceil(timeInterval / 10.0) * 10);
			} else if (timeInterval > 600) {
				return 660;
			} else {
				return (int) (Math.ceil(timeInterval / 60.0) * 60);
			}
		}
	}
}
