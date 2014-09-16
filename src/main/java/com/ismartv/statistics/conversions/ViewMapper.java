package com.ismartv.statistics.conversions;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ViewMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	private String date = "";

	private TextPair outKey = new TextPair();
	private Text outValue = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		date = "";
		InputSplit inputSplit = context.getInputSplit();
		if (inputSplit instanceof FileSplit) {
			FileSplit fileSplit = (FileSplit) inputSplit;
			Path path = fileSplit.getPath();
			String name = path.getParent().getName();
			if (name.startsWith("parsets=")) {
				date = name.substring("parsets=".length());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().trim();
		String[] fields = line.split(",");
		if (fields.length < 5) {
			return;
		}

		// ts 日志的时间戳
		// device device类型，设备型号
		// sn 设备的sn，设备号（作为用户标示）
		// token 日志中的token，16位随机数，标识一次开机后的一系列事件
		// event 事件类型
		// ip 设备的ip
		// duration 时间, 单位s
		// clip 视频clip,视频id, 例如: 153976
		// item 媒体id
		// subitem 子媒体id

		String ts = date
				+ new DecimalFormat("000000").format(Integer
						.parseInt(fields[0]));// 日志的时间戳
		String device = fields[1];// 设备型号
		String sn = fields[2];// 设备的设备号（作为用户标示）
		// String token = fields[3];// 日志中的token，16位随机数，标识一次开机后的一系列事件
		String event = fields[4].intern();// 事件类型

		REPORT_EVENT reportEvent = REPORT_EVENT.getEventsByEvent(event, fields);
		if (reportEvent == REPORT_EVENT.EVENT_UNKNOWN) {
			return;
		}

		outKey.set(new Text(device + "\u0001" + sn),
				new Text(ts + reportEvent.getMillisSecond()));
		outValue.set(reportEvent.name());
		context.write(outKey, outValue);

		outKey.set(new Text("ALL\u0001" + sn),
				new Text(ts + reportEvent.getMillisSecond()));
		outValue.set(reportEvent.name());
		context.write(outKey, outValue);
	}
}
