package com.ismartv.statistics.event;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class TransformDailyEvent extends Configured implements Tool {

	public static final String TABLE_SEPERATOR = "\t";

	@Override
	public int run(String[] args) throws Exception {
		execute(args[0], args[1], args[2]);
		return 0;
	}

	/**
	 * read the source log file and transform to hive structure file and move
	 * processed log file to archive dir
	 * 
	 * @param path
	 * @param resultPath
	 * @param date
	 * @throws Exception
	 */
	public void execute(String path, String resultPath, String date)
			throws Exception {

		File filePath = new File(path);
		if (!filePath.exists() || !filePath.isDirectory()) {
			throw new Exception("source path is invalid!");
		}

		// hdfs
		FileSystem hdfs = FileSystem.get(getConf());
		Path hdfspath = new Path(resultPath, "parsets=" + date);
		if (!hdfs.exists(hdfspath)) {
			hdfs.mkdirs(hdfspath);
		}
		FileStatus[] hdfsFileStatus = hdfs.listStatus(hdfspath);

		// local file
		File[] localFiles = filePath.listFiles();

		boolean flag = false;
		String localFileName = null;
		String hdfsFileName = null;
		for (int i = 0; i < localFiles.length; i++) {
			localFileName = localFiles[i].getName();
			for (int j = 0; j < hdfsFileStatus.length; j++) {
				hdfsFileName = hdfsFileStatus[j].getPath().getName();
				if (localFileName != null && localFileName.equals(hdfsFileName)) {
					flag = true;
					break;
				}
			}
			if (flag == true) {
				flag = false;
				continue;
			} else {
				readLocalLogAndWriteToHive(localFiles[i].getAbsolutePath(),
						new Path(hdfspath, localFileName), hdfs);
			}
		}
		//
		Process p = null;
		BufferedReader bufferedReader = null;
		String line = null;
		List<String> checkCommands = new ArrayList<>();
		checkCommands.add("hive");
		checkCommands.add("-e");
		checkCommands.add("show partitions daily_event");

		p = new ProcessBuilder(checkCommands).start();
		bufferedReader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		// 检查分区是否存在
		while ((line = bufferedReader.readLine()) != null) {
			if (line.equals("parsets=" + date)) {
				return;
			}
		}

		String cmd = "alter table daily_event add partition (parsets='" + date
				+ "') location '" + hdfspath.toString() + "'";

		List<String> commands = new ArrayList<>();
		commands.add("hive");
		commands.add("-e");
		commands.add(cmd);
		// 添加分区
		p = new ProcessBuilder(commands).start();
	}

	private void readLocalLogAndWriteToHive(String localPath, Path hdfsPath,
			FileSystem hdfs) throws Exception {
		// read source file
		File file = new File(localPath);
		hdfs.createNewFile(hdfsPath);
		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(
				file));
				FSDataOutputStream fsDataOutputStream = hdfs.create(hdfsPath)) {
			String line = null;

			while ((line = bufferedReader.readLine()) != null) {
				String hiveStr = log2Hive(line);
				if (hiveStr != null) {
					fsDataOutputStream.write(hiveStr.getBytes());
				}
			}
		}
	}

	private String log2Hive(String logString) throws Exception {
		if (logString == null || logString.trim().length() <= 0) {
			throw new Exception("jsonString is invalid!");
		}

		String[] parts = logString.substring(0, logString.indexOf("{") - 1)
				.split(" ");
		String jsonString = logString.substring(logString.indexOf("{"));

		String ts = parts[0];
		String token = parts[1];
		if (token == null || token.equals("-")) {
			return null;
		}
		String device = parts[2];
		String sn = parts[3];
		String event = parts[4];
		String ip = parts[5];

		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append(ts).append(TABLE_SEPERATOR).append(device)
				.append(TABLE_SEPERATOR).append(sn).append(TABLE_SEPERATOR)
				.append(token).append(TABLE_SEPERATOR).append(event)
				.append(TABLE_SEPERATOR).append(ip).append(TABLE_SEPERATOR);

		JSONObject jsonObject = JSON.parseObject(jsonString);
		String tmp = null;

		// column:duration
		tmp = jsonObject.getString("duration");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:clip
		tmp = jsonObject.getString("clip");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:item
		tmp = jsonObject.getString("item");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:subitem
		tmp = jsonObject.getString("subitem");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:title
		tmp = jsonObject.getString("title");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:data1
		if ((tmp = jsonObject.getString("to_item")) != null
				|| (tmp = jsonObject.getString("live_id")) != null
				|| (tmp = jsonObject.getString("scope")) != null
				|| (tmp = jsonObject.getString("to")) != null
				|| (tmp = jsonObject.getString("pk")) != null
				|| (tmp = jsonObject.getString("channel")) != null
				|| (tmp = jsonObject.getString("code")) != null
				|| (tmp = jsonObject.getString("flag")) != null) {
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		} else {
			stringBuffer.append("").append(TABLE_SEPERATOR);
		}

		// column:data2
		if ((tmp = jsonObject.getString("to_subitem")) != null
				|| (tmp = jsonObject.getString("sid")) != null
				|| (tmp = jsonObject.getString("q")) != null
				|| (tmp = jsonObject.getString("type")) != null
				|| (tmp = jsonObject.getString("area")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);

		// column:data3
		if ((tmp = jsonObject.getString("position")) != null
				|| (tmp = jsonObject.getString("content_type")) != null
				|| (tmp = jsonObject.getString("genre")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);

		// column:data4
		if ((tmp = jsonObject.getString("section")) != null
				|| (tmp = jsonObject.getString("air_date")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);

		// column:data5
		if ((tmp = jsonObject.getString("age")) != null
				|| (tmp = jsonObject.getString("source")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);

		// column:data6
		if ((tmp = jsonObject.getString("feature")) != null
				|| (tmp = jsonObject.getString("location")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);

		// column:data7;
		tmp = jsonObject.getString("userid");
		stringBuffer.append(tmp == null ? "" : tmp).append("\n");

		return stringBuffer.toString();
	}

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 3) {
			System.out
					.println("Usage: UserLogItemCFRecommend <inputPath> <outputPath> <date>");
			System.exit(-1);
		}
		int exitCode = -1;
		exitCode = ToolRunner.run(new TransformDailyEvent(), args);
		System.exit(exitCode);
	}
}
