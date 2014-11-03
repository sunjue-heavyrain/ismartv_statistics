package com.ismartv.statistics.event;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ismartv.util.HiveConnection;

public class TransformDailyEvent {

	public static SimpleDateFormat dateFormat = new SimpleDateFormat("HHmmss");

	public static final String TABLE_SEPERATOR = "\t";

	/**
	 * read the source log file and transform to hive structure file and move
	 * processed log file to archive dir
	 * 
	 * @param path
	 * @param resultPath
	 * @param runDate
	 *            format as 2014-10-31 01:59:52
	 * @throws Exception
	 */
	public void execute(String path, String resultPath, String runDate)
			throws Exception {

		File filePath = new File(path);
		if (!filePath.exists()) {
			throw new Exception("source path is invalid!");
		}

		String[] dateString = runDate.split(" ");
		String day = dateString[0].replace("-", "_");
		String hour = dateString[1].substring(0, 2);

		// 按跑数时间构造源文件
		File sourceFile = new File(new File(path, "data_" + day), "data_" + day
				+ "_" + hour + ".log");

		if (!sourceFile.exists()) {
			throw new Exception("sourceFile is invalid");
		}

		// hdfs
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://10.0.4.10");
		configuration.set("mapred.job.tracker", "10.0.4.10:8021");

		FileSystem hdfs = FileSystem.get(configuration);

		String anothorDate = day.replace("_", "");
		Path hdfspath = new Path(resultPath, "parsets=" + anothorDate);
		if (!hdfs.exists(hdfspath)) {
			hdfs.mkdirs(hdfspath);
		}

		// 读源文件并写入hdfs
		readLocalLogAndWriteToHive(sourceFile,
				new Path(hdfspath, sourceFile.getName()), hdfs);

		Connection conn = null;
		try {
			// 使用jdbc连接hive
			conn = HiveConnection.getHiveConnection();
			Statement statement = conn.createStatement();
			ResultSet resultSet = statement
					.executeQuery("show partitions daily_event");

			// 检查分区是否存在
			while (resultSet.next()) {
				if (resultSet.getString(1).equals("parsets=" + anothorDate)) {
					return;
				}
			}
			String cmd = "alter table daily_event add partition (parsets='"
					+ anothorDate + "') location '" + hdfspath.toString() + "'";
			statement.execute(cmd);
		} catch (Exception e) {
			throw e;
		} finally {
			HiveConnection.close();
		}
	}

	private void readLocalLogAndWriteToHive(File file, Path hdfsPath,
			FileSystem hdfs) throws Exception {
		// read source file
		int countRows = 0;
		int skipRows = 0;
		if (hdfs.exists(hdfsPath)) {
			throw new Exception("hdfs file exist");
		}
		hdfs.createNewFile(hdfsPath);
		try (BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(file), "UTF-8"));
		// new BufferedReader(new FileReader(file));
				FSDataOutputStream fsDataOutputStream = hdfs.create(hdfsPath)) {
			String line = null;

			while ((line = bufferedReader.readLine()) != null) {
				String hiveStr = log2Hive(line);
				if (hiveStr == null) {
					skipRows++;
					continue;
				}
				fsDataOutputStream.write(hiveStr.getBytes("UTF-8"));
				countRows++;
			}
		}
		System.out.println("sourceFile:" + file.getAbsolutePath()
				+ " targetFile:" + hdfsPath.toString() + " countRows="
				+ countRows + " skipRows=" + skipRows);
	}

	private String log2Hive(String logString) throws Exception {
		if (logString == null || logString.trim().length() <= 0) {
			throw new Exception("jsonString is invalid!");
		}

		if (logString.indexOf("{") - 1 <= 0) {
			return null;
		}

		String[] parts = logString.substring(0, logString.indexOf("{") - 1)
				.split(" ");
		String jsonString = logString.substring(logString.indexOf("{"));

		// 过滤掉不规则的数据
		if (parts.length != 6) {
			return null;
		}

		String ts = dateFormat.format(Long.valueOf(parts[0]) * 1000);
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

	public static void main(String[] args) {
		if (args == null || args.length != 3) {
			System.out
					.println("Usage: UserLogItemCFRecommend <inputPath> <outputPath> <date>");
			System.exit(-1);
		}
		try {
			new TransformDailyEvent().execute(args[0], args[1], args[2]);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		System.exit(0);
	}
}
