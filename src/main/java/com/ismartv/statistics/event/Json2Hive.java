package com.ismartv.statistics.event;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Json2Hive {

	public static final String TABLE_SEPERATOR = "\t";

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

		File fileSource = new File(filePath, date);
		if (!fileSource.exists() || !fileSource.isDirectory()) {
			throw new Exception("source path is invalid!");
		}

		File fileArchive = new File(filePath, "archive");
		if (!fileArchive.exists()) {
			fileArchive.mkdirs();
		}

		// archive file
		File archiveFile = new File(fileArchive, date);
		if (!archiveFile.exists()) {
			archiveFile.mkdirs();
		}

		// crate result file
		File resultFile = new File(resultPath, date + ".result");

		try (FileOutputStream fileOutputStream = new FileOutputStream(
				resultFile, true);
				OutputStreamWriter outputStreamWriter = new OutputStreamWriter(
						fileOutputStream, "UTF-8");
				BufferedWriter bufferedWriter = new BufferedWriter(
						outputStreamWriter);) {
			File[] files = fileSource.listFiles();
			for (File f : files) {
				transformJson(f.getAbsolutePath(), bufferedWriter);
				f.renameTo(new File(archiveFile, f.getName()));
			}
		}
	}

	/**
	 * read source file and write result file
	 * 
	 * @param path
	 * @param bufferedWriter
	 * @throws Exception
	 */
	private void transformJson(String path, BufferedWriter bufferedWriter)
			throws Exception {
		// read source file
		File file = new File(path);

		try (BufferedReader bufferedReader = new BufferedReader(new FileReader(
				file));) {
			String line = null;

			while ((line = bufferedReader.readLine()) != null) {
				String hiveStr = jsonString2Hive(line);
				bufferedWriter.write(hiveStr);
				bufferedWriter.newLine();
			}
		}
	}

	/**
	 * transform json to hive string
	 * 
	 * @param jsonString
	 * @return
	 * @throws Exception
	 */
	private String jsonString2Hive(String jsonString) throws Exception {
		if (jsonString == null || jsonString.trim().length() <= 0) {
			throw new Exception("jsonString is invalid!");
		}

		JSONObject jsonObject = JSON.parseObject(jsonString);

		StringBuffer stringBuffer = new StringBuffer();
		String tmp = null;

		// column:ts
		tmp = jsonObject.getString("time");
		stringBuffer.append(tmp == null ? "" : tmp.substring(8)).append(
				TABLE_SEPERATOR);

		// column:device
		tmp = jsonObject.getString("_device");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:sn
		tmp = jsonObject.getString("sn");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:token
		tmp = jsonObject.getString("token");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:event
		tmp = jsonObject.getString("event");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		// column:ip
		tmp = jsonObject.getString("ip");
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

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
		stringBuffer.append(tmp == null ? "" : tmp).append(TABLE_SEPERATOR);

		return stringBuffer.toString();
	}

	public static void main(String[] args) throws Exception {
		// String path = "D:\\work\\test\\home\\deploy\\statistics_event_daily";
		// String resultPath = "D:\\work\\test\\home\\deploy\\result";
		// String date = "20140917";
		// new CopyOfJson2Hive().execute(path, resultPath, date);

	}

}
