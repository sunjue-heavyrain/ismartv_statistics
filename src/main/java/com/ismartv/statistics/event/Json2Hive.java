package com.ismartv.statistics.event;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import net.sf.json.JSONObject;

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
		JSONObject jsonObject = JSONObject.fromObject(jsonString);
		StringBuffer stringBuffer = new StringBuffer();

		// column:ts
		stringBuffer.append(jsonObject.optString("time", "").substring(8))
				.append(TABLE_SEPERATOR);
		// column:device
		stringBuffer.append(jsonObject.optString("_device", "")).append(
				TABLE_SEPERATOR);
		// column:sn
		stringBuffer.append(jsonObject.optString("sn", "")).append(
				TABLE_SEPERATOR);
		// column:token
		stringBuffer.append(jsonObject.optString("token", "")).append(
				TABLE_SEPERATOR);
		// column:event
		stringBuffer.append(jsonObject.optString("event", "")).append(
				TABLE_SEPERATOR);
		// column:ip
		stringBuffer.append(jsonObject.optString("ip", "")).append(
				TABLE_SEPERATOR);
		// column:duration
		stringBuffer.append(jsonObject.optString("duration", "")).append(
				TABLE_SEPERATOR);
		// column:clip
		stringBuffer.append(jsonObject.optString("clip", "")).append(
				TABLE_SEPERATOR);
		// column:item
		stringBuffer.append(jsonObject.optString("item", "")).append(
				TABLE_SEPERATOR);
		// column:subitem
		stringBuffer.append(jsonObject.optString("subitem", "")).append(
				TABLE_SEPERATOR);
		// column:title
		stringBuffer.append(jsonObject.optString("title", "")).append(
				TABLE_SEPERATOR);
		
		// column:data1
		String tmp = null;
		if ((tmp = jsonObject.optString("to_item")) != null
				|| (tmp = jsonObject.optString("live_id")) != null
				|| (tmp = jsonObject.optString("scope")) != null
				|| (tmp = jsonObject.optString("to")) != null
				|| (tmp = jsonObject.optString("pk")) != null
				|| (tmp = jsonObject.optString("channel")) != null
				|| (tmp = jsonObject.optString("code")) != null
				|| (tmp = jsonObject.optString("flag")) != null) {
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		} else {
			stringBuffer.append("").append(TABLE_SEPERATOR);
		}
		// column:data2
		tmp = null;
		if ((tmp = jsonObject.optString("to_subitem")) != null
				|| (tmp = jsonObject.optString("sid")) != null
				|| (tmp = jsonObject.optString("q")) != null
				|| (tmp = jsonObject.optString("type")) != null
				|| (tmp = jsonObject.optString("area")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);
		// column:data3
		tmp = null;
		if ((tmp = jsonObject.optString("position")) != null
				|| (tmp = jsonObject.optString("content_type")) != null
				|| (tmp = jsonObject.optString("genre")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);
		// column:data4
		tmp = null;
		if ((tmp = jsonObject.optString("section")) != null
				|| (tmp = jsonObject.optString("air_date")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);
		// column:data5
		tmp = null;
		if ((tmp = jsonObject.optString("age")) != null
				|| (tmp = jsonObject.optString("source")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);
		// column:data6
		tmp = null;
		if ((tmp = jsonObject.optString("feature")) != null
				|| (tmp = jsonObject.optString("location")) != null)
			stringBuffer.append(tmp).append(TABLE_SEPERATOR);
		else
			stringBuffer.append("").append(TABLE_SEPERATOR);
		// column:data7;
		stringBuffer.append(jsonObject.optString("userid", "")).append(
				TABLE_SEPERATOR);

		return stringBuffer.toString();
	}

	public static void main(String[] args) throws Exception {
		String path = "D:\\work\\test\\home\\deploy\\statistics_event_daily";
		String resultPath = "D:\\work\\test\\home\\deploy\\result";
		String date = "20140917";
		new Json2Hive().execute(path, resultPath, date);
	}

}
