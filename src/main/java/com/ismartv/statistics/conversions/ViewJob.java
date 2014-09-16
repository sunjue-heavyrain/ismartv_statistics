package com.ismartv.statistics.conversions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

public class ViewJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args == null || args.length != 4) {
			return -1;
		}
		String date = args[0];
		String strInputPath = args[1] + date;
		String strOutputPath = args[2];
		String outHbaseTableName = args[3];

		Configuration conf = getConf();
		conf.set("mapred.compress.map.output", "true");

		FileSystem fileSystem = FileSystem.get(conf);

		Path outRootPath = new Path(strOutputPath);
		Path outputPath = new Path(outRootPath, String.valueOf(System
				.currentTimeMillis()));
		while (fileSystem.exists(outputPath)) {
			outputPath = new Path(outRootPath, String.valueOf(System
					.currentTimeMillis()));
		}

		// Job job = Job.getInstance(conf, "ViewJob");
		Job job = new Job(conf, "ViewJob");
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(strInputPath));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(ViewMapper.class);
		job.setReducerClass(ViewReducer.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setPartitionerClass(TextPairKeyPartitioner.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		long l = System.currentTimeMillis();
		boolean tf = false;
		try {
			tf = job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		l = System.currentTimeMillis() - l;
		System.out.println("Run ViewJob: result=" + tf + "; time=" + l);

		HashMap<String, StatisticsData> mapStatisticsData = new HashMap<String, StatisticsData>();
		if (fileSystem.isFile(outputPath)) {
			processOutFile(fileSystem, outputPath, mapStatisticsData);
		} else {
			FileStatus[] fss = fileSystem.listStatus(outputPath,
					new PathFilter() {
						public boolean accept(Path p) {
							String name = p.getName();
							return ((!(name.startsWith("_"))) && (!(name
									.startsWith("."))));
						}
					});
			for (FileStatus fileStatus : fss) {
				processOutFile(fileSystem, fileStatus.getPath(),
						mapStatisticsData);
			}
		}
		System.out.println("result in " + date + ": "
				+ mapStatisticsData.size());

		// TODO test not hbase
		// HTable hTable = new HTable(conf, outHbaseTableName);
		long ts = System.currentTimeMillis();
		ArrayList<Put> lstPut = new ArrayList<Put>(mapStatisticsData.size());
		for (Entry<String, StatisticsData> entry : mapStatisticsData.entrySet()) {
			String device = entry.getKey();
			StatisticsData statisticsData = entry.getValue();
			Put put = new Put(
					Bytes.toBytes("ALL".equalsIgnoreCase(device) ? date : date
							+ "_" + device), ts);
			putData(put, statisticsData, ts);
			lstPut.add(put);
		}

		// hTable.put(lstPut);
		// hTable.close();

		for (Put p : lstPut) {
			Map<byte[], List<KeyValue>> map = p.getFamilyMap();
			for (Entry<byte[], List<KeyValue>> entry : map.entrySet()) {
				for (KeyValue kv : entry.getValue()) {
					System.out.println(new String(kv.getRow()) + ":"
							+ new String(kv.getFamily()) + ":"
							+ new String(kv.getQualifier()) + ":"
							+ new String(kv.getValue()));
				}
			}
		}

		return 0;
	}

	private void putData(Put put, StatisticsData statisticsData, long ts) {
		DecimalFormat format = new DecimalFormat("0.00");
		String strPercentage = "";
		byte[] family = Bytes.toBytes("data");

		// 总PV
		put.add(family, Bytes.toBytes("all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.allPv)));
		// 总UV
		put.add(family, Bytes.toBytes("all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.allUv)));

		// 历史总PV
		put.add(family, Bytes.toBytes("history_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.historyAllPv)));
		// 历史触发播放PV
		put.add(family, Bytes.toBytes("history_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.historyPlayPv)));
		// 历史触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.historyPlayPv) / statisticsData.historyAllPv) * 100);
		put.add(family, Bytes.toBytes("history_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 历史触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.historyPlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("history_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 历史总UV
		put.add(family, Bytes.toBytes("history_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.historyAllUv)));
		// 历史触发播放UV
		put.add(family, Bytes.toBytes("history_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.historyPlayUv)));
		// 历史触发播放UV转化率
		strPercentage = format
				.format((((float) statisticsData.historyPlayUv) / statisticsData.historyAllUv) * 100);
		put.add(family, Bytes.toBytes("history_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 历史触发播放UV占比
		strPercentage = format
				.format((((float) statisticsData.historyPlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("history_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 看吧总PV
		put.add(family, Bytes.toBytes("kanba_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.kanbaAllPv)));
		// 看吧触发播放PV
		put.add(family, Bytes.toBytes("kanba_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.kanbaPlayPv)));
		// 看吧触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.kanbaPlayPv) / statisticsData.kanbaAllPv) * 100);
		put.add(family, Bytes.toBytes("kanba_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 看吧触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.kanbaPlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("kanba_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 看吧总UV
		put.add(family, Bytes.toBytes("kanba_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.kanbaAllUv)));
		// 看吧触发播放UV
		put.add(family, Bytes.toBytes("kanba_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.kanbaPlayUv)));
		// 看吧触发播放UV转化率
		strPercentage = format
				.format((((float) statisticsData.kanbaPlayUv) / statisticsData.kanbaAllUv) * 100);
		put.add(family, Bytes.toBytes("kanba_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 看吧触发播放UV占比
		strPercentage = format
				.format((((float) statisticsData.kanbaPlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("kanba_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索总PV
		put.add(family, Bytes.toBytes("search_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.searchAllPv)));
		// 搜索触发播放PV
		put.add(family, Bytes.toBytes("search_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.searchPlayPv)));
		// 搜索触发详情页PV
		put.add(family, Bytes.toBytes("search_detail_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.searchDetailPv)));
		// 搜索触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.searchPlayPv) / statisticsData.searchAllPv) * 100);
		put.add(family, Bytes.toBytes("search_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.searchPlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("search_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.searchDetailPv) / statisticsData.searchAllPv) * 100);
		put.add(family, Bytes.toBytes("search_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.searchDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("search_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索总UV
		put.add(family, Bytes.toBytes("search_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.searchAllUv)));
		// 搜索触发播放UV
		put.add(family, Bytes.toBytes("search_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.searchPlayUv)));
		// 搜索触发详情页UV
		put.add(family, Bytes.toBytes("search_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.searchDetailUv)));
		// 搜索触发播放UV转化率
		strPercentage = format
				.format((((float) statisticsData.searchPlayUv) / statisticsData.searchAllUv) * 100);
		put.add(family, Bytes.toBytes("search_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索触发播放UV占比
		strPercentage = format
				.format((((float) statisticsData.searchPlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("search_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.searchDetailUv) / statisticsData.searchAllUv) * 100);
		put.add(family, Bytes.toBytes("search_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 搜索触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.searchDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("search_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 详情页总PV
		put.add(family, Bytes.toBytes("detail_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.detailAllPv)));
		// 详情页触发播放PV
		put.add(family, Bytes.toBytes("detail_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.detailPlayPv)));
		// 详情页触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.detailPlayPv) / statisticsData.detailAllPv) * 100);
		put.add(family, Bytes.toBytes("detail_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 详情页触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.detailPlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("detail_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 详情页总UV
		put.add(family, Bytes.toBytes("detail_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.detailAllUv)));
		// 详情页触发播放UV
		put.add(family, Bytes.toBytes("detail_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.detailPlayUv)));
		// 详情页触发播放UV转化率
		strPercentage = format
				.format((((float) statisticsData.detailPlayUv) / statisticsData.detailAllUv) * 100);
		put.add(family, Bytes.toBytes("detail_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 详情页触发播放UV占比
		strPercentage = format
				.format((((float) statisticsData.detailPlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("detail_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢总PV
		put.add(family, Bytes.toBytes("guess_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.guessAllPv)));
		// 分类猜你喜欢触发播放PV
		put.add(family, Bytes.toBytes("guess_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.guessPlayPv)));
		// 分类猜你喜欢触发详情页PV
		put.add(family, Bytes.toBytes("guess_detail_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.guessDetailPv)));
		// 分类猜你喜欢触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.guessPlayPv) / statisticsData.guessAllPv) * 100);
		put.add(family, Bytes.toBytes("guess_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.guessPlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("guess_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.guessDetailPv) / statisticsData.guessAllPv) * 100);
		put.add(family, Bytes.toBytes("guess_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.guessDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("guess_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢总UV
		put.add(family, Bytes.toBytes("guess_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.guessAllUv)));
		// 分类猜你喜欢触发播放UV
		put.add(family, Bytes.toBytes("guess_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.guessPlayUv)));
		// 分类猜你喜欢触发详情页UV
		put.add(family, Bytes.toBytes("guess_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.guessDetailUv)));
		// 分类猜你喜欢触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.guessPlayUv) / statisticsData.guessAllUv) * 100);
		put.add(family, Bytes.toBytes("guess_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.guessPlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("guess_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.guessDetailUv) / statisticsData.guessPlayUv) * 100);
		put.add(family, Bytes.toBytes("guess_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类猜你喜欢触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.guessDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("guess_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页总PV
		put.add(family, Bytes.toBytes("list_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.listAllPv)));
		// 列表页触发播放PV
		put.add(family, Bytes.toBytes("list_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.listPlayPv)));
		// 列表页触发详情页PV
		put.add(family, Bytes.toBytes("list_detail_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.listDetailPv)));
		// 列表页触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.listPlayPv) / statisticsData.listAllPv) * 100);
		put.add(family, Bytes.toBytes("list_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.listPlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("list_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.listDetailPv) / statisticsData.listAllPv) * 100);
		put.add(family, Bytes.toBytes("list_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.listDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("list_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页总UV
		put.add(family, Bytes.toBytes("list_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.listAllUv)));
		// 列表页触发播放UV
		put.add(family, Bytes.toBytes("list_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.listPlayUv)));
		// 列表页触发详情页UV
		put.add(family, Bytes.toBytes("list_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.listDetailUv)));
		// 列表页触发播放UV转化率
		strPercentage = format
				.format((((float) statisticsData.listPlayUv) / statisticsData.listAllUv) * 100);
		put.add(family, Bytes.toBytes("list_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页触发播放UV占比
		strPercentage = format
				.format((((float) statisticsData.listPlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("list_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.listDetailUv) / statisticsData.listPlayUv) * 100);
		put.add(family, Bytes.toBytes("list_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 列表页触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.listDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("list_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频总PV
		put.add(family, Bytes.toBytes("relate_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.relateAllPv)));
		// 关联视频触发播放PV
		put.add(family, Bytes.toBytes("relate_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.relatePlayPv)));
		// 关联视频触发详情页PV
		put.add(family, Bytes.toBytes("relate_detail_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.relateDetailPv)));
		// 关联视频触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.relatePlayPv) / statisticsData.relateAllPv) * 100);
		put.add(family, Bytes.toBytes("relate_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.relatePlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("relate_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.relateDetailPv) / statisticsData.relateAllPv) * 100);
		put.add(family, Bytes.toBytes("relate_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.relateDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("relate_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频总UV
		put.add(family, Bytes.toBytes("relate_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.relateAllUv)));
		// 关联视频触发播放UV
		put.add(family, Bytes.toBytes("relate_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.relatePlayUv)));
		// 关联视频触发详情页UV
		put.add(family, Bytes.toBytes("relate_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.relateDetailUv)));
		// 关联视频触发详情页UV
		put.add(family, Bytes.toBytes("relate_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.relateDetailUv)));
		// 关联视频触发播放UV转化率
		strPercentage = format
				.format((((float) statisticsData.relatePlayUv) / statisticsData.relateAllUv) * 100);
		put.add(family, Bytes.toBytes("relate_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频触发播放UV占比
		strPercentage = format
				.format((((float) statisticsData.relatePlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("relate_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.relateDetailUv) / statisticsData.relateAllUv) * 100);
		put.add(family, Bytes.toBytes("relate_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 关联视频触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.relateDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("relate_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 首页预告片总PV
		put.add(family, Bytes.toBytes("trailer_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.trailerAllPv)));
		// 首页预告片触发详情页PV
		put.add(family, Bytes.toBytes("trailer_detail_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.trailerDetailPv)));
		// 首页预告片触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.trailerDetailPv) / statisticsData.trailerAllPv) * 100);
		put.add(family, Bytes.toBytes("trailer_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 首页预告片触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.trailerDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("trailer_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 首页预告片总UV
		put.add(family, Bytes.toBytes("trailer_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.trailerAllUv)));
		// 首页预告片触发详情页UV
		put.add(family, Bytes.toBytes("trailer_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.trailerDetailUv)));
		// 首页预告片触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.trailerDetailUv) / statisticsData.trailerAllUv) * 100);
		put.add(family, Bytes.toBytes("trailer_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 首页预告片触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.trailerDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("trailer_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 首页推荐位总PV
		put.add(family, Bytes.toBytes("recommended_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.recommendedAllPv)));
		// 首页推荐位触发详情页PV
		put.add(family, Bytes.toBytes("recommended_detail_pv"), ts, Bytes
				.toBytes(String.valueOf(statisticsData.recommendedDetailPv)));
		// 首页推荐位触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.recommendedDetailPv) / statisticsData.recommendedAllPv) * 100);
		put.add(family, Bytes.toBytes("recommended_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 首页推荐位触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.recommendedDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("recommended_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 首页推荐位总UV
		put.add(family, Bytes.toBytes("recommended_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.recommendedAllUv)));
		// 首页推荐位触发详情页UV
		put.add(family, Bytes.toBytes("recommended_detail_uv"), ts, Bytes
				.toBytes(String.valueOf(statisticsData.recommendedDetailUv)));
		// 首页推荐位触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.recommendedDetailUv) / statisticsData.recommendedAllUv) * 100);
		put.add(family, Bytes.toBytes("recommended_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 首页推荐位触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.recommendedDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("recommended_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 我的频道(收藏和追剧)总PV
		put.add(family, Bytes.toBytes("mychannel_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.mychannelAllPv)));
		// 我的频道(收藏和追剧)触发详情页PV
		put.add(family, Bytes.toBytes("mychannel_detail_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.mychannelDetailPv)));
		// 我的频道(收藏和追剧)触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.mychannelDetailPv) / statisticsData.mychannelAllPv) * 100);
		put.add(family, Bytes.toBytes("mychannel_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 我的频道(收藏和追剧)触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.mychannelDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("mychannel_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 我的频道(收藏和追剧)总UV
		put.add(family, Bytes.toBytes("mychannel_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.mychannelAllUv)));
		// 我的频道(收藏和追剧)触发详情页UV
		put.add(family, Bytes.toBytes("mychannel_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.mychannelDetailUv)));
		// 我的频道(收藏和追剧)触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.mychannelDetailUv) / statisticsData.mychannelAllUv) * 100);
		put.add(family, Bytes.toBytes("mychannel_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 我的频道(收藏和追剧)触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.mychannelDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("mychannel_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选总PV
		put.add(family, Bytes.toBytes("filter_all_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.filterAllPv)));
		// 分类筛选触发播放PV
		put.add(family, Bytes.toBytes("filter_play_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.filterPlayPv)));
		// 分类筛选触发详情页PV
		put.add(family, Bytes.toBytes("filter_detail_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.filterDetailPv)));
		// 分类筛选触发播放PV转化率
		strPercentage = format
				.format((((float) statisticsData.filterPlayPv) / statisticsData.filterAllPv) * 100);
		put.add(family, Bytes.toBytes("filter_play_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选触发播放PV占比
		strPercentage = format
				.format((((float) statisticsData.filterPlayPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("filter_play_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选触发详情页PV转化率
		strPercentage = format
				.format((((float) statisticsData.filterDetailPv) / statisticsData.filterAllPv) * 100);
		put.add(family, Bytes.toBytes("filter_detail_pv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选触发详情页PV占比
		strPercentage = format
				.format((((float) statisticsData.filterDetailPv) / statisticsData.allPv) * 100);
		put.add(family, Bytes.toBytes("filter_detail_pv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选总UV
		put.add(family, Bytes.toBytes("filter_all_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.filterAllUv)));
		// 分类筛选触发播放UV
		put.add(family, Bytes.toBytes("filter_play_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.filterPlayUv)));
		// 分类筛选触发详情页UV
		put.add(family, Bytes.toBytes("filter_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.filterDetailUv)));
		// 分类筛选触发详情页UV
		put.add(family, Bytes.toBytes("filter_detail_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.filterDetailUv)));
		// 分类筛选触发播放UV转化率
		strPercentage = format
				.format((((float) statisticsData.filterPlayUv) / statisticsData.filterAllUv) * 100);
		put.add(family, Bytes.toBytes("filter_play_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选触发播放UV占比
		strPercentage = format
				.format((((float) statisticsData.filterPlayUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("filter_play_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选触发详情页UV转化率
		strPercentage = format
				.format((((float) statisticsData.filterDetailUv) / statisticsData.filterAllUv) * 100);
		put.add(family, Bytes.toBytes("filter_detail_uv_conversion"), ts,
				Bytes.toBytes(strPercentage));
		// 分类筛选触发详情页UV占比
		strPercentage = format
				.format((((float) statisticsData.filterDetailUv) / statisticsData.allUv) * 100);
		put.add(family, Bytes.toBytes("filter_detail_uv_ratio"), ts,
				Bytes.toBytes(strPercentage));
		// 网络测速PV
		put.add(family, Bytes.toBytes("speedtester_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.speedtesterPv)));
		// 网络测速UV
		put.add(family, Bytes.toBytes("speedtester_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.speedtesterUv)));
		// 收藏及取消收藏PV
		put.add(family, Bytes.toBytes("collect_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.collectPv)));
		// 收藏及取消收藏UV
		put.add(family, Bytes.toBytes("collect_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.collectUv)));
		// 追剧PV
		put.add(family, Bytes.toBytes("follow_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.followPv)));
		// 追剧UV
		put.add(family, Bytes.toBytes("follow_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.followUv)));
		// 取消追剧PV
		put.add(family, Bytes.toBytes("follow_cancel_pv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.followCancelPv)));
		// 取消追剧UV
		put.add(family, Bytes.toBytes("follow_cancel_uv"), ts,
				Bytes.toBytes(String.valueOf(statisticsData.followCancelUv)));
	}

	private void processOutFile(FileSystem fileSystem, Path outputPath,
			HashMap<String, StatisticsData> mapStatisticsData) {
		try (InputStream in = fileSystem.open(outputPath);
				BufferedReader r = new BufferedReader(new InputStreamReader(in))) {
			String line = null;
			while ((line = r.readLine()) != null) {
				int idx = line.indexOf('\u0001');
				if (idx <= 0) {
					return;
				}
				String device = line.substring(0, idx);
				StatisticsData sd = new ObjectMapper().readValue(
						line.substring(idx + 1), StatisticsData.class);
				if (sd != null && device != null) {
					StatisticsData statisticsData = mapStatisticsData
							.get(device);
					if (statisticsData == null) {
						statisticsData = new StatisticsData();
						mapStatisticsData.put(device, statisticsData);
					}
					statisticsData.addStatistics(sd);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args == null || args.length != 4) {
			System.out
					.println("Usage: java com.ismartv.statistics.views.ViewJob date hive_input_path_partition temp_output_path out_hbase_table");
		}
		int exitCode = -1;
		try {
			exitCode = ToolRunner.run(new ViewJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

}
