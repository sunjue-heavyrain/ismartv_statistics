package com.ismartv.statistics.conversions;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;

public class ViewReducer extends Reducer<TextPair, Text, Text, NullWritable> {

	private HashMap<String, StatisticsData> mapStatisticsData = new HashMap<String, StatisticsData>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);

		for (Entry<String, StatisticsData> entry : mapStatisticsData.entrySet()) {
			String device = entry.getKey();
			StatisticsData statisticsData = entry.getValue();

			statisticsData.allPv += statisticsData.historyAllPv;
			statisticsData.allPv += statisticsData.kanbaAllPv;
			statisticsData.allPv += statisticsData.searchAllPv;
			statisticsData.allPv += statisticsData.detailAllPv;
			statisticsData.allPv += statisticsData.listAllPv;
			statisticsData.allPv += statisticsData.relateAllPv;
			statisticsData.allPv += statisticsData.trailerAllPv;
			statisticsData.allPv += statisticsData.recommendedAllPv;
			statisticsData.allPv += statisticsData.mychannelAllPv;
			statisticsData.allPv += statisticsData.filterAllPv;
			statisticsData.allPv += statisticsData.speedtesterPv;
			statisticsData.allPv += statisticsData.collectPv;
			statisticsData.allPv += statisticsData.followCancelPv;
			statisticsData.allPv += statisticsData.followPv;
			// 分类猜你喜欢统计数据已计入列表统计数据
			// 所以这里不要把分类猜你喜欢统计数据计入总计数据

			StringWriter buf = new StringWriter();
			// String json = new
			// ObjectMapper().writeValueAsString(statisticsData);
			new ObjectMapper().writeValue(buf, statisticsData);
			context.write(new Text(device + "\u0001" + buf.toString()),
					NullWritable.get());
		}
	}

	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String keyFirst = key.getFirst().toString();
		int idx = keyFirst.indexOf('\u0001');
		if (idx <= 0) {
			return;
		}
		String device = keyFirst.substring(0, idx);
		// String sn = keyFirst.substring(idx + 1);

		REPORT_EVENT lastEvent = null;

		StatisticsData statisticsData = mapStatisticsData.get(device);
		if (statisticsData == null) {
			statisticsData = new StatisticsData();
			mapStatisticsData.put(device, statisticsData);
		}

		statisticsData.allUv++;

		UserOperation userOperation = new UserOperation();

		// 是否处于“猜你喜欢”
		boolean isInGuess = false;

		for (Text text : values) {
			// String ts = key.getSecond().toString();
			REPORT_EVENT rEvent = REPORT_EVENT.valueOf(text.toString());

			if (rEvent.getType() == REPORT_EVENT_TYPE.IN) {
				// 是否点击“猜你喜欢”section
				boolean isGuessIn = false;
				switch (rEvent) {
				case EVENT_VIDEO_DETAIL_IN:
					// 详情页进入事件，即触发详情页的相关统计和详情页触发播放的统计
					statisticsData.detailAllPv++;
					userOperation.detailAllUv = true;
					switch (lastEvent) {
					case EVENT_LAUNCHER_VOD_CLICK_MYCHANNEL:
						// 我的频道，即收藏和追剧触发详情页
						statisticsData.mychannelDetailPv++;
						userOperation.mychannelDetailUv = true;
						break;
					case EVENT_SEARCH_IN:
					case EVENT_VIDEO_SEARCH_ARRIVE:
					case EVENT_SEARCH:
						// 搜索触发详情页
						statisticsData.searchDetailPv++;
						userOperation.searchDetailUv = true;
						break;
					case EVENT_VIDEO_RELATE:
					case EVENT_VIDEO_RELATE_IN:
						// 关联视频触发详情页
						statisticsData.relateDetailPv++;
						userOperation.relateDetailUv = true;
						break;
					case EVENT_LAUNCHER_VOD_CLICK_TRAILER:
						// 首页预告片触发详情页
						statisticsData.trailerDetailPv++;
						userOperation.trailerDetailUv = true;
						break;
					case EVENT_LAUNCHER_VOD_CLICK_ITEM:
						// 首页推荐位触发详情页
						statisticsData.recommendedDetailPv++;
						userOperation.recommendedDetailUv = true;
						break;
					case EVENT_VIDEO_CATEGORY_GUESS_IN:
						// 分类猜你喜欢触发详情页
						statisticsData.guessDetailPv++;
						userOperation.guessDetailUv = true;
						// 分类猜你喜欢的统计数据同时计入列表统计数据
						// 因此不要break
					case EVENT_VIDEO_CATEGORY_IN:
					case EVENT_VIDEO_CHANNEL_IN:
						// 列表页触发详情页
						statisticsData.listDetailPv++;
						userOperation.listDetailUv = true;
						break;
					case EVENT_VIDEO_FILTER_IN:
					case EVENT_VIDEO_FILTER:
						statisticsData.filterDetailPv++;
						userOperation.filterDetailUv = true;
						break;
					default:
						break;
					}
					// end switch lastEvent
					break;
				// end EVENT_VIDEO_DETAIL_IN
				case EVENT_VIDEO_HISTORY_IN:
					// 进入播放记录，即历史
					statisticsData.historyAllPv++;
					userOperation.historyAllUv = true;
					break;
				case EVENT_SEARCH_IN:
					// 进入搜索页
					statisticsData.searchAllPv++;
					userOperation.searchAllUv = true;
					break;
				case EVENT_VIDEO_DRAMALIST_IN:
					// 进入剧集列表
					statisticsData.detailAllPv++;
					userOperation.detailAllUv = true;
					break;
				case EVENT_VIDEO_RELATE:
					// 点击关联视频
				case EVENT_VIDEO_RELATE_IN:
					// 进入关联视频更多页面
					statisticsData.relateAllPv++;
					userOperation.relateAllUv = true;
					break;
				case EVENT_VIDEO_CATEGORY_GUESS_IN:
					// 进入分类猜你喜欢
					statisticsData.guessAllPv++;
					userOperation.guessAllUv = true;
					// 分类猜你喜欢的统计数据同时计入列表统计数据
					// 因此不要break

					// 进入“猜你喜欢”
					isInGuess = true;
					// 点击“猜你喜欢”section
					isGuessIn = true;
				case EVENT_VIDEO_CATEGORY_IN:
					// 进入分类列表

					if (!isGuessIn) {
						// 点击的不是“猜你喜欢”section，则不在“猜你喜欢”section
						isInGuess = false;
					}
				case EVENT_VIDEO_CHANNEL_IN:
					// 进入频道列表
					statisticsData.listAllPv++;
					userOperation.listAllUv = true;
					if (isInGuess
							&& lastEvent != REPORT_EVENT.EVENT_LAUNCHER_VOD_CLICK) {
						// 如果最后一次在列表页时是处于“猜你喜欢”section，
						// 并且不是从首页点击进入列表页，则表示是从其他模块(如详情页)进入列表页
						// 则把当前事件模拟为进入“猜你喜欢”section事件
						if (rEvent != REPORT_EVENT.EVENT_VIDEO_CATEGORY_GUESS_IN) {
							// 如果当前事件不是进入分类猜你喜欢
							// 则把当前事件模拟为进入“猜你喜欢”section事件，并累加“猜你喜欢”的计数
							rEvent = REPORT_EVENT.EVENT_VIDEO_CATEGORY_GUESS_IN;
							statisticsData.guessAllPv++;
							userOperation.guessAllUv = true;
						}
					} else {
						// 从首页点击进入列表页，默认section不为“猜你喜欢”section
						// 最后一次在列表页时不是处于“猜你喜欢”section，则当前section不为“猜你喜欢”section
						isInGuess = false;
					}
					break;
				case EVENT_VIDEO_FILTER_IN:
					// 进入分类过滤
					statisticsData.filterAllPv++;
					userOperation.filterAllUv = true;
					break;
				default:
					break;
				}
				// 进入类事件，需要保存事件以作为下一事件的入口
				lastEvent = rEvent;
			} else if (rEvent.getType() == REPORT_EVENT_TYPE.OUT) {
				// switch (rEvent) {
				// case EVENT_VIDEO_HISTORY_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_HISTORY_IN) {
				// continue;
				// }
				// break;
				// case EVENT_SEARCH_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_SEARCH_IN) {
				// continue;
				// }
				// break;
				// case EVENT_VIDEO_DETAIL_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_DETAIL_IN) {
				// continue;
				// }
				// break;
				// case EVENT_VIDEO_DRAMALIST_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_DRAMALIST_IN) {
				// continue;
				// }
				// break;
				// case EVENT_VIDEO_COLLECT_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_COLLECT_IN) {
				// continue;
				// }
				// break;
				// case EVENT_VIDEO_RELATE_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_RELATE_IN) {
				// continue;
				// }
				// break;
				// case EVENT_VIDEO_CATEGORY_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_CATEGORY_IN) {
				// continue;
				// }
				// break;
				// case EVENT_VIDEO_CHANNEL_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_CHANNEL_IN) {
				// continue;
				// }
				// break;
				// case EVENT_VIDEO_FILTER_OUT:
				// if (lastEvent != REPORT_EVENT.EVENT_VIDEO_FILTER_IN) {
				// continue;
				// }
				// break;
				// default:
				// continue;
				// }
				// lastEvent = REPORT_EVENT.EVENT_UNKNOWN;
			} else if (rEvent.getType() == REPORT_EVENT_TYPE.OPERATION) {
				switch (rEvent) {
				case EVENT_SPEED_TESTER:
					// 网络测速
					statisticsData.speedtesterPv++;
					userOperation.speedtesterUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_VIDEO_COLLECT:
					// 视频收藏和取消收藏
					statisticsData.collectPv++;
					userOperation.collectUv = true;
					break;
				case EVENT_VIDEO_FILTER:
					// 点击分类搜索
					statisticsData.filterAllPv++;
					userOperation.filterAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_VIDEO_FOLLOW_ADD:
					// 追剧
					statisticsData.followPv++;
					userOperation.followUv = true;
					break;
				case EVENT_VIDEO_FOLLOW_CANCEL:
					// 取消追剧
					statisticsData.followCancelPv++;
					userOperation.followCancelUv = true;
					break;
				case EVENT_VIDEO_SEARCH_ARRIVE:
					// 点击搜索命中的视频
					statisticsData.searchAllPv++;
					userOperation.searchAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_VIDEO_RELATE:
					// 点击关联视频
					statisticsData.relateAllPv++;
					userOperation.relateAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_LAUNCHER_VOD_CLICK_TRAILER:
					// 点击首页预告片
					statisticsData.trailerAllPv++;
					userOperation.trailerAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_LAUNCHER_VOD_CLICK_ITEM:
					// 点击首页推荐视频
					statisticsData.recommendedAllPv++;
					userOperation.recommendedAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_LAUNCHER_VOD_CLICK_KANBA:
					// 点击看吧
					statisticsData.kanbaAllPv++;
					userOperation.kanbaAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_LAUNCHER_VOD_CLICK_MYCHANNEL:
					// 点击我的频道
					statisticsData.mychannelAllPv++;
					userOperation.mychannelAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_LAUNCHER_VOD_CLICK_HISTORY:
					// 点击播放记录
					statisticsData.historyAllPv++;
					userOperation.historyAllUv = true;
					lastEvent = rEvent;
					break;
				case EVENT_LAUNCHER_VOD_CLICK:
					// 标记从首页点击频道进入分类
					lastEvent = rEvent;
				default:
					break;
				}
			} else if (rEvent.getType() == REPORT_EVENT_TYPE.MARK) {
				if (rEvent == REPORT_EVENT.EVENT_VIDEO_PLAY_LOAD) {
					// 开始播放视频
					switch (lastEvent) {
					case EVENT_SPEED_TESTER:
						// 从网络测速返回，不需要再统计一次
						break;
					case EVENT_VIDEO_HISTORY_IN:
						// 历史触发播放
					case EVENT_LAUNCHER_VOD_CLICK_HISTORY:
						// 播放记录(即播放历史)触发播放
						statisticsData.historyPlayPv++;
						userOperation.historyPlayUv = true;
						break;
					case EVENT_VIDEO_RELATE:
					case EVENT_VIDEO_RELATE_IN:
						// 关联视频触发播放
						statisticsData.relatePlayPv++;
						userOperation.relatePlayUv = true;
						break;
					case EVENT_VIDEO_CATEGORY_GUESS_IN:
						// 分类猜你喜欢触发播放
						statisticsData.guessPlayPv++;
						userOperation.guessPlayUv = true;
						// 分类猜你喜欢的统计数据同时计入列表统计数据
						// 因此不要break
					case EVENT_VIDEO_CATEGORY_IN:
						// 分类列表触发播放
					case EVENT_VIDEO_CHANNEL_IN:
						// 频道列表进入后未切换分类，触发播放
						statisticsData.listPlayPv++;
						userOperation.listPlayUv = true;
						break;
					case EVENT_VIDEO_DRAMALIST_IN:
						// 详情页-剧集列表触发播放
					case EVENT_VIDEO_DETAIL_IN:
						// 详情页触发播放
						statisticsData.detailPlayPv++;
						userOperation.detailPlayUv = true;
						break;
					case EVENT_SEARCH_IN:
						// 搜索触发播放
					case EVENT_VIDEO_SEARCH_ARRIVE:
						// 点击搜索后有命中视频触发播放
					case EVENT_SEARCH:
						// 点击搜索后触发民众
						statisticsData.searchPlayPv++;
						userOperation.searchPlayUv = true;
						break;
					case EVENT_VIDEO_FILTER_IN:
						// 进入分类过滤
					case EVENT_VIDEO_FILTER:
						// TODO 分类筛选触发播放
						statisticsData.filterPlayPv++;
						userOperation.filterPlayUv = true;
						break;
					default:
						// 无来源播放事件，如来源在零点之前播放在零点之后
						statisticsData.allPv++;
						break;
					}
				} else if (rEvent == REPORT_EVENT.EVENT_LIVE_PLAY_LOAD) {
					// 看吧 播放视频
					if (lastEvent == REPORT_EVENT.EVENT_LAUNCHER_VOD_CLICK_KANBA) {
						// 首页点击看吧进入看吧轮播
						statisticsData.kanbaPlayPv++;
						userOperation.kanbaPlayUv = true;
					}
				}
			}
		}

		statisticsData.addUv(userOperation);

		allDetailPlayPv(statisticsData);
	}

	private void allDetailPlayPv(StatisticsData statisticsData) {
		statisticsData.historyAllPv += statisticsData.historyPlayPv;
		statisticsData.kanbaAllPv += statisticsData.kanbaPlayPv;
		statisticsData.searchAllPv += statisticsData.searchPlayPv;
		statisticsData.searchAllPv += statisticsData.searchDetailPv;
		statisticsData.detailAllPv += statisticsData.detailPlayPv;
		statisticsData.guessAllPv += statisticsData.guessPlayPv;
		statisticsData.guessAllPv += statisticsData.guessDetailPv;
		statisticsData.listAllPv += statisticsData.listPlayPv;
		statisticsData.listAllPv += statisticsData.listDetailPv;
		statisticsData.relateAllPv += statisticsData.relateDetailPv;
		statisticsData.relateAllPv += statisticsData.relatePlayPv;
		statisticsData.trailerAllPv += statisticsData.trailerDetailPv;
		statisticsData.recommendedAllPv += statisticsData.recommendedDetailPv;
		statisticsData.mychannelAllPv += statisticsData.mychannelDetailPv;
		statisticsData.filterAllPv += statisticsData.filterDetailPv;
		statisticsData.filterAllPv += statisticsData.filterPlayPv;
	}

}
