package com.ismartv.statistics.conversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public enum REPORT_EVENT {

	/** 进入播放历史界面 */
	EVENT_VIDEO_HISTORY_IN(REPORT_EVENT_TYPE.IN, "video_history_in", 600),
	/** 退出播放历史界面 */
	EVENT_VIDEO_HISTORY_OUT(REPORT_EVENT_TYPE.OUT, "video_history_out", 100),
	/** 轮播缓冲结束，用以标记一次轮播 */
	EVENT_LIVE_PLAY_LOAD(REPORT_EVENT_TYPE.MARK, "live_play_load", 900),
	// /** 进入轮播频道列表界面 */
	// EVENT_LIVE_CHANNELLIST_IN(REPORT_EVENT_TYPE.MARK, "live_channellist_in",
	// 600),
	// /** 退出轮播频道列表界面 */
	// EVENT_LIVE_CHANNELLIST_OUT(REPORT_EVENT_TYPE.MARK,
	// "live_channellist_out",
	// 600),
	// /** 退出轮播 */
	// EVENT_LIVE_PLAY_EXIT(REPORT_EVENT_TYPE.MARK, "live_play_exit", 600),
	/** 进入搜索 */
	EVENT_SEARCH_IN(REPORT_EVENT_TYPE.IN, "search_in", 600),
	/** 搜索结果命中 */
	EVENT_VIDEO_SEARCH_ARRIVE(REPORT_EVENT_TYPE.OPERATION,
			"video_search_arrive", 300),
	/** 搜索 */
	EVENT_SEARCH(REPORT_EVENT_TYPE.OPERATION, "search", 300),
	/** 退出搜索 */
	EVENT_SEARCH_OUT(REPORT_EVENT_TYPE.OUT, "search_out", 100),
	/** 进入媒体详情页 */
	EVENT_VIDEO_DETAIL_IN(REPORT_EVENT_TYPE.IN, "video_detail_in", 600),
	/** 退出媒体详情页 */
	EVENT_VIDEO_DETAIL_OUT(REPORT_EVENT_TYPE.OUT, "video_detail_out", 100),
	/** 进入剧集列表页 */
	EVENT_VIDEO_DRAMALIST_IN(REPORT_EVENT_TYPE.IN, "video_dramalist_in", 600),
	/** 退出剧集列表页 */
	EVENT_VIDEO_DRAMALIST_OUT(REPORT_EVENT_TYPE.OUT, "video_dramalist_out", 100),
	/** 进入收藏界面 */
	EVENT_VIDEO_COLLECT_IN(REPORT_EVENT_TYPE.IN, "video_collect_in", 600),
	/** 退出收藏界面 */
	EVENT_VIDEO_COLLECT_OUT(REPORT_EVENT_TYPE.OUT, "video_collect_out", 100),
	/** 在详情页或者播放结束页进入关联 */
	EVENT_VIDEO_RELATE(REPORT_EVENT_TYPE.OPERATION, "video_relate", 300),
	/** 进入关联界面 */
	EVENT_VIDEO_RELATE_IN(REPORT_EVENT_TYPE.IN, "video_relate_in", 600),
	/** 退出关联界面 */
	EVENT_VIDEO_RELATE_OUT(REPORT_EVENT_TYPE.OUT, "video_relate_out", 100),
	/** 用户点击某个预告片 */
	EVENT_LAUNCHER_VOD_CLICK_TRAILER(REPORT_EVENT_TYPE.OPERATION,
			"launcher_vod_click", 300, "type\0002trailer\000212"),
	/** 用户点击某个推荐影片 */
	EVENT_LAUNCHER_VOD_CLICK_ITEM(REPORT_EVENT_TYPE.OPERATION,
			"launcher_vod_click", 300, "type\0002item\000212"),
	/** 用户点击看吧 */
	EVENT_LAUNCHER_VOD_CLICK_KANBA(REPORT_EVENT_TYPE.OPERATION,
			"launcher_vod_click", 300,
			"type\0002section\000212\0001title\0002\u770b\u5427\000210"),
	/** 用户点击我的频道 */
	EVENT_LAUNCHER_VOD_CLICK_MYCHANNEL(REPORT_EVENT_TYPE.OPERATION,
			"launcher_vod_click", 300,
			"type\0002section\000212\0001title\0002\u6211\u7684\u9891\u9053\000210"),
	/** 用户点击首页分类(即频道) */
	EVENT_LAUNCHER_VOD_CLICK(REPORT_EVENT_TYPE.OPERATION,
			"launcher_vod_click", 300,
			"type\0002section\000212"),
	/** 用户点击播放记录 */
	EVENT_LAUNCHER_VOD_CLICK_HISTORY(REPORT_EVENT_TYPE.OPERATION,
			"launcher_vod_click", 300,
			"type\0002section\000212\0001title\0002\u64ad\u653e\u8bb0\u5f55\000210"),
	/** 进入分类猜你喜欢浏览 */
	EVENT_VIDEO_CATEGORY_GUESS_IN(REPORT_EVENT_TYPE.IN, "video_category_in",
			600, "title\0002\u731c\u4f60\u559c\u6b22\000210"),
	/** 退出分类猜你喜欢浏览 */
	EVENT_VIDEO_CATEGORY_GUESS_OUT(REPORT_EVENT_TYPE.OUT, "video_category_out",
			100, "title\0002\u731c\u4f60\u559c\u6b22\000210"),
	/** 进入分类浏览 */
	EVENT_VIDEO_CATEGORY_IN(REPORT_EVENT_TYPE.IN, "video_category_in", 600),
	/** 退出分类浏览 */
	EVENT_VIDEO_CATEGORY_OUT(REPORT_EVENT_TYPE.OUT, "video_category_out", 100),
	/** 进入某视频频道 */
	EVENT_VIDEO_CHANNEL_IN(REPORT_EVENT_TYPE.IN, "video_channel_in", 600),
	/** 退出某视频频道 */
	EVENT_VIDEO_CHANNEL_OUT(REPORT_EVENT_TYPE.OUT, "video_channel_out", 100),
	/** 进入分类筛选 */
	EVENT_VIDEO_FILTER_IN(REPORT_EVENT_TYPE.IN, "video_filter_in", 600),
	/** 分类筛选 */
	EVENT_VIDEO_FILTER(REPORT_EVENT_TYPE.OPERATION, "video_filter", 300),
	/** 退出分类筛选 */
	EVENT_VIDEO_FILTER_OUT(REPORT_EVENT_TYPE.OUT, "video_filter_out", 100),
	/** 网络测速 */
	EVENT_SPEED_TESTER(REPORT_EVENT_TYPE.OPERATION, "app_exit", 300,
			"code\0002cn.ismartv.speedtester\000211"),
	/** 收藏 */
	EVENT_VIDEO_COLLECT(REPORT_EVENT_TYPE.OPERATION, "video_collect", 300),
	/** 追剧 */
	EVENT_VIDEO_FOLLOW_ADD(REPORT_EVENT_TYPE.OPERATION, "video_follow", 300,
			"flag\0002add\000211"),
	/** 取消追剧 */
	EVENT_VIDEO_FOLLOW_CANCEL(REPORT_EVENT_TYPE.OPERATION, "video_follow", 300,
			"flag\0002cancel\000211"),
	/** 播放器打开(不在详情页) */
	EVENT_VIDEO_START(REPORT_EVENT_TYPE.MARK, "video_start", 700,
			"location\0002\000216\0002true\0002false"),
	/** 开始播放缓冲结束(用以标记开始播放) */
	EVENT_VIDEO_PLAY_LOAD(REPORT_EVENT_TYPE.MARK, "video_play_load", 701),
	/** 未知 */
	EVENT_UNKNOWN(REPORT_EVENT_TYPE.UNKNOWN, "");

	private REPORT_EVENT_TYPE type = REPORT_EVENT_TYPE.UNKNOWN;
	private String event = "".intern();
	private List<Attr> attrs = Collections.EMPTY_LIST;
	private int millisSecond = 0;

	private REPORT_EVENT(REPORT_EVENT_TYPE type, String event) {
		this.type = type;
		this.event = event.intern();
	}

	private REPORT_EVENT(REPORT_EVENT_TYPE type, String event, int ms) {
		this(type, event);
		this.millisSecond = ms;
	}

	private REPORT_EVENT(REPORT_EVENT_TYPE type, String event, int ms,
			String attrs) {
		this(type, event, ms);
		this.attrs = Attr.parse(attrs);
	}

	private REPORT_EVENT(REPORT_EVENT_TYPE type, String event, int ms,
			List<Attr> attrs) {
		this(type, event, ms);
		this.attrs = attrs;
	}

	/**
	 * @return the type
	 */
	public REPORT_EVENT_TYPE getType() {
		return type;
	}

	/**
	 * @return the event
	 */
	public String getEvent() {
		return event;
	}

	/**
	 * @return the millisSecond
	 */
	public int getMillisSecond() {
		return millisSecond;
	}

	private static final HashMap<String, List<REPORT_EVENT>> EVENT_MAP = new HashMap<String, List<REPORT_EVENT>>();
	static {
		for (REPORT_EVENT event : REPORT_EVENT.values()) {
			List<REPORT_EVENT> lst = EVENT_MAP.get(event.event);
			if (lst == null) {
				lst = new LinkedList<REPORT_EVENT>();
				EVENT_MAP.put(event.event, lst);
			}
			lst.add(event);
		}
	}

	public static REPORT_EVENT getEventsByEvent(String event, String[] fields) {
		event = event.intern();
		// REPORT_EVENT[] values = REPORT_EVENT.values();
		List<REPORT_EVENT> values = EVENT_MAP.get(event);
		if (values == null || values.isEmpty()) {
			return EVENT_UNKNOWN;
		}
		for (REPORT_EVENT currEvent : values) {
			if (currEvent.event == event) {
				if (currEvent.attrs.isEmpty()) {
					return currEvent;
				} else {
					boolean tf = true;
					for (int i = 0; tf && i < currEvent.attrs.size(); i++) {
						Attr attr = currEvent.attrs.get(i);
						String fieldValue = attr.index < fields.length ? fields[attr.index]
								.intern() : "".intern();
						if (attr.isNotAttr) {
							if (!fieldValue.isEmpty()) {
								tf = false;
							}
						} else {
							if (attr.isNotValue) {
								if (attr.value == fieldValue) {
									tf = false;
								}
							} else {
								if (attr.value != fieldValue) {
									tf = false;
								}
							}
						}
					}
					if (tf) {
						return currEvent;
					}
				}
			}
		}
		return EVENT_UNKNOWN;
	}

	private static class Attr {
		public String name;
		public String value;
		public int index;
		public boolean isNotAttr;
		public boolean isNotValue;

		public Attr(String name, String value, int index, boolean isNotAttr,
				boolean isNotValue) {
			this.name = name.intern();
			this.value = value.intern();
			this.index = index;
			this.isNotAttr = isNotAttr;
			this.isNotValue = isNotValue;
		}

		public static List<Attr> parse(String string) {
			if (string == null || string.isEmpty()) {
				return Collections.EMPTY_LIST;
			}
			String[] attrs = string.split("\0001");
			ArrayList<Attr> lst = new ArrayList<Attr>(attrs.length);
			int index = 0;
			for (String strAttr : attrs) {
				String[] fields = strAttr.split("\0002");
				if (fields.length < 3 || fields.length > 5) {
					continue;
				}
				index = Integer.parseInt(fields[2]);
				if (index >= 0) {
					switch (fields.length) {
					case 3:
						lst.add(new Attr(fields[0], fields[1], index, false,
								false));
						break;
					case 4:
						lst.add(new Attr(fields[0], fields[1], index, Boolean
								.parseBoolean(fields[3].toLowerCase()), false));
						break;
					case 5:
						lst.add(new Attr(fields[0], fields[1], index, Boolean
								.parseBoolean(fields[3].toLowerCase()), Boolean
								.parseBoolean(fields[4].toLowerCase())));
						break;

					default:
						break;
					}
				}
			}
			return lst;
		}
	}

}
