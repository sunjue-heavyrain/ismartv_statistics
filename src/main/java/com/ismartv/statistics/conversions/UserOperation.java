package com.ismartv.statistics.conversions;

/**
 * 单个用户的统计，即UV
 * 
 * @Title: ismartv_statistics
 * @FileName: ViewReducer.java
 * @Description:
 * @Copyright: Copyright (c) 2008
 * @Company:
 * @author Sun Jue
 * @Create Date: 2014年9月3日
 */
class UserOperation {
	// 历史触发播放
	public boolean historyAllUv = false;
	public boolean historyPlayUv = false;

	// 看吧触发播放
	public boolean kanbaAllUv = false;
	public boolean kanbaPlayUv = false;

	// 搜索影片触发播放及详情页
	public boolean searchAllUv = false;
	public boolean searchPlayUv = false;
	public boolean searchDetailUv = false;

	// 详情页触发播放
	public boolean detailAllUv = false;
	public boolean detailPlayUv = false;

	// 列表页触发播放及详情页
	public boolean listAllUv = false;
	public boolean listPlayUv = false;
	public boolean listDetailUv = false;

	// 分类猜你喜欢触发播放及详情页
	public boolean guessAllUv = false;
	public boolean guessPlayUv = false;
	public boolean guessDetailUv = false;

	// 关联视频触发播放及详情页
	public boolean relateAllUv = false;
	public boolean relatePlayUv = false;
	public boolean relateDetailUv = false;

	// 首页预告片触发详情页
	public boolean trailerAllUv = false;
	public boolean trailerDetailUv = false;

	// 首页推荐位触发详情页
	public boolean recommendedAllUv = false;
	public boolean recommendedDetailUv = false;

	// 收藏及追剧(我的频道)触发详情页
	public boolean mychannelAllUv = false;
	public boolean mychannelDetailUv = false;

	// 分裂筛选触发播放及详情页
	public boolean filterAllUv = false;
	public boolean filterPlayUv = false;
	public boolean filterDetailUv = false;

	// 网络测速
	public boolean speedtesterUv = false;

	// 收藏及取消收藏
	public boolean collectUv = false;

	// 追剧
	public boolean followUv = false;

	// 取消追剧
	public boolean followCancelUv = false;
}