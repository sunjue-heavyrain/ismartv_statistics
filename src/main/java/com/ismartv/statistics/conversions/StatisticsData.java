package com.ismartv.statistics.conversions;

/**
 * 统计指标
 * 
 * @Title: ismartv_statistics
 * @FileName: ViewReducer.java
 * @Description:
 * @Copyright: Copyright (c) 2008
 * @Company:
 * @author Sun Jue
 * @Create Date: 2014年9月3日
 */
class StatisticsData {
	public long allUv;
	public long allPv;

	// 历史触发播放
	public long historyAllPv;
	public long historyPlayPv;
	public long historyAllUv;
	public long historyPlayUv;

	// 看吧触发播放
	public long kanbaAllPv;
	public long kanbaPlayPv;
	public long kanbaAllUv;
	public long kanbaPlayUv;

	// 搜索影片触发播放及详情页
	public long searchAllPv;
	public long searchPlayPv;
	public long searchDetailPv;
	public long searchAllUv;
	public long searchPlayUv;
	public long searchDetailUv;

	// 详情页触发播放
	public long detailAllPv;
	public long detailPlayPv;
	public long detailAllUv;
	public long detailPlayUv;

	// 分类猜你喜欢触发播放及详情页
	public long guessAllPv;
	public long guessPlayPv;
	public long guessDetailPv;
	public long guessAllUv;
	public long guessPlayUv;
	public long guessDetailUv;

	// 列表页触发播放及详情页
	public long listAllPv;
	public long listPlayPv;
	public long listDetailPv;
	public long listAllUv;
	public long listPlayUv;
	public long listDetailUv;

	// 关联视频触发详情页
	public long relateAllPv;
	public long relateDetailPv;
	public long relatePlayPv;
	public long relateAllUv;
	public long relateDetailUv;
	public long relatePlayUv;

	// 首页预告片触发详情页
	public long trailerAllPv;
	public long trailerDetailPv;
	public long trailerAllUv;
	public long trailerDetailUv;

	// 首页推荐位触发详情页
	public long recommendedAllPv;
	public long recommendedDetailPv;
	public long recommendedAllUv;
	public long recommendedDetailUv;

	// 收藏及追剧(我的频道)触发详情页
	public long mychannelAllPv;
	public long mychannelDetailPv;
	public long mychannelAllUv;
	public long mychannelDetailUv;

	// 分裂筛选触发播放及详情页
	public long filterAllPv;
	public long filterDetailPv;
	public long filterPlayPv;
	public long filterAllUv;
	public long filterDetailUv;
	public long filterPlayUv;

	// 网络测速
	public long speedtesterPv;
	public long speedtesterUv;

	// 收藏及取消收藏
	public long collectPv;
	public long collectUv;

	// 追剧
	public long followPv;
	public long followUv;

	// 取消追剧
	public long followCancelPv;
	public long followCancelUv;

	public void addUv(UserOperation operation) {
		if (operation.historyAllUv) {
			historyAllUv++;
		}
		if (operation.historyPlayUv) {
			historyPlayUv++;
		}
		if (operation.kanbaAllUv) {
			kanbaAllUv++;
		}
		if (operation.kanbaPlayUv) {
			kanbaPlayUv++;
		}
		if (operation.searchAllUv) {
			searchAllUv++;
		}
		if (operation.searchDetailUv) {
			searchDetailUv++;
		}
		if (operation.searchPlayUv) {
			searchPlayUv++;
		}
		if (operation.detailAllUv) {
			detailAllUv++;
		}
		if (operation.detailPlayUv) {
			detailPlayUv++;
		}
		if (operation.guessAllUv) {
			guessAllUv++;
		}
		if (operation.guessDetailUv) {
			guessDetailUv++;
		}
		if (operation.guessPlayUv) {
			guessPlayUv++;
		}
		if (operation.listAllUv) {
			listAllUv++;
		}
		if (operation.listDetailUv) {
			listDetailUv++;
		}
		if (operation.listPlayUv) {
			listPlayUv++;
		}
		if (operation.relateAllUv) {
			relateAllUv++;
		}
		if (operation.relatePlayUv) {
			relatePlayUv++;
		}
		if (operation.relateDetailUv) {
			relateDetailUv++;
		}
		if (operation.trailerAllUv) {
			trailerAllUv++;
		}
		if (operation.trailerDetailUv) {
			trailerDetailUv++;
		}
		if (operation.recommendedAllUv) {
			recommendedAllUv++;
		}
		if (operation.recommendedDetailUv) {
			recommendedDetailUv++;
		}
		if (operation.mychannelAllUv) {
			mychannelAllUv++;
		}
		if (operation.mychannelDetailUv) {
			mychannelDetailUv++;
		}
		if (operation.filterAllUv) {
			filterAllUv++;
		}
		if (operation.filterDetailUv) {
			filterDetailUv++;
		}
		if (operation.filterPlayUv) {
			filterPlayUv++;
		}
		if (operation.speedtesterUv) {
			speedtesterUv++;
		}
		if (operation.collectUv) {
			collectUv++;
		}
		if (operation.followUv) {
			followUv++;
		}
		if (operation.followCancelUv) {
			followCancelUv++;
		}
	}

	public void addStatistics(StatisticsData data) {
		allPv += data.allPv;
		allUv += data.allUv;
		historyAllPv += data.historyAllPv;
		historyPlayPv += data.historyPlayPv;
		historyAllUv += data.historyAllUv;
		historyPlayUv += data.historyPlayUv;
		kanbaAllPv += data.kanbaAllPv;
		kanbaPlayPv += data.kanbaPlayPv;
		kanbaAllUv += data.kanbaAllUv;
		kanbaPlayUv += data.kanbaPlayUv;
		searchAllPv += data.searchAllPv;
		searchPlayPv += data.searchPlayPv;
		searchDetailPv += data.searchDetailPv;
		searchAllUv += data.searchAllUv;
		searchPlayUv += data.searchPlayUv;
		searchDetailUv += data.searchDetailUv;
		detailAllPv += data.detailAllPv;
		detailPlayPv += data.detailPlayPv;
		detailAllUv += data.detailAllUv;
		detailPlayUv += data.detailPlayUv;
		guessAllPv += data.guessAllPv;
		guessPlayPv += data.guessPlayPv;
		guessDetailPv += data.guessDetailPv;
		guessAllUv += data.guessAllUv;
		guessPlayUv += data.guessPlayUv;
		guessDetailUv += data.guessDetailUv;
		listAllPv += data.listAllPv;
		listPlayPv += data.listPlayPv;
		listDetailPv += data.listDetailPv;
		listAllUv += data.listAllUv;
		listPlayUv += data.listPlayUv;
		listDetailUv += data.listDetailUv;
		relateAllPv += data.relateAllPv;
		relatePlayPv += data.relatePlayPv;
		relateDetailPv += data.relateDetailPv;
		relateAllUv += data.relateAllUv;
		relatePlayUv += data.relatePlayUv;
		relateDetailUv += data.relateDetailUv;
		trailerAllPv += data.trailerAllPv;
		trailerDetailPv += data.trailerDetailPv;
		trailerAllUv += data.trailerAllUv;
		trailerDetailUv += data.trailerDetailUv;
		recommendedAllPv += data.recommendedAllPv;
		recommendedDetailPv += data.recommendedDetailPv;
		recommendedAllUv += data.recommendedAllUv;
		recommendedDetailUv += data.recommendedDetailUv;
		mychannelAllPv += data.mychannelAllPv;
		mychannelDetailPv += data.mychannelDetailPv;
		mychannelAllUv += data.mychannelAllUv;
		mychannelDetailUv += data.mychannelDetailUv;
		filterAllPv += data.filterAllPv;
		filterPlayPv += data.filterPlayPv;
		filterDetailPv += data.filterDetailPv;
		filterAllUv += data.filterAllUv;
		filterPlayUv += data.filterPlayUv;
		filterDetailUv += data.filterDetailUv;
		speedtesterPv += data.speedtesterPv;
		speedtesterUv += data.speedtesterUv;
		collectPv += data.collectPv;
		collectUv += data.collectUv;
		followPv += data.followPv;
		followUv += data.followUv;
		followCancelPv += data.followCancelPv;
		followCancelUv += data.followCancelUv;
	}
}