from typing import Iterable

from config.database.session import SessionLocal
from content.application.port.content_repository_port import ContentRepositoryPort
from content.domain.channel import Channel
from content.domain.comment_sentiment import CommentSentiment
from content.domain.crawl_log import CrawlLog
from content.domain.creator_account import CreatorAccount
from content.domain.keyword_mapping import KeywordMapping
from content.domain.keyword_trend import KeywordTrend
from content.domain.video import Video
from content.domain.video_comment import VideoComment
from content.domain.video_score import VideoScore
from content.domain.video_sentiment import VideoSentiment
from content.infrastructure.orm.models import (
    ChannelORM,
    CreatorAccountORM,
    VideoORM,
    VideoCommentORM,
    VideoSentimentORM,
    CommentSentimentORM,
    KeywordTrendORM,
    KeywordMappingORM,
    VideoScoreORM,
    CrawlLogORM,
)


class ContentRepositoryImpl(ContentRepositoryPort):
    def __init__(self):
        self.db = SessionLocal()

    def upsert_channel(self, channel: Channel) -> Channel:
        orm = self.db.get(ChannelORM, channel.channel_id)
        if orm is None:
            orm = ChannelORM(channel_id=channel.channel_id)
            self.db.add(orm)
            # 한국어 주석: 최초 적재 시 정적 메타데이터 전체를 저장합니다.
            orm.platform = channel.platform or "youtube"
            orm.title = channel.title
            orm.description = channel.description
            orm.country = channel.country
            orm.created_at = channel.created_at
        # 한국어 주석: 기존 레코드는 변동성 필드(구독/조회/영상 수, 수집시각)만 최신화합니다.
        orm.subscriber_count = channel.subscriber_count
        orm.view_count = channel.view_count
        orm.video_count = channel.video_count
        orm.crawled_at = channel.crawled_at

        self.db.commit()
        return channel

    def upsert_account(self, account: CreatorAccount) -> CreatorAccount:
        orm = (
            self.db.query(CreatorAccountORM)
            .filter(
                CreatorAccountORM.account_id == account.account_id,
                CreatorAccountORM.platform == account.platform,
            )
            .one_or_none()
        )
        if orm is None:
            orm = CreatorAccountORM(account_id=account.account_id, platform=account.platform)
            self.db.add(orm)
            # 한국어 주석: 최초 적재 시 정적 프로필 정보를 저장합니다.
            orm.display_name = account.display_name
            orm.username = account.username
            orm.profile_url = account.profile_url
            orm.description = account.description
            orm.country = account.country
        # 한국어 주석: 변동성 필드(팔로워/게시물 수, 최신 업데이트 시간)만 갱신합니다.
        orm.follower_count = account.follower_count
        orm.post_count = account.post_count
        orm.last_updated_at = account.last_updated_at
        orm.crawled_at = account.crawled_at
        self.db.commit()
        return account

    def upsert_video(self, video: Video) -> Video:
        orm = self.db.get(VideoORM, video.video_id)
        if orm is None:
            orm = VideoORM(video_id=video.video_id)
            self.db.add(orm)
            # 한국어 주석: 최초 적재 시 정적 메타데이터를 저장하여 이후 변동성 업데이트와 분리합니다.
            orm.platform = video.platform or "youtube"
            orm.channel_id = video.channel_id
            orm.title = video.title
            orm.description = video.description
            orm.tags = video.tags
            orm.category_id = video.category_id
            orm.published_at = video.published_at
            orm.duration = video.duration
            orm.thumbnail_url = video.thumbnail_url
        # 한국어 주석: 기존 레코드는 변동성 필드(조회/좋아요/댓글 수, 최신 수집시각)만 갱신합니다.
        orm.view_count = video.view_count
        orm.like_count = video.like_count
        orm.comment_count = video.comment_count
        orm.crawled_at = video.crawled_at

        self.db.commit()
        return video

    def upsert_comments(self, comments: Iterable[VideoComment]) -> None:
        for comment in comments:
            orm = self.db.get(VideoCommentORM, comment.comment_id)
            if orm is None:
                orm = VideoCommentORM(comment_id=comment.comment_id)
                self.db.add(orm)
                # 한국어 주석: 최초 적재 시 댓글 본문/작성자 등 정적 정보를 저장합니다.
                orm.platform = comment.platform or "youtube"
                orm.video_id = comment.video_id
                orm.author = comment.author
                orm.content = comment.content
                orm.published_at = comment.published_at
            # 한국어 주석: 기존 댓글은 변동성 필드(좋아요 수)만 업데이트합니다.
            orm.like_count = comment.like_count
        self.db.commit()

    def upsert_video_sentiment(self, sentiment: VideoSentiment) -> VideoSentiment:
        orm = self.db.get(VideoSentimentORM, sentiment.video_id)
        if orm is None:
            orm = VideoSentimentORM(video_id=sentiment.video_id)
            self.db.add(orm)
        orm.platform = sentiment.platform or "youtube"
        orm.category = sentiment.category
        orm.trend_score = sentiment.trend_score
        orm.sentiment_label = sentiment.sentiment_label
        orm.sentiment_score = sentiment.sentiment_score
        orm.keywords = sentiment.keywords
        orm.summary = sentiment.summary
        orm.analyzed_at = sentiment.analyzed_at
        self.db.commit()
        return sentiment

    def upsert_comment_sentiments(self, sentiments: Iterable[CommentSentiment]) -> None:
        for sentiment in sentiments:
            orm = self.db.get(CommentSentimentORM, sentiment.comment_id)
            if orm is None:
                orm = CommentSentimentORM(comment_id=sentiment.comment_id)
                self.db.add(orm)
            orm.platform = sentiment.platform or "youtube"
            orm.sentiment_label = sentiment.sentiment_label
            orm.sentiment_score = sentiment.sentiment_score
            orm.analyzed_at = sentiment.analyzed_at
        self.db.commit()

    def upsert_keyword_trend(self, trend: KeywordTrend) -> KeywordTrend:
        orm = (
            self.db.query(KeywordTrendORM)
            .filter(
                KeywordTrendORM.keyword == trend.keyword,
                KeywordTrendORM.date == trend.date,
                KeywordTrendORM.platform == trend.platform,
            )
            .one_or_none()
        )
        if orm is None:
            orm = KeywordTrendORM(
                keyword=trend.keyword, date=trend.date, platform=trend.platform
            )
            self.db.add(orm)
        orm.search_volume = trend.search_volume
        self.db.commit()
        return trend

    def upsert_keyword_mapping(self, mapping: KeywordMapping) -> KeywordMapping:
        orm = None
        if mapping.mapping_id:
            orm = self.db.get(KeywordMappingORM, mapping.mapping_id)
        if orm is None:
            orm = KeywordMappingORM()
            self.db.add(orm)
        orm.platform = mapping.platform or "youtube"
        orm.video_id = mapping.video_id
        orm.channel_id = mapping.channel_id
        orm.keyword = mapping.keyword
        orm.weight = mapping.weight
        self.db.commit()
        mapping.mapping_id = orm.mapping_id
        return mapping

    def upsert_video_score(self, score: VideoScore) -> VideoScore:
        orm = self.db.get(VideoScoreORM, score.video_id)
        if orm is None:
            orm = VideoScoreORM(video_id=score.video_id)
            self.db.add(orm)
        orm.platform = score.platform or "youtube"
        orm.engagement_score = score.engagement_score
        orm.sentiment_score = score.sentiment_score
        orm.trend_score = score.trend_score
        orm.total_score = score.total_score
        orm.updated_at = score.updated_at
        self.db.commit()
        return score

    def log_crawl(self, log: CrawlLog) -> CrawlLog:
        orm = CrawlLogORM(
            target_type=log.target_type,
            target_id=log.target_id,
            status=log.status,
            message=log.message,
            crawled_at=log.crawled_at,
        )
        self.db.add(orm)
        self.db.commit()
        log.id = orm.id
        return log
