from abc import ABC, abstractmethod
from typing import Iterable

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


class ContentRepositoryPort(ABC):
    @abstractmethod
    def upsert_channel(self, channel: Channel) -> Channel:
        raise NotImplementedError

    @abstractmethod
    def upsert_account(self, account: CreatorAccount) -> CreatorAccount:
        raise NotImplementedError

    @abstractmethod
    def upsert_video(self, video: Video) -> Video:
        raise NotImplementedError

    @abstractmethod
    def upsert_comments(self, comments: Iterable[VideoComment]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert_video_sentiment(self, sentiment: VideoSentiment) -> VideoSentiment:
        raise NotImplementedError

    @abstractmethod
    def upsert_comment_sentiments(self, sentiments: Iterable[CommentSentiment]) -> None:
        raise NotImplementedError

    @abstractmethod
    def upsert_keyword_trend(self, trend: KeywordTrend) -> KeywordTrend:
        raise NotImplementedError

    @abstractmethod
    def upsert_keyword_mapping(self, mapping: KeywordMapping) -> KeywordMapping:
        raise NotImplementedError

    @abstractmethod
    def upsert_video_score(self, score: VideoScore) -> VideoScore:
        raise NotImplementedError

    @abstractmethod
    def log_crawl(self, log: CrawlLog) -> CrawlLog:
        raise NotImplementedError
