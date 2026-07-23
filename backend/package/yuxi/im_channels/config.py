"""IM 渠道配置加载。

从环境变量读取,遵循 .env 优先级。所有 IM_* 配置项集中在此,
im-worker 与 api-dev 共享同一份配置读取逻辑。
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeishuConfig:
    enabled: bool = False
    app_id: str = ""
    app_secret: str = ""
    domain: str = "https://open.feishu.cn"
    max_text_length: int = 30000


@dataclass(frozen=True)
class DingtalkConfig:
    enabled: bool = False
    app_key: str = ""
    app_secret: str = ""
    robot_code: str = ""
    max_text_length: int = 5000


@dataclass(frozen=True)
class IMConfig:
    enabled: bool = False
    max_concurrency: int = 5
    http_timeout_seconds: int = 30
    sse_idle_timeout_seconds: int = 300
    sse_reconnect_interval: int = 5
    run_total_timeout_seconds: int = 600
    stream_throttle_seconds: float = 0.5
    default_agent_slug: str = "default-chatbot"
    default_department: str = "IM用户"
    user_resolve_rate_limit: int = 30
    outputs_host_path_template: str = "/app/saves/threads/{thread_id}/user-data/outputs"
    feishu: FeishuConfig = field(default_factory=FeishuConfig)
    dingtalk: DingtalkConfig = field(default_factory=DingtalkConfig)


def _env_bool(key: str, default: bool = False) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in ("true", "1", "yes")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


def load_im_config() -> IMConfig:
    """从环境变量加载 IM 配置。

    启用但缺凭证时强制 enabled=False 并告警,避免 im-worker 启动后反复重试。
    """
    feishu_enabled = _env_bool("IM_FEISHU_ENABLED")
    feishu_app_id = os.getenv("IM_FEISHU_APP_ID", "").strip()
    feishu_app_secret = os.getenv("IM_FEISHU_APP_SECRET", "").strip()
    if feishu_enabled and not (feishu_app_id and feishu_app_secret):
        logger.warning("[IM] Feishu enabled but missing app_id/app_secret, force disabled")
        feishu_enabled = False

    dingtalk_enabled = _env_bool("IM_DINGTALK_ENABLED")
    dingtalk_app_key = os.getenv("IM_DINGTALK_APP_KEY", "").strip()
    dingtalk_app_secret = os.getenv("IM_DINGTALK_APP_SECRET", "").strip()
    if dingtalk_enabled and not (dingtalk_app_key and dingtalk_app_secret):
        logger.warning("[IM] Dingtalk enabled but missing app_key/app_secret, force disabled")
        dingtalk_enabled = False

    return IMConfig(
        enabled=_env_bool("IM_WORKER_ENABLED"),
        max_concurrency=_env_int("IM_MAX_CONCURRENCY", 5),
        http_timeout_seconds=_env_int("IM_HTTP_TIMEOUT_SECONDS", 30),
        sse_idle_timeout_seconds=_env_int("IM_SSE_IDLE_TIMEOUT_SECONDS", 300),
        sse_reconnect_interval=_env_int("IM_SSE_RECONNECT_INTERVAL", 5),
        run_total_timeout_seconds=_env_int("IM_RUN_TOTAL_TIMEOUT_SECONDS", 600),
        stream_throttle_seconds=_env_float("IM_STREAM_THROTTLE_SECONDS", 0.5),
        default_agent_slug=os.getenv("IM_DEFAULT_AGENT_SLUG", "default-chatbot").strip(),
        default_department=os.getenv("IM_DEFAULT_DEPARTMENT", "IM用户").strip(),
        user_resolve_rate_limit=_env_int("IM_USER_RESOLVE_RATE_LIMIT", 30),
        outputs_host_path_template=os.getenv(
            "IM_OUTPUTS_HOST_PATH_TEMPLATE",
            "/app/saves/threads/{thread_id}/user-data/outputs",
        ).strip(),
        feishu=FeishuConfig(
            enabled=feishu_enabled,
            app_id=feishu_app_id,
            app_secret=feishu_app_secret,
            domain=os.getenv("IM_FEISHU_DOMAIN", "https://open.feishu.cn").strip(),
            max_text_length=_env_int("IM_FEISHU_MAX_TEXT_LENGTH", 30000),
        ),
        dingtalk=DingtalkConfig(
            enabled=dingtalk_enabled,
            app_key=dingtalk_app_key,
            app_secret=dingtalk_app_secret,
            robot_code=os.getenv("IM_DINGTALK_ROBOT_CODE", "").strip(),
            max_text_length=_env_int("IM_DINGTALK_MAX_TEXT_LENGTH", 5000),
        ),
    )
