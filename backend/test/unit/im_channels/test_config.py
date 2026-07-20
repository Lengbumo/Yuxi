"""IM 渠道配置加载测试。"""
from yuxi.im_channels.config import load_im_config


def test_im_config_disabled_by_default(monkeypatch):
    """未设置 IM_WORKER_ENABLED 时默认关闭。"""
    for key in ("IM_WORKER_ENABLED", "IM_FEISHU_ENABLED", "IM_DINGTALK_ENABLED",
                "IM_FEISHU_APP_ID", "IM_FEISHU_APP_SECRET",
                "IM_DINGTALK_APP_KEY", "IM_DINGTALK_APP_SECRET"):
        monkeypatch.delenv(key, raising=False)
    config = load_im_config()
    assert config.enabled is False
    assert config.feishu.enabled is False
    assert config.dingtalk.enabled is False


def test_im_config_feishu_enabled(monkeypatch):
    """飞书启用时 app_id/app_secret 必填。"""
    monkeypatch.setenv("IM_WORKER_ENABLED", "true")
    monkeypatch.setenv("IM_FEISHU_ENABLED", "true")
    monkeypatch.setenv("IM_FEISHU_APP_ID", "cli_xxx")
    monkeypatch.setenv("IM_FEISHU_APP_SECRET", "secret_xxx")
    config = load_im_config()
    assert config.feishu.enabled is True
    assert config.feishu.app_id == "cli_xxx"


def test_im_config_feishu_missing_credentials(monkeypatch):
    """启用但缺凭证时 enabled 强制 False,日志告警。"""
    monkeypatch.setenv("IM_WORKER_ENABLED", "true")
    monkeypatch.setenv("IM_FEISHU_ENABLED", "true")
    monkeypatch.delenv("IM_FEISHU_APP_ID", raising=False)
    config = load_im_config()
    assert config.feishu.enabled is False


def test_im_config_default_agent_slug(monkeypatch):
    """默认 agent slug 可配置,缺省 default-chatbot。"""
    monkeypatch.delenv("IM_DEFAULT_AGENT_SLUG", raising=False)
    config = load_im_config()
    assert config.default_agent_slug == "default-chatbot"
