"""消息超长截断测试。"""
from yuxi.im_channels.truncation import truncate_text, FEISHU_MAX, DINGTALK_MAX


def test_feishu_under_limit_unchanged():
    text = "你好" * 100
    assert truncate_text(text, FEISHU_MAX) == text


def test_feishu_over_limit_truncated_with_notice():
    text = "a" * (FEISHU_MAX + 100)
    result = truncate_text(text, FEISHU_MAX)
    assert len(result) <= FEISHU_MAX
    assert "截断" in result


def test_dingtalk_over_limit_truncated():
    text = "b" * (DINGTALK_MAX + 50)
    result = truncate_text(text, DINGTALK_MAX)
    assert len(result) <= DINGTALK_MAX
    assert "截断" in result


def test_empty_text_unchanged():
    assert truncate_text("", FEISHU_MAX) == ""
