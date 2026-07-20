"""消息超长截断。

IM 平台单条消息有长度上限,超出截断并追加提示。
飞书 interactive card 单条约 30KB,钉钉 markdown 卡片 5000 字。
"""


FEISHU_MAX = 30000
DINGTALK_MAX = 5000
_TRUNCATION_NOTICE = "\n\n⚠️ 内容过长已截断,完整内容请前往 Yuxi 查看"


def truncate_text(text: str, max_length: int) -> str:
    """超过 max_length 截断并追加提示,使总长度不超过 max_length。

    未超长则原样返回。
    """
    if len(text) <= max_length:
        return text
    return text[: max_length - len(_TRUNCATION_NOTICE)] + _TRUNCATION_NOTICE
