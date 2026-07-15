"""
Monkey patch: 让 langchain_openai 从 OpenAI 兼容 API 响应中提取 reasoning_content。

langchain_openai 原版不提取 reasoning_content 字段（火山引擎 doubao、Ollama qwen3 等模型使用），
导致 thinking 内容丢失。此 patch 在流式和非流式两个解析函数中注入提取逻辑。

同时 patch AIMessageChunk.content_blocks，让 LangGraph 的 CustomTransformer / _compat_bridge
能将 reasoning_content 转换为 reasoning-delta 协议事件，最终传递到前端。

应用方式：在应用启动时调用 apply()。
"""

import langchain_openai.chat_models.base as _base
from langchain_core.messages import AIMessageChunk

from yuxi.utils.logging_config import logger

# 保存原始函数引用
_orig_convert_dict = _base._convert_dict_to_message
_orig_convert_delta = _base._convert_delta_to_message_chunk
_orig_content_blocks_get = AIMessageChunk.content_blocks.fget


def _patched_convert_dict_to_message(_dict):
    """非流式消息解析：提取 reasoning_content 到 additional_kwargs。"""
    msg = _orig_convert_dict(_dict)
    rc = _dict.get("reasoning_content") or _dict.get("reasoning") or _dict.get("thinking")
    if rc:
        logger.info(f"[reasoning_patch] NON-STREAM reasoning_content found, len={len(str(rc))}")
        if isinstance(msg.additional_kwargs, dict):
            msg.additional_kwargs["reasoning_content"] = rc
    elif _dict.get("content"):
        if not getattr(_patched_convert_dict_to_message, "_logged_once", False):
            _patched_convert_dict_to_message._logged_once = True
            logger.info(f"[reasoning_patch] NON-STREAM no rc, keys={list(_dict.keys())}")
    return msg


def _patched_convert_delta_to_message_chunk(_dict, default_class):
    """流式 chunk 解析：提取 reasoning_content 到 additional_kwargs。"""
    chunk = _orig_convert_delta(_dict, default_class)
    rc = _dict.get("reasoning_content") or _dict.get("reasoning") or _dict.get("thinking")
    if rc:
        if isinstance(chunk.additional_kwargs, dict):
            chunk.additional_kwargs["reasoning_content"] = rc
        else:
            logger.warning(f"[reasoning_patch] STREAM additional_kwargs is not a dict: {type(chunk.additional_kwargs)}")
    return chunk


def _patched_content_blocks(self):
    """扩展 content_blocks：将 additional_kwargs.reasoning_content 作为 reasoning block 输出。

    LangGraph 的 _compat_bridge 通过 content_blocks 读取内容块并生成协议事件。
    原版 content_blocks 不包含 additional_kwargs 中的 reasoning_content，导致推理内容
    在转换为协议事件时被丢弃。此 patch 在原版结果基础上补充 reasoning block。
    """
    blocks = _orig_content_blocks_get(self)
    rc = self.additional_kwargs.get("reasoning_content")
    if rc and not any(b.get("type") == "reasoning" for b in blocks):
        blocks = [*blocks, {"type": "reasoning", "reasoning": rc}]
    return blocks


def apply():
    """应用补丁，替换 langchain_openai 的消息解析函数。"""
    _base._convert_dict_to_message = _patched_convert_dict_to_message
    _base._convert_delta_to_message_chunk = _patched_convert_delta_to_message_chunk
    AIMessageChunk.content_blocks = property(_patched_content_blocks)
    logger.info("[reasoning_patch] patch applied successfully (convert_dict + convert_delta + content_blocks)")
