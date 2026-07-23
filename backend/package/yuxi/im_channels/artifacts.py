"""artifact 路径白名单解析。

只允许 /home/gem/user-data/outputs/ 前缀,Path.resolve() 后必须仍在 outputs 目录内,
防符号链接与路径穿越。文件不存在跳过,不抛异常。
"""
from __future__ import annotations

import logging
import mimetypes
from pathlib import Path

from yuxi.im_channels.message_bus import ResolvedAttachment

logger = logging.getLogger(__name__)

OUTPUTS_PREFIX = "/home/gem/user-data/outputs/"


def resolve_artifacts(
    artifacts: list[str],
    outputs_dir: Path,
) -> list[ResolvedAttachment]:
    """把 virtual_path 解析为宿主文件系统路径,构造 ResolvedAttachment。

    outputs_dir 是当前 thread 的 outputs 目录绝对路径(已 resolve)。
    """
    outputs_dir = outputs_dir.resolve()
    attachments: list[ResolvedAttachment] = []

    for virtual_path in artifacts:
        if not virtual_path.startswith(OUTPUTS_PREFIX):
            logger.warning("[artifacts] rejected non-outputs path: %s", virtual_path)
            continue

        relative = virtual_path.removeprefix(OUTPUTS_PREFIX)
        actual = (outputs_dir / relative).resolve()

        # 双重校验:resolve 后仍在 outputs_dir 内
        try:
            actual.relative_to(outputs_dir)
        except ValueError:
            logger.warning("[artifacts] path escapes outputs dir: %s -> %s", virtual_path, actual)
            continue

        if not actual.is_file():
            logger.warning("[artifacts] file not found: %s -> %s", virtual_path, actual)
            continue

        mime, _ = mimetypes.guess_type(str(actual))
        mime = mime or "application/octet-stream"

        attachments.append(ResolvedAttachment(
            virtual_path=virtual_path,
            actual_path=actual,
            filename=actual.name,
            mime_type=mime,
            size=actual.stat().st_size,
            is_image=mime.startswith("image/"),
        ))

    return attachments
