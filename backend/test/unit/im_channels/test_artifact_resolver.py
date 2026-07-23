"""artifact 路径白名单解析测试。"""
from yuxi.im_channels.artifacts import resolve_artifacts, OUTPUTS_PREFIX


def test_outputs_prefix_accepted(tmp_path):
    """outputs/ 前缀路径且文件存在 -> 解析成功。"""
    # 模拟 thread_id 对应的 outputs 目录
    fake_outputs = tmp_path / "outputs"
    fake_outputs.mkdir()
    fake_file = fake_outputs / "report.png"
    fake_file.write_bytes(b"\x89PNG fake")

    virtual_path = f"{OUTPUTS_PREFIX}report.png"
    attachments = resolve_artifacts(
        artifacts=[virtual_path],
        outputs_dir=fake_outputs,
    )
    assert len(attachments) == 1
    assert attachments[0].is_image is True
    assert attachments[0].filename == "report.png"


def test_non_outputs_prefix_rejected(tmp_path):
    """非 outputs/ 前缀路径被拒绝。"""
    attachments = resolve_artifacts(
        artifacts=["/home/gem/user-data/uploads/secret.txt"],
        outputs_dir=tmp_path / "outputs",
    )
    assert attachments == []


def test_path_traversal_rejected(tmp_path):
    """路径穿越(../)被拒绝。"""
    fake_outputs = tmp_path / "outputs"
    fake_outputs.mkdir()
    # 创建一个 outputs 外的文件
    secret = tmp_path / "secret.txt"
    secret.write_text("secret")

    attachments = resolve_artifacts(
        artifacts=[f"{OUTPUTS_PREFIX}../secret.txt"],
        outputs_dir=fake_outputs,
    )
    assert attachments == []


def test_missing_file_skipped(tmp_path):
    """文件不存在 -> 跳过。"""
    attachments = resolve_artifacts(
        artifacts=[f"{OUTPUTS_PREFIX}nonexistent.png"],
        outputs_dir=tmp_path / "outputs",
    )
    assert attachments == []


def test_non_image_mime_detected(tmp_path):
    """非图片 MIME 正确识别。"""
    fake_outputs = tmp_path / "outputs"
    fake_outputs.mkdir()
    fake_file = fake_outputs / "report.pdf"
    fake_file.write_bytes(b"%PDF-1.4 fake")

    attachments = resolve_artifacts(
        artifacts=[f"{OUTPUTS_PREFIX}report.pdf"],
        outputs_dir=fake_outputs,
    )
    assert len(attachments) == 1
    assert attachments[0].is_image is False
    assert attachments[0].mime_type == "application/pdf"
