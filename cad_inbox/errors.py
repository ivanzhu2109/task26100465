"""异常类型定义。"""


class CadInboxError(Exception):
    """库所有异常的基类。"""


class ValidationError(CadInboxError):
    """请求不合法（批次大小、文件大小、参数缺失等）。"""


class SourceFileError(CadInboxError):
    """源文件不可读或在收件过程中发生变化。"""


class IdempotencyConflictError(CadInboxError):
    """幂等键在有效期内，但请求内容与首次提交不一致。"""
