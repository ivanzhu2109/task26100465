"""异常类型定义。"""


class CadInboxError(Exception):
    """库内所有异常的基类。"""


class ValidationError(CadInboxError):
    """请求参数不满足业务约束（数量、大小、类型等）。"""


class IdempotencyConflictError(CadInboxError):
    """幂等键在有效期内被不同内容的请求复用。"""


class NotFoundError(CadInboxError):
    """查询的对象不存在。"""
