class DomainError(Exception):
    pass


class InvalidTransition(DomainError):
    pass


class OrderNotFound(DomainError):
    pass


class IdempotencyConflict(DomainError):
    pass
