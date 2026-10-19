class DomainError(Exception):
    pass


class InvalidTransition(DomainError):
    pass


class OrderNotFound(DomainError):
    pass


class IdempotencyConflict(DomainError):
    pass


class InventoryError(DomainError):
    pass


class ProductNotFound(InventoryError):
    pass


class PriceMismatch(InventoryError):
    pass


class InsufficientStock(InventoryError):
    pass


class InvalidQuantity(InventoryError):
    pass
