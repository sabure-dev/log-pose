import enum


class OrderStatus(str, enum.Enum):
    PENDING = "pending"
    RESERVED = "reserved"
    READY_FOR_SHIPPING = "ready_for_shipping"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
