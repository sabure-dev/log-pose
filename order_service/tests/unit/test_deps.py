import pytest
from fastapi import HTTPException

from app.api.deps import _safe_compare_digest, verify_operator_access
from app.config import settings


def test_safe_compare_digest_with_ascii_and_unicode():
    assert _safe_compare_digest("valid-secret", "valid-secret") is True
    assert _safe_compare_digest("wrong-secret", "valid-secret") is False
    # Non-ASCII Unicode strings must not raise TypeError
    assert _safe_compare_digest("invalid_töken_ñ_123", "valid-secret") is False
    assert _safe_compare_digest("valid-secret", "invalid_töken_ñ_123") is False


def test_verify_operator_access_with_valid_token_and_roles():
    verify_operator_access(x_internal_token=settings.internal_api_token)
    verify_operator_access(
        x_internal_token=settings.internal_api_token, x_user_role="warehouse"
    )
    verify_operator_access(
        x_internal_token=settings.internal_api_token, x_user_role="warehouse_operator"
    )
    verify_operator_access(
        x_internal_token=settings.internal_api_token, x_user_role="admin"
    )


def test_verify_operator_access_blocks_spoofed_role_without_token():
    with pytest.raises(HTTPException) as exc_info:
        verify_operator_access(x_internal_token=None, x_user_role="admin")
    assert exc_info.value.status_code == 403


def test_verify_operator_access_blocks_non_operator_role_even_with_token():
    with pytest.raises(HTTPException) as exc_info:
        verify_operator_access(
            x_internal_token=settings.internal_api_token, x_user_role="customer"
        )
    assert exc_info.value.status_code == 403


def test_verify_operator_access_with_non_ascii_token_raises_403_not_500():
    with pytest.raises(HTTPException) as exc_info:
        verify_operator_access(x_internal_token="invalid_töken_unicode_ñ_123")
    assert exc_info.value.status_code == 403
