import pytest
from pydantic import BaseModel
from waldur_cscs_hpc_storage.utils import paginate_response


class MockFilter(BaseModel):
    page: int = 1
    page_size: int = 10


class MockResource(BaseModel):
    id: int


def test_paginate_response_with_total_count():
    resources = [MockResource(id=i) for i in range(5)]
    filters = MockFilter(page=1, page_size=10)

    # Simulate first page of 50 total items
    response = paginate_response(resources, filters, total_count=50)

    pagination = response["pagination"]
    assert pagination["total"] == 50
    assert pagination["pages"] == 5
    assert pagination["has_next"] is True


def test_paginate_response_last_page():
    resources = [MockResource(id=i) for i in range(10)]
    filters = MockFilter(page=5, page_size=10)

    # Simulate last page of 50 total items
    response = paginate_response(resources, filters, total_count=50)

    pagination = response["pagination"]
    assert pagination["total"] == 50
    assert pagination["pages"] == 5
    assert pagination["has_next"] is False


def test_paginate_response_without_total_count():
    resources = [MockResource(id=i) for i in range(5)]
    filters = MockFilter(page=1, page_size=10)

    # Fallback to len(resources)
    response = paginate_response(resources, filters)

    pagination = response["pagination"]
    assert pagination["total"] == 5
    assert pagination["pages"] == 1
    assert pagination["has_next"] is False


def test_paginate_response_empty():
    resources = []
    filters = MockFilter(page=1, page_size=10)

    response = paginate_response(resources, filters, total_count=0)

    pagination = response["pagination"]
    assert pagination["total"] == 0
    assert pagination["pages"] == 0
    assert pagination["has_next"] is False
