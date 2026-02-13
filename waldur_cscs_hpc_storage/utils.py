from typing import Any, Dict, Optional, Sequence

from pydantic import BaseModel


def paginate_response(
    resources: Sequence[BaseModel],
    filters: BaseModel,
    extra_filters: Optional[Dict[str, Any]] = None,
    total_count: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Generic pagination and serialization utility.

    Args:
        resources: List of Pydantic models to serialize and paginate.
        filters: Pydantic model containing filter parameters (must have page and page_size).
        extra_filters: Dict of additional filters to include in the response (e.g. calculated ones).
        total_count: Optional total number of items across all pages. Defaults to len(resources).

    Returns:
        Dict containing the formatted response with 'status', 'resources', 'pagination', and 'filters_applied'.
    """
    page = getattr(filters, "page", 1)
    page_size = getattr(filters, "page_size", 100)

    total_items = total_count if total_count is not None else len(resources)
    total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 0
    has_next = page < total_pages

    # Slice to the requested page
    start = (page - 1) * page_size
    end = start + page_size
    page_resources = resources[start:end]

    serialized_resources = [r.model_dump(by_alias=True) for r in page_resources]

    filters_applied = filters.model_dump(exclude_none=True)
    if extra_filters:
        filters_applied.update(extra_filters)

    return {
        "status": "success",
        "resources": serialized_resources,
        "pagination": {
            "current": page,
            "limit": page_size,
            "offset": (page - 1) * page_size,
            "pages": total_pages,
            "total": total_items,
            "has_next": has_next,
        },
        "filters_applied": filters_applied,
    }
