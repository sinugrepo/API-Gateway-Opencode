"""Route group: usage_routes."""
import asyncio
import json
import secrets
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from app.security.gateway_auth import verify_gateway_key
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
)
from app.core.schemas import UsagePeriodsResponse, UsageResponse
from app.services.usage import _VALID_PERIODS, _query_usage, _resolve_period

router = APIRouter()

@router.get("/v1/usage", response_model=UsageResponse, dependencies=[Depends(verify_gateway_key)])
async def get_usage(
    period: str = "today",
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """Return aggregated token usage for the requested period.

    Supported periods (query parameter `period`):
    - `today` - from 00:00 local time to now
    - `3h`, `6h` - last 3 or 6 hours
    - `1d`, `7d`, `30d` - last 1, 7, or 30 days

    For custom date ranges, pass `start` and `end` as YYYY-MM-DD (e.g.
    `?start=2026-07-20&end=2026-07-25`). When both are present they
    override `period`.
    """
    description, start_time, end_time = _resolve_period(period, datetime.now(), start, end)
    stats = await asyncio.to_thread(_query_usage, start_time, end_time)
    return UsageResponse(
        period="custom" if (start and end) else period,
        description=description,
        start_time=datetime.fromtimestamp(start_time).astimezone().isoformat(timespec="seconds"),
        end_time=datetime.fromtimestamp(end_time).astimezone().isoformat(timespec="seconds"),
        request_count=stats["request_count"],
        prompt_tokens=stats["prompt_tokens"],
        completion_tokens=stats["completion_tokens"],
        total_tokens=stats["total_tokens"],
        by_model=stats["by_model"],
    )


@router.get("/v1/usage/periods", response_model=UsagePeriodsResponse, dependencies=[Depends(verify_gateway_key)])
async def list_usage_periods():
    """List the period keywords accepted by `GET /v1/usage`."""
    return UsagePeriodsResponse(
        supported_periods=list(_VALID_PERIODS),
        default="today",
        details={
            "today": "00:00 local time to now",
            "3h": "last 3 hours",
            "6h": "last 6 hours",
            "1d": "last 1 day",
            "7d": "last 7 days",
            "30d": "last 30 days",
        },
    )
