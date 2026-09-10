"""Pydantic request/response schemas (OpenAI-compatible)."""
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class ChatMessage(BaseModel):
    """Permissive OpenAI-compatible message model.

    Hermes sends `tool` role messages after a function call and may send
    assistant messages with `tool_calls`. Content can be a string, array,
    object, or null depending on the client and API version.
    """

    model_config = ConfigDict(extra="allow")

    role: str
    content: Any = None
    name: Optional[str] = None
    tool_call_id: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None

    def to_upstream(self) -> Dict[str, Any]:
        # Include extra OpenAI-compatible message fields but omit null values.
        return self.model_dump(exclude_none=True)


class ChatCompletionRequest(BaseModel):
    """OpenAI Chat Completions request fields used by Hermes and similar agents."""

    model_config = ConfigDict(extra="allow")

    # OpenAI clients normally send `model`; MODEL is only an optional
    # environment override for clients that omit it.
    model: Optional[str] = None
    messages: List[ChatMessage]
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = 65536
    stream: bool = False
    use_relay: Optional[bool] = None

    # Tool calling required by Hermes.
    tools: Optional[List[Dict[str, Any]]] = None
    tool_choice: Optional[Union[str, Dict[str, Any]]] = None
    parallel_tool_calls: Optional[bool] = None

    # Common OpenAI-compatible options Hermes or its SDK may send.
    top_p: Optional[float] = None
    stop: Optional[Union[str, List[str]]] = None
    presence_penalty: Optional[float] = None
    frequency_penalty: Optional[float] = None
    response_format: Optional[Dict[str, Any]] = None
    seed: Optional[int] = None
    n: Optional[int] = None
    stream_options: Optional[Dict[str, Any]] = None
    # Hanya diteruskan bila klien mengset eksplisit. Nilai default "max" yang
    # lama SELALU dikirim dan ditolak upstream OpenCode (400) karena
    # /chat/completions tidak mengenal field ini (itu field Responses API),
    # dan "max" bukan nilai valid. Ini penyebab 400 di relay MAUPUN direct.
    reasoning_effort: Optional[str] = None


class ModelInfo(BaseModel):
    id: str
    object: str = "model"
    created: int
    owned_by: str = "opencode"


class ModelList(BaseModel):
    object: str = "list"
    data: List[ModelInfo]


class RelayStatus(BaseModel):
    success: bool
    relay_url: str
    direct_ip: Optional[str]
    relay_ip: Optional[str]
    is_masked: bool
    response_time_ms: float
    error: Optional[str] = None
    all_relays: Optional[List[Dict[str, Any]]] = None


class HealthResponse(BaseModel):
    status: str
    model: str
    relay: str
    relay_enabled: bool
    hermes_compatible: bool
    version: str


class UsageByModel(BaseModel):
    request_count: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


class UsageResponse(BaseModel):
    """Aggregated token usage inside a time window.

    `prompt_tokens` is the total input tokens used; `completion_tokens` is
    the total output tokens produced; `total_tokens` is their sum.
    """

    period: str
    description: str
    start_time: str
    end_time: str
    request_count: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    by_model: Dict[str, UsageByModel]


class UsagePeriodsResponse(BaseModel):
    supported_periods: List[str]
    default: str
    details: Dict[str, str]


class PropsCapability(BaseModel):
    """One capability the proxy supports, with a short description."""

    name: str
    supported: bool
    description: Optional[str] = None


class PropsEndpoint(BaseModel):
    """One HTTP endpoint exposed by the proxy."""

    path: str
    method: str
    description: str


class PropsRelay(BaseModel):
    """Relay configuration snapshot."""

    url: str
    enabled: bool
    fallback: bool


class PropsDefaults(BaseModel):
    """Default values applied when the client omits optional parameters."""

    temperature: float = 0.7
    max_tokens: int = 65536


class PropsInfo(BaseModel):
    """Read-only snapshot of the proxy configuration and capabilities.

    Exposed at `GET /v1/props`. Clients (Hermes probes, dashboards) can
    use it to introspect what the proxy supports without inspecting the
    source code.
    """

    object: str = "props"
    model: str
    version: str
    request_timeout: int
    relay: PropsRelay
    relay_urls: List[str]
    hermes_compatible: bool
    usage_tracking: bool
    capabilities: List[PropsCapability]
    supported_parameters: List[str]
    defaults: PropsDefaults
    endpoints: List[PropsEndpoint]
