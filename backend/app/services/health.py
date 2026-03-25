from app.core.config import Settings
from app.schemas.health import HealthResponse


def build_health_response(settings: Settings) -> HealthResponse:
    return HealthResponse(
        status="ok",
        service=settings.app_name,
        environment=settings.app_env,
    )
