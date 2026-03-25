import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.router import api_router
from app.core.cors import configure_cors
from app.core.db import create_db_engine, create_session_factory, init_db
from app.core.config import get_settings
from app.services.dlq_store import DeadLetterQueueStore
from app.services.event_consumer import TransactionEventConsumer
from app.services.event_repository import EventRepository
from app.services.event_store import EventStore
from app.services.live_metrics import LiveMetricsService
from app.services.ops_metrics import OperationalMetricsService
from app.websocket.manager import ConnectionManager
from app.websocket.routes import router as websocket_router

settings = get_settings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logging.getLogger("kafka").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_engine = create_db_engine(settings)
    app.state.db_session_factory = create_session_factory(app.state.db_engine)
    init_db(app.state.db_engine)
    app.state.event_repository = EventRepository(app.state.db_session_factory)
    app.state.event_store = EventStore()
    app.state.live_metrics = LiveMetricsService()
    app.state.ops_metrics = OperationalMetricsService()
    app.state.dlq_store = DeadLetterQueueStore()
    app.state.connection_manager = ConnectionManager()
    app.state.connection_manager.bind_loop(asyncio.get_running_loop())
    app.state.transaction_event_consumer = TransactionEventConsumer(
        settings,
        app.state.event_store,
        app.state.live_metrics,
        app.state.ops_metrics,
        app.state.dlq_store,
        app.state.event_repository,
        app.state.connection_manager,
    )
    app.state.transaction_event_consumer.start()

    try:
        yield
    finally:
        app.state.transaction_event_consumer.stop()
        app.state.db_engine.dispose()


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

configure_cors(app, settings)
app.include_router(api_router)
app.include_router(websocket_router)


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    return {
        "name": settings.app_name,
        "environment": settings.app_env,
    }
