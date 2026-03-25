from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.models import Base


def create_db_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,
    )


def create_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(bind=engine)
    ensure_transaction_event_columns(engine)


def ensure_transaction_event_columns(engine: Engine) -> None:
    inspector = inspect(engine)
    if "transaction_events" not in inspector.get_table_names():
        return

    columns = {column["name"] for column in inspector.get_columns("transaction_events")}
    alter_statements: list[str] = []

    if "unit_price" not in columns:
        alter_statements.append("ALTER TABLE transaction_events ADD COLUMN unit_price NUMERIC(18, 4)")
    if "quantity" not in columns:
        alter_statements.append("ALTER TABLE transaction_events ADD COLUMN quantity INTEGER")
    if "notional_amount" not in columns:
        alter_statements.append("ALTER TABLE transaction_events ADD COLUMN notional_amount NUMERIC(18, 2)")

    if not alter_statements:
        return

    with engine.begin() as connection:
        for statement in alter_statements:
            connection.execute(text(statement))
