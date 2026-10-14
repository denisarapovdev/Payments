from argparse import Namespace
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.api.app import create_app
from payments.core.config import Settings
from payments.db.session import create_engine, create_session_factory

ROOT = Path(__file__).resolve().parents[2]
API_KEY = "test-api-key"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.integration)


def _docker_available() -> bool:
    try:
        import docker  # noqa: PLC0415

        docker.from_env().ping()
    except Exception:
        return False
    return True


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    if not _docker_available():
        pytest.skip("Docker is not available: integration tests need PostgreSQL")
    from testcontainers.community.postgres import PostgresContainer  # noqa: PLC0415

    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        url: str = pg.get_connection_url()
        config = Config(str(ROOT / "alembic.ini"))
        config.cmd_opts = Namespace(x=[f"url={url}"])
        command.upgrade(config, "head")
        yield url


@pytest.fixture(scope="session")
def settings(database_url: str) -> Settings:
    return Settings(database_url=database_url, api_key=SecretStr(API_KEY))


@pytest.fixture(scope="session")
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture(autouse=True)
async def _clean_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        yield client
