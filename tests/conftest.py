import pytest as pytest
from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseSettings

from tests.tasks import (
    SimpleTask,
    TaskWithDirectDependency,
    TaskWithAllOfDependency,
    TaskWithAnyOfDependency,
    TaskWithOptionalDependency,
    TaskWithOptionalAllOfDependency,
    TaskWithOptionalAnyOfDependency,
)


class Settings(BaseSettings):
    mongodb_dsn: str = "mongodb://localhost:27017/beanie_db"
    mongodb_db_name: str = "beanie_queue_db"


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture()
def cli(settings):
    return AsyncIOMotorClient(settings.mongodb_dsn)


@pytest.fixture()
def db(cli, settings):
    return cli[settings.mongodb_db_name]


@pytest.fixture(autouse=True)
async def init(db):
    models = [
        SimpleTask,
        TaskWithDirectDependency,
        TaskWithAllOfDependency,
        TaskWithAnyOfDependency,
        TaskWithOptionalDependency,
        TaskWithOptionalAllOfDependency,
        TaskWithOptionalAnyOfDependency,
    ]
    await init_beanie(
        database=db,
        document_models=models,
    )

    yield None

    for model in models:
        await model.get_motor_collection().drop()
        await model.get_motor_collection().drop_indexes()
