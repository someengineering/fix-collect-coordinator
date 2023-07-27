from __future__ import annotations

from typing import TypeVar, Generic, Type, AsyncIterator, Optional, Any, Tuple

from sqlalchemy import select, Select, Executable
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import ColumnExpressionArgument

from collect_coordinator.service import Service


class DbEntity(DeclarativeBase):
    """
    Base class for all database entities
    """


KeyType = TypeVar("KeyType")
DbEntityType = TypeVar("DbEntityType", bound=DbEntity)


class DbSession(Generic[KeyType, DbEntityType]):
    """
    Database session wrapper, that supports the async context manager protocol.
    All changes are committed automatically on exit.
    """

    def __init__(self, session: AsyncSession, key_class: Type[KeyType], entity_class: Type[DbEntityType]) -> None:
        self.session: AsyncSession = session
        self.key_class = key_class
        self.entity_class = entity_class

    async def __aenter__(self) -> DbSession[KeyType, DbEntityType]:
        await self.session.__aenter__()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.session.commit()
        await self.session.__aexit__(exc_type, exc_val, exc_tb)

    def add(self, entity: DbEntityType) -> None:
        self.session.add(entity)

    # noinspection PyTypeChecker
    def select(self) -> Select[Tuple[DbEntityType]]:
        return select(self.entity_class)

    async def commit(self) -> None:
        await self.session.commit()

    async def get(self, key: KeyType) -> Optional[DbEntityType]:
        return await self.session.get(self.entity_class, key)

    def all(self) -> AsyncIterator[DbEntityType]:
        return self.query(select(self.entity_class))

    def where(self, *where_clause: ColumnExpressionArgument[bool]) -> AsyncIterator[DbEntityType]:
        return self.query(select(self.entity_class).where(*where_clause))

    # noinspection PyTypeChecker
    async def query(self, select_statement: Select[Tuple[DbEntityType]]) -> AsyncIterator[DbEntityType]:
        result = await self.session.stream(select_statement)
        async for row in result:
            yield row[0]

    async def execute(self, statement: Executable) -> Any:
        result = await self.session.stream(statement)
        async for row in result:
            yield row


class EntityDb(Generic[KeyType, DbEntityType]):
    """
    This class is able to manage one specific database entity type.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        session_maker: async_sessionmaker[AsyncSession],
        key_class: Type[KeyType],
        entity_class: Type[DbEntityType],
    ) -> None:
        self.engine = engine
        self.session_maker = session_maker
        self.key_class = key_class
        self.entity_class = entity_class

    def session(self) -> DbSession[KeyType, DbEntityType]:
        return DbSession(self.session_maker(), self.key_class, self.entity_class)


class DbEngine(Service):
    """
    The engine should exist exactly once per database server.
    """

    def __init__(self, connection_string: str) -> None:
        self.engine: AsyncEngine = create_async_engine(connection_string)
        self.session_maker = async_sessionmaker(self.engine, expire_on_commit=False)

    def db(self, key_clazz: Type[KeyType], entity_class: Type[DbEntityType]) -> EntityDb[KeyType, DbEntityType]:
        return EntityDb(self.engine, self.session_maker, key_clazz, entity_class)

    async def start(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(DbEntity.metadata.create_all)

    async def stop(self) -> None:
        await self.engine.dispose()
