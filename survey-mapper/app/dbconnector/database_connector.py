from sqlalchemy import create_engine, Engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Literal, Union


class DatabaseConnector:
    """
    A utility class to create SQLAlchemy Engine connections to various database types.

    Supported database types:
        - "postgresql": PostgreSQL or PostGIS (sync or async with asyncpg)
        - "sqlite": SQLite
        - "spatialite": SQLite with SpatiaLite
        - "mssql": Microsoft SQL Server
        - None: No database connection

    Attributes:
        db_type (str): Type of the database to connect to.
        username (str): Username for authentication (if required).
        password (str): Password for authentication (if required).
        host (str): Hostname or IP address of the database server.
        port (str): Port number of the database server.
        database (str): Name of the target database.
        filepath (str): File path for SQLite or SpatiaLite database.
        driver (str): ODBC driver for MS SQL Server.
        spatialite_path (str): Path to SpatiaLite shared object.
        use_async (bool): Whether to use asyncpg for async PostgreSQL connections.
    """

    def __init__(
        self,
        db_type: Literal["postgresql", "sqlite", "spatialite", "mssql"],
        username: Union[str, None],
        password:Union[str, None],
        host: Union[str, None] ,
        port: Union[str, int, None],
        database: Union[str, None],
        filepath: Optional[str] = None,
        driver: Optional[str] = None,
        spatialite_path: Optional[str] = None,
        use_async: bool = False,
    ):
        """
        Initializes a DatabaseConnector with connection settings.

        Args:
            db_type: Type of the database.
            username: DB username.
            password: DB password.
            host: Hostname (default: localhost).
            port: Port number.
            database: Name of the database.
            filepath: Path to SQLite or SpatiaLite DB file.
            driver: ODBC driver for MSSQL.
            spatialite_path: Path to SpatiaLite shared object.
            use_async: Enable asyncpg for PostgreSQL.
        """
        self.db_type = db_type
        self.username = username
        self.password = password
        self.host = host or "localhost"
        self.port = str(port) if port else None
        self.database = database
        self.filepath = filepath
        self.driver = driver or "ODBC Driver 17 for SQL Server"
        self.spatialite_path = spatialite_path
        self.use_async = use_async

    def get_connection_string(self) -> str:
        """
        Builds the SQLAlchemy connection URL based on the DB type.

        Returns:
            str: Connection string.

        Raises:
            ValueError: If required parameters are missing.
        """
        if self.db_type == "postgresql":
            if self.use_async:
                return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            else:
                return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

        elif self.db_type == "sqlite":
            if not self.filepath:
                raise ValueError("SQLite filepath must be provided.")
            return f"sqlite:///{self.filepath}"

        elif self.db_type == "spatialite":
            if not self.filepath:
                raise ValueError("SpatiaLite filepath must be provided.")
            if not self.spatialite_path:
                raise ValueError("Path to SpatiaLite library must be provided.")
            return f"sqlite:///{self.filepath}"

        elif self.db_type == "mssql":
            if not all([self.username, self.password, self.host, self.database]):
                raise ValueError("Missing MS SQL Server connection parameters.")
            return (
                f"mssql+pyodbc://{self.username}:{self.password}@{self.host}/{self.database}"
                f"?driver={self.driver.replace(' ', '+')}"
            )

        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def connect(self) -> Engine:
        """
        Creates a synchronous SQLAlchemy engine.

        Returns:
            Engine: SQLAlchemy engine instance.

        Raises:
            ConnectionError: On connection failure.
        """
        if self.use_async:
            raise ValueError("Use connect_async() for async connections.")

        try:
            if self.db_type == "spatialite":
                engine = create_engine(
                    self.get_connection_string(),
                    connect_args={"check_same_thread": False},
                )
                with engine.connect() as conn:
                    conn.execute(f"SELECT load_extension('{self.spatialite_path}');")
            else:
                engine = create_engine(self.get_connection_string())

            # Test connection
            with engine.connect() as conn:
                conn.execute("SELECT 1")

            return engine

        except SQLAlchemyError as e:
            raise ConnectionError(f"Database connection failed: {e}") from e

    async def connect_async(self) -> AsyncEngine:
        """
        Creates an asynchronous SQLAlchemy engine using asyncpg.

        Returns:
            AsyncEngine: Asynchronous SQLAlchemy engine.

        Raises:
            ConnectionError: On connection failure.
        """
        if self.db_type != "postgresql" or not self.use_async:
            raise ValueError("Async connections are only supported for PostgreSQL with use_async=True.")

        try:
            engine = create_async_engine(self.get_connection_string(), echo=False)

            # Optional: test connection
            async with engine.connect() as conn:
                await conn.execute("SELECT 1")

            return engine

        except SQLAlchemyError as e:
            raise ConnectionError(f"Async database connection failed: {e}") from e
