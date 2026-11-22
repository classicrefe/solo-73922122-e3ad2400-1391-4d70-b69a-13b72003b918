import tempfile

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import main as main_module
from app.main import app, get_db
from app.models import Base


def _throwaway_engine():
    _fd, path = tempfile.mkstemp(suffix=".db")
    return create_engine(
        f"sqlite:///{path}", connect_args={"check_same_thread": False}
    )


@pytest.fixture()
def client(tmp_path):
    """Each test gets an isolated SQLite database."""
    engine = create_engine(
        f"sqlite:///{tmp_path / 'test.db'}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    test_session = sessionmaker(bind=engine, future=True)

    def override_get_db():
        db = test_session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    main_module.engine = engine
    # raise_server_exceptions=False lets route-level handling convert internal
    # failures into HTTP 500 responses in the failure-recovery tests.
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    app.dependency_overrides.clear()
    main_module.engine = _throwaway_engine()
