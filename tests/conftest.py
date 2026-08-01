import os
import sys

os.environ.setdefault('BOT_TOKEN', 'test-token')
os.environ.setdefault('SHEETS_ID', 'test-sheet-id')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

import db


@pytest.fixture
async def temp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, 'DB_PATH', str(tmp_path / 'test_leads.db'))
    await db.init_db()
    yield db
    await db.close_db()
