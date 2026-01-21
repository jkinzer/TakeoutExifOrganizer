import sqlite3
import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

from .media_type import MediaType
from .media_metadata import MediaMetadata

logger = logging.getLogger(__name__)

class FileStatus(str, Enum):
    NEW = 'NEW'
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    METADATA_READ = 'METADATA_READ'
    TARGET_RESOLVED = 'TARGET_RESOLVED'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    SKIPPED = 'SKIPPED'

MEMORY = ":memory:"

class ProcessingPhase(str, Enum):
    DISCOVERY = 'DISCOVERY'
    METADATA_READ = 'METADATA_READ'
    RESOLUTION = 'RESOLUTION'
    EXECUTION = 'EXECUTION'

class PersistenceManager:
    """Manages persistence using SQLite."""

    @staticmethod
    def in_memory() -> 'PersistenceManager':
        return PersistenceManager(MEMORY)

    @staticmethod
    def file_db(db_path: str | Path) -> 'PersistenceManager':
        return PersistenceManager(db_path)

    def __init__(self, db_path: str | Path):
        self.db_path = db_path
        self.conn = None
        # If in-memory, initialize immediately to keep connection open
        if str(db_path) == MEMORY:
            self.initialize()

    def initialize(self):
        if self.conn:
            return
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        
        # Files Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT UNIQUE NOT NULL,
                media_type TEXT,
                file_size INTEGER,
                mtime REAL,
                status TEXT,
                phase TEXT,
                target_path TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Metadata Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                file_id INTEGER,
                source TEXT,
                data TEXT,
                PRIMARY KEY (file_id, source),
                FOREIGN KEY(file_id) REFERENCES files(id)
            )
        ''')
        
        self.conn.commit()

    def add_file(self, path: Path, media_type: MediaType, file_size: int, mtime: float) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO files (source_path, media_type, file_size, mtime, status, phase)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (str(path), media_type.type, file_size, mtime, FileStatus.NEW.value, ProcessingPhase.DISCOVERY.value))
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # File might already exist, retrieve it
            existing = self.get_file_by_path(path)
            if existing:
                return existing['id']
            raise

    def get_file_by_path(self, path: Path) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM files WHERE source_path = ?', (str(path),))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_file_by_id(self, file_id: int) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM files WHERE id = ?', (file_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def update_status(self, file_id: int, status: FileStatus, phase: ProcessingPhase, error: str = None):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE files 
            SET status = ?, phase = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (status.value, phase.value, error, file_id))
        self.conn.commit()
    
    def update_target_path(self, file_id: int, target_path: Path):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE files 
            SET target_path = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (str(target_path), file_id))
        self.conn.commit()

    def save_metadata(self, file_id: int, source: str, metadata: MediaMetadata):
        cursor = self.conn.cursor()
        from dataclasses import asdict
        data_json = json.dumps(asdict(metadata))
        
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (file_id, source, data)
            VALUES (?, ?, ?)
        ''', (file_id, source, data_json))
        self.conn.commit()

    def get_metadata(self, file_id: int, source: str) -> Optional[MediaMetadata]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT data FROM metadata WHERE file_id = ? AND source = ?', (file_id, source))
        row = cursor.fetchone()
        if row:
            data_dict = json.loads(row['data'])
            return MediaMetadata(**data_dict)
        return None

    def get_files_by_status(self, status: List[FileStatus], limit: int = 1000) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        placeholders = ','.join(['?'] * len(status))
        query = f'SELECT * FROM files WHERE status IN ({placeholders}) LIMIT ?'
        args = [s.value for s in status] + [limit]
        cursor.execute(query, args)
        return [dict(row) for row in cursor.fetchall()]
    
    def get_all_files(self) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM files')
        return [dict(row) for row in cursor.fetchall()]

    def close(self):
        # If in-memory, we might want to keep it open for tests, 
        # but typically close() is called at end of process.
        # For tests using :memory:, we rely on the object staying alive.
        if self.conn and str(self.db_path) != MEMORY:
            self.conn.close()
