import sqlite3
import json
import logging
import os

# DB Path - Absolute Path Fix
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'sqlite.db')


class DBManager:
    """Centralized Database Manager for AI Data Reporter (v8.0 Refined)"""

    @staticmethod
    def init_db():
        """Initialize all required database tables with strict schema (v9.0)"""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # 1. conversations table (Metadata)
        c.execute('''CREATE TABLE IF NOT EXISTS conversations 
                     (conversation_id TEXT PRIMARY KEY, 
                      user_id TEXT, 
                      created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # 2. states table (Strict Source-aware)
        c.execute('''CREATE TABLE IF NOT EXISTS states (
                        conversation_id TEXT,
                        source TEXT, -- 'ga4', 'file', 'mixed'
                        state_json TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(conversation_id, source))''')

        # 3. conversation_context table (Decision/Routing state)
        c.execute('''CREATE TABLE IF NOT EXISTS conversation_context (
                        conversation_id TEXT PRIMARY KEY,
                        active_source TEXT,
                        property_id TEXT,
                        file_path TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # 4. Event registry
        c.execute('''CREATE TABLE IF NOT EXISTS event_registry 
                     (property_id TEXT, 
                      event_name TEXT, 
                      last_seen DATETIME, 
                      PRIMARY KEY(property_id, event_name))''')

        # 5. File registry
        c.execute('''CREATE TABLE IF NOT EXISTS file_registry 
                     (file_path TEXT PRIMARY KEY,
                      file_name TEXT,
                      file_type TEXT,
                      schema_info TEXT,
                      row_count INTEGER,
                      uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # 6. Reports table (v9.5)
        c.execute('''CREATE TABLE IF NOT EXISTS reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        conversation_id TEXT,
                        title TEXT,
                        content_json TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP)''')

        # 7. last_results table (for followup post-processing)
        c.execute('''CREATE TABLE IF NOT EXISTS last_results (
                        conversation_id TEXT,
                        source TEXT,
                        result_json TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(conversation_id, source)
                    )''')

        # Cleanup states_v2 if it exists (legacy from v8.0)
        try:
            c.execute("INSERT OR IGNORE INTO states (conversation_id, source, state_json, updated_at) "
                      "SELECT conversation_id, source, state_json, updated_at FROM states_v2")
            c.execute("DROP TABLE IF EXISTS states_v2")
        except:
            pass

        conn.commit()
        conn.close()
        logging.info(f"[DBManager] Database re-initialized at {DB_PATH}")

    @staticmethod
    def load_last_state(conversation_id, source="ga4"):
        """Load last successful state from 'states' table"""
        if not conversation_id:
            return None

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                "SELECT state_json FROM states WHERE conversation_id = ? AND source = ?",
                (conversation_id, source)
            )
            row = c.fetchone()
            conn.close()

            if row:
                return json.loads(row[0])

        except Exception as e:
            logging.error(f"[DBManager] Failed to load state (conv={conversation_id}, src={source}): {e}")

        return None

    @staticmethod
    def save_success_state(conversation_id, source, state):
        """Save successful state to 'states' table"""
        if not conversation_id or not source:
            return

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO states
                (conversation_id, source, state_json, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (conversation_id, source, json.dumps(state, ensure_ascii=False)))
            conn.commit()
            conn.close()

            logging.info(f"[DBManager] State saved: Conv={conversation_id}, Source={source}")

        except Exception as e:
            logging.error(f"[DBManager] Failed to save state (conv={conversation_id}, src={source}): {e}")

    @staticmethod
    def save_conversation_record(conversation_id, user_id, property_id=None, file_path=None):
        """Create or update a conversation session record (v8.1 Compatibility)"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO conversations
                (conversation_id, user_id, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (conversation_id, user_id))
            conn.commit()
            conn.close()

            if property_id or file_path:
                DBManager.save_conversation_context(conversation_id, {
                    "property_id": property_id,
                    "file_path": file_path
                })

        except Exception as e:
            logging.error(f"[DBManager] Failed to save conversation record: {e}")

    @staticmethod
    def get_session_info(conversation_id):
        """Get user_id for a session"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT user_id FROM conversations WHERE conversation_id = ?", (conversation_id,))
            row = c.fetchone()
            conn.close()

            if row:
                return {"user_id": row[0]}

        except Exception as e:
            logging.error(f"[DBManager] Failed to get session info: {e}")

        return None

    @staticmethod
    def get_events(property_id):
        """Get all registered events for a property"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT event_name FROM event_registry WHERE property_id = ?", (property_id,))
            events = [r[0] for r in c.fetchall()]
            conn.close()
            return events

        except Exception as e:
            logging.error(f"[DBManager] Failed to get events: {e}")
            return []

    @staticmethod
    def load_conversation_context(conversation_id):
        """Load active context (active_source, property_id, file_path)"""
        if not conversation_id:
            return None

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT active_source, property_id, file_path
                FROM conversation_context
                WHERE conversation_id = ?
            """, (conversation_id,))
            row = c.fetchone()
            conn.close()

            if row:
                return {"active_source": row[0], "property_id": row[1], "file_path": row[2]}

        except Exception as e:
            logging.error(f"[DBManager] Failed to load conversion context: {e}")

        return None

    @staticmethod
    def save_conversation_context(conversation_id, context):
        """Save or update conversation context"""
        if not conversation_id:
            logging.error("[DBManager] save_conversation_context failed: conversation_id is required")
            return

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO conversation_context
                (conversation_id, active_source, property_id, file_path, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                conversation_id,
                context.get("active_source"),
                context.get("property_id"),
                context.get("file_path")
            ))
            conn.commit()
            conn.close()

        except Exception as e:
            logging.error(f"[DBManager] Failed to save conversation context: {e}")

    @staticmethod
    def save_events(property_id, event_names):
        """Save/update event registry"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()

            for event in event_names:
                c.execute("""
                    INSERT OR REPLACE INTO event_registry
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (property_id, event))

            conn.commit()
            conn.close()

        except Exception as e:
            logging.error(f"[DBManager] Failed to save events: {e}")

    @staticmethod
    def register_file(file_path, file_name, file_type, schema_info, row_count):
        """Register uploaded file in registry"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO file_registry
                (file_path, file_name, file_type, schema_info, row_count, uploaded_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (file_path, file_name, file_type, json.dumps(schema_info), row_count))
            conn.commit()
            conn.close()

        except Exception as e:
            logging.error(f"[DBManager] Failed to register file: {e}")

    @staticmethod
    def get_file_info(file_path):
        """Get file metadata from registry"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT file_name, file_type, schema_info, row_count, uploaded_at
                FROM file_registry
                WHERE file_path = ?
            """, (file_path,))
            row = c.fetchone()
            conn.close()

            if row:
                return {
                    "file_name": row[0],
                    "file_type": row[1],
                    "schema": json.loads(row[2]) if row[2] else {},
                    "row_count": row[3],
                    "uploaded_at": row[4]
                }

        except Exception as e:
            logging.error(f"[DBManager] Failed to get file info: {e}")

        return None

    @staticmethod
    def save_report(user_id, conversation_id, title, content_json):
        """Save a report to the persistent database (v9.5)"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT INTO reports (user_id, conversation_id, title, content_json)
                VALUES (?, ?, ?, ?)
            """, (user_id, conversation_id, title, json.dumps(content_json, ensure_ascii=False)))
            conn.commit()
            conn.close()

            logging.info(f"[DBManager] Report saved: Title='{title}' for User={user_id}")
            return True

        except Exception as e:
            logging.error(f"[DBManager] Failed to save report: {e}")
            return False

    @staticmethod
    def get_reports(user_id):
        """Get all reports for a specific user"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT id, title, created_at
                FROM reports
                WHERE user_id = ?
                ORDER BY created_at DESC
            """, (user_id,))
            reports = []

            for row in c.fetchall():
                reports.append({
                    "id": row[0],
                    "title": row[1],
                    "created_at": row[2]
                })

            conn.close()
            return reports

        except Exception as e:
            logging.error(f"[DBManager] Failed to get reports: {e}")
            return []

    @staticmethod
    def get_report_by_id(report_id):
        """Get a specific report by ID"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT title, content_json, created_at
                FROM reports
                WHERE id = ?
            """, (report_id,))
            row = c.fetchone()
            conn.close()

            if row:
                return {
                    "title": row[0],
                    "content": json.loads(row[1]) if row[1] else "",
                    "created_at": row[2]
                }

        except Exception as e:
            logging.error(f"[DBManager] Failed to get report: {e}")

        return None

    # -------------------------------
    # Follow-up Post-processing 지원
    # -------------------------------
    @staticmethod
    def save_last_result(conversation_id, source, result):
        """Save last GA4/File/Mixed result for follow-up questions"""
        if not conversation_id or not source:
            return

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO last_results
                (conversation_id, source, result_json, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (conversation_id, source, json.dumps(result, ensure_ascii=False)))
            conn.commit()
            conn.close()

            logging.info(f"[DBManager] Last result saved: Conv={conversation_id}, Source={source}")

        except Exception as e:
            logging.error(f"[DBManager] Failed to save last result: {e}")

    @staticmethod
    def load_last_result(conversation_id, source="ga4"):
        """Load last GA4/File/Mixed result for follow-up questions"""
        if not conversation_id:
            return None

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT result_json FROM last_results
                WHERE conversation_id = ? AND source = ?
            """, (conversation_id, source))

            row = c.fetchone()
            conn.close()

            if row:
                return json.loads(row[0])

        except Exception as e:
            logging.error(f"[DBManager] Failed to load last result: {e}")

        return None
