import sqlite3
import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Optional

# DB Path - Absolute Path Fix
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, 'sqlite.db'))


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

        # 8. interaction_logs table (learning dataset)
        c.execute('''CREATE TABLE IF NOT EXISTS interaction_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        conversation_id TEXT,
                        route TEXT,
                        question TEXT,
                        response_json TEXT,
                        has_plot INTEGER DEFAULT 0,
                        has_raw_data INTEGER DEFAULT 0,
                        abstained INTEGER DEFAULT 0,
                        feedback_label TEXT DEFAULT 'unlabeled',
                        feedback_note TEXT,
                        labeled_at DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')

        # 9. qa_failure_logs table (explicit user feedback logs)
        c.execute('''CREATE TABLE IF NOT EXISTS qa_failure_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        conversation_id TEXT,
                        feedback_text TEXT,
                        target_question TEXT,
                        target_response_json TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')

        # 10. webhook_presets table (named webhook storage)
        c.execute('''CREATE TABLE IF NOT EXISTS webhook_presets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        channel TEXT,
                        name TEXT,
                        url TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, channel, name)
                    )''')

        # 11. intelligence_runs table (feature/signal/scoring snapshot)
        c.execute('''CREATE TABLE IF NOT EXISTS intelligence_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_id TEXT,
                        user_id TEXT,
                        conversation_id TEXT,
                        source TEXT,
                        payload_json TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')

        # 12. action_experiments table (execution mode tracking)
        c.execute('''CREATE TABLE IF NOT EXISTS action_experiments (
                        experiment_id TEXT PRIMARY KEY,
                        company_id TEXT,
                        run_id INTEGER,
                        signal_type TEXT,
                        primary_metric TEXT,
                        baseline_value REAL,
                        expected_direction TEXT,
                        related_dimensions_json TEXT,
                        evaluation_date TEXT,
                        status TEXT DEFAULT 'planned',
                        result_json TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')

        # 13. insight_feedback table (selection/rejection/success loop)
        c.execute('''CREATE TABLE IF NOT EXISTS insight_feedback (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        company_id TEXT,
                        run_id INTEGER,
                        experiment_id TEXT,
                        signal_type TEXT,
                        selected INTEGER DEFAULT 0,
                        rejected INTEGER DEFAULT 0,
                        experiment_success INTEGER,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')

        # 14. insight_weights table (company-specific signal weights)
        c.execute('''CREATE TABLE IF NOT EXISTS insight_weights (
                        company_id TEXT,
                        signal_type TEXT,
                        base_weight REAL DEFAULT 0.0,
                        weight REAL DEFAULT 0.0,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(company_id, signal_type)
                    )''')

        # Backward-compatible migration
        try:
            c.execute("ALTER TABLE interaction_logs ADD COLUMN feedback_label TEXT DEFAULT 'unlabeled'")
        except Exception:
            pass
        try:
            c.execute("ALTER TABLE interaction_logs ADD COLUMN feedback_note TEXT")
        except Exception:
            pass
        try:
            c.execute("ALTER TABLE interaction_logs ADD COLUMN labeled_at DATETIME")
        except Exception:
            pass

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
    def _anonymize_text(text: str) -> str:
        if text is None:
            return ""
        out = str(text)
        # 이메일/전화 등 기본 마스킹
        out = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[EMAIL]", out)
        out = re.sub(r"\b01[0-9]-?\d{3,4}-?\d{4}\b", "[PHONE]", out)
        return out

    @staticmethod
    def _ensure_interaction_schema():
        """Ensure new interaction_logs columns exist (safe for old DB files)."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("PRAGMA table_info(interaction_logs)")
            cols = {row[1] for row in (c.fetchall() or [])}
            if "feedback_label" not in cols:
                c.execute("ALTER TABLE interaction_logs ADD COLUMN feedback_label TEXT DEFAULT 'unlabeled'")
            if "feedback_note" not in cols:
                c.execute("ALTER TABLE interaction_logs ADD COLUMN feedback_note TEXT")
            if "labeled_at" not in cols:
                c.execute("ALTER TABLE interaction_logs ADD COLUMN labeled_at DATETIME")
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"[DBManager] Failed to ensure interaction schema: {e}")

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

    # -------------------------------
    # Learning Dataset
    # -------------------------------
    @staticmethod
    def log_interaction(user_id, conversation_id, route, question, response, has_plot=False, has_raw_data=False, abstained=False):
        """Persist a Q/A interaction for continuous prompt/data learning."""
        try:
            DBManager._ensure_interaction_schema()
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT INTO interaction_logs
                (user_id, conversation_id, route, question, response_json, has_plot, has_raw_data, abstained, feedback_label, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'unlabeled', CURRENT_TIMESTAMP)
            """, (
                user_id,
                conversation_id,
                route,
                question,
                json.dumps(response, ensure_ascii=False) if not isinstance(response, str) else response,
                1 if has_plot else 0,
                1 if has_raw_data else 0,
                1 if abstained else 0
            ))
            interaction_id = c.lastrowid
            conn.commit()
            conn.close()
            return int(interaction_id) if interaction_id else None
        except Exception as e:
            logging.error(f"[DBManager] Failed to log interaction: {e}")
            return None

    @staticmethod
    def mark_last_interaction_bad(conversation_id, note=None):
        """Mark latest interaction in a conversation as bad."""
        if not conversation_id:
            return False
        try:
            DBManager._ensure_interaction_schema()
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT id, feedback_note
                FROM interaction_logs
                WHERE conversation_id = ?
                ORDER BY id DESC
                LIMIT 1
            """, (conversation_id,))
            row = c.fetchone()
            if not row:
                conn.close()
                return False
            interaction_id = int(row[0])
            prev_note = str(row[1] or "").strip()
            new_note = str(note or "").strip()
            merged_note = new_note if not prev_note else (prev_note + (" | " + new_note if new_note else ""))
            c.execute("""
                UPDATE interaction_logs
                SET feedback_label = 'bad',
                    feedback_note = ?,
                    labeled_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (merged_note or None, interaction_id))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"[DBManager] Failed to mark last interaction bad: {e}")
            return False

    @staticmethod
    def log_failure_feedback(user_id, conversation_id, feedback_text, target_question=None, target_response=None):
        """Persist explicit user failure feedback for regression datasets."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT INTO qa_failure_logs
                (user_id, conversation_id, feedback_text, target_question, target_response_json, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                user_id,
                conversation_id,
                str(feedback_text or ""),
                str(target_question or "") if target_question is not None else None,
                json.dumps(target_response, ensure_ascii=False) if isinstance(target_response, (dict, list)) else (str(target_response) if target_response is not None else None)
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"[DBManager] Failed to log failure feedback: {e}")
            return False

    @staticmethod
    def get_learning_status(user_id, days=30):
        """Return summary stats of accumulated learning data."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()

            c.execute("""
                SELECT COUNT(*),
                       SUM(CASE WHEN has_plot=1 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN has_raw_data=1 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN abstained=1 THEN 1 ELSE 0 END)
                FROM interaction_logs
                WHERE user_id = ?
                  AND created_at >= datetime('now', ?)
            """, (user_id, f"-{int(days)} days"))
            row = c.fetchone() or (0, 0, 0, 0)

            c.execute("""
                SELECT route, COUNT(*)
                FROM interaction_logs
                WHERE user_id = ?
                  AND created_at >= datetime('now', ?)
                GROUP BY route
                ORDER BY COUNT(*) DESC
            """, (user_id, f"-{int(days)} days"))
            route_rows = c.fetchall() or []
            conn.close()

            total = int(row[0] or 0)
            abstained = int(row[3] or 0)
            return {
                "total_interactions": total,
                "with_plot": int(row[1] or 0),
                "with_raw_data": int(row[2] or 0),
                "abstained": abstained,
                "abstain_rate": round((abstained / total) * 100, 2) if total else 0.0,
                "routes": [{"route": r[0], "count": int(r[1])} for r in route_rows],
            }
        except Exception as e:
            logging.error(f"[DBManager] Failed to get learning status: {e}")
            return {
                "total_interactions": 0,
                "with_plot": 0,
                "with_raw_data": 0,
                "abstained": 0,
                "abstain_rate": 0.0,
                "routes": [],
            }

    @staticmethod
    def get_recent_learning_samples(user_id, limit=50):
        """Return recent interaction samples for evaluation/fine-tuning prep."""
        try:
            DBManager._ensure_interaction_schema()
            lim = max(1, min(int(limit), 500))
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT id, route, question, response_json, has_plot, has_raw_data, abstained, created_at,
                       feedback_label, feedback_note, labeled_at
                FROM interaction_logs
                WHERE user_id = ?
                ORDER BY id DESC
                LIMIT ?
            """, (user_id, lim))
            rows = c.fetchall() or []
            conn.close()

            samples = []
            for r in rows:
                try:
                    response = json.loads(r[3]) if r[3] else {}
                except Exception:
                    response = r[3]
                samples.append({
                    "id": r[0],
                    "route": r[1],
                    "question": r[2],
                    "response": response,
                    "has_plot": bool(r[4]),
                    "has_raw_data": bool(r[5]),
                    "abstained": bool(r[6]),
                    "created_at": r[7],
                    "feedback_label": r[8] if len(r) > 8 else "unlabeled",
                    "feedback_note": r[9] if len(r) > 9 else None,
                    "labeled_at": r[10] if len(r) > 10 else None
                })
            return samples
        except Exception as e:
            logging.error(f"[DBManager] Failed to get learning samples: {e}")
            return []

    @staticmethod
    def export_training_examples(
        user_id=None,
        days=30,
        limit=5000,
        include_abstained=False,
        label_filter="good",
        include_unlabeled=False
    ):
        """Export anonymized instruction-response pairs for re-training."""
        try:
            DBManager._ensure_interaction_schema()
            lim = max(1, min(int(limit), 100000))
            params = []
            where = ["created_at >= datetime('now', ?)"]
            params.append(f"-{max(1, int(days))} days")

            if user_id:
                where.append("user_id = ?")
                params.append(user_id)
            if not include_abstained:
                where.append("abstained = 0")
            valid_labels = {"good", "bad", "unknown", "unlabeled"}
            lf = str(label_filter or "").strip().lower()
            if lf == "all":
                pass
            elif lf in valid_labels:
                where.append("feedback_label = ?")
                params.append(lf)
            elif include_unlabeled:
                where.append("feedback_label IN ('good','unlabeled')")
            else:
                where.append("feedback_label = 'good'")

            query = f"""
                SELECT id, user_id, conversation_id, route, question, response_json, created_at, feedback_label
                FROM interaction_logs
                WHERE {' AND '.join(where)}
                ORDER BY id DESC
                LIMIT ?
            """
            params.append(lim)

            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(query, tuple(params))
            rows = c.fetchall() or []
            conn.close()

            examples = []
            for r in rows:
                q = DBManager._anonymize_text(r[4] or "")
                resp_raw = r[5]
                answer = ""
                try:
                    payload = json.loads(resp_raw) if resp_raw else {}
                    if isinstance(payload, dict):
                        answer = payload.get("message") or json.dumps(payload, ensure_ascii=False)
                    else:
                        answer = str(payload)
                except Exception:
                    answer = str(resp_raw or "")
                answer = DBManager._anonymize_text(answer)
                if not q or not answer:
                    continue
                examples.append({
                    "instruction": q,
                    "output": answer,
                    "metadata": {
                        "id": r[0],
                        "route": r[3],
                        "conversation_id": r[2],
                        "created_at": r[6],
                        "feedback_label": r[7] if len(r) > 7 else "unlabeled",
                    }
                })
            return examples
        except Exception as e:
            logging.error(f"[DBManager] Failed to export training examples: {e}")
            return []

    @staticmethod
    def prune_old_interactions(retention_days=180):
        """Delete interaction logs older than retention_days."""
        try:
            DBManager._ensure_interaction_schema()
            days = max(1, int(retention_days))
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                DELETE FROM interaction_logs
                WHERE created_at < datetime('now', ?)
            """, (f"-{days} days",))
            deleted = c.rowcount or 0
            conn.commit()
            conn.close()
            return int(deleted)
        except Exception as e:
            logging.error(f"[DBManager] Failed to prune interactions: {e}")
            return 0

    @staticmethod
    def set_interaction_label(interaction_id, label, note=None):
        """Set manual quality label for one interaction."""
        allowed = {"good", "bad", "unknown", "unlabeled"}
        lb = str(label or "").strip().lower()
        if lb not in allowed:
            return False
        try:
            DBManager._ensure_interaction_schema()
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                UPDATE interaction_logs
                SET feedback_label = ?, feedback_note = ?, labeled_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (lb, (note or "")[:1000], int(interaction_id)))
            updated = c.rowcount or 0
            conn.commit()
            conn.close()
            return updated > 0
        except Exception as e:
            logging.error(f"[DBManager] Failed to set interaction label: {e}")
            return False

    @staticmethod
    def get_label_status(user_id=None, days=30):
        """Get label distribution stats."""
        try:
            DBManager._ensure_interaction_schema()
            params = [f"-{max(1, int(days))} days"]
            where = ["created_at >= datetime('now', ?)"]
            if user_id:
                where.append("user_id = ?")
                params.append(user_id)
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(f"""
                SELECT feedback_label, COUNT(*)
                FROM interaction_logs
                WHERE {' AND '.join(where)}
                GROUP BY feedback_label
            """, tuple(params))
            rows = c.fetchall() or []
            conn.close()
            counts = {str(r[0] or "unlabeled"): int(r[1]) for r in rows}
            total = sum(counts.values())
            return {
                "total": total,
                "counts": counts,
                "good_rate": round((counts.get("good", 0) / total) * 100, 2) if total else 0.0
            }
        except Exception as e:
            logging.error(f"[DBManager] Failed to get label status: {e}")
            return {"total": 0, "counts": {}, "good_rate": 0.0}

    @staticmethod
    def get_regression_snapshot(user_id=None, days=14, limit=200):
        """Aggregate recent good/bad signals for regression monitoring."""
        try:
            DBManager._ensure_interaction_schema()
            d = max(1, min(int(days), 3650))
            lim = max(10, min(int(limit), 5000))
            params = [f"-{d} days"]
            where = ["created_at >= datetime('now', ?)"]
            if user_id:
                where.append("user_id = ?")
                params.append(user_id)

            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()

            c.execute(f"""
                SELECT route, feedback_label, COUNT(*)
                FROM interaction_logs
                WHERE {' AND '.join(where)}
                GROUP BY route, feedback_label
            """, tuple(params))
            rows = c.fetchall() or []

            c.execute(f"""
                SELECT question, COUNT(*) AS cnt
                FROM interaction_logs
                WHERE {' AND '.join(where)}
                  AND feedback_label = 'bad'
                  AND question IS NOT NULL
                  AND TRIM(question) != ''
                GROUP BY question
                ORDER BY cnt DESC
                LIMIT ?
            """, tuple(params + [lim]))
            bad_q_rows = c.fetchall() or []
            conn.close()

            by_route = {}
            total = 0
            good = 0
            bad = 0
            for r, lb, cnt in rows:
                rt = str(r or "unknown")
                lb2 = str(lb or "unlabeled")
                cnum = int(cnt or 0)
                total += cnum
                if lb2 == "good":
                    good += cnum
                if lb2 == "bad":
                    bad += cnum
                by_route.setdefault(rt, {}).setdefault(lb2, 0)
                by_route[rt][lb2] += cnum

            route_summary = []
            for rt, obj in by_route.items():
                r_total = sum(obj.values())
                r_bad = int(obj.get("bad", 0))
                route_summary.append({
                    "route": rt,
                    "total": r_total,
                    "good": int(obj.get("good", 0)),
                    "bad": r_bad,
                    "bad_rate": round((r_bad / r_total) * 100, 2) if r_total else 0.0
                })
            route_summary.sort(key=lambda x: (-x["bad_rate"], -x["total"]))

            return {
                "days": d,
                "total": total,
                "good": good,
                "bad": bad,
                "good_rate": round((good / total) * 100, 2) if total else 0.0,
                "bad_rate": round((bad / total) * 100, 2) if total else 0.0,
                "route_summary": route_summary,
                "top_bad_questions": [{"question": str(q), "count": int(c)} for q, c in bad_q_rows]
            }
        except Exception as e:
            logging.error(f"[DBManager] Failed to get regression snapshot: {e}")
            return {
                "days": max(1, int(days)),
                "total": 0,
                "good": 0,
                "bad": 0,
                "good_rate": 0.0,
                "bad_rate": 0.0,
                "route_summary": [],
                "top_bad_questions": []
            }

    @staticmethod
    def get_matching_status(user_id, days=30):
        """Return local-LLM matching contribution stats from logged responses."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()

            c.execute("""
                SELECT COUNT(*)
                FROM interaction_logs
                WHERE user_id = ?
                  AND created_at >= datetime('now', ?)
                  AND response_json LIKE '%"matching_debug"%'
            """, (user_id, f"-{int(days)} days"))
            logged = int((c.fetchone() or [0])[0] or 0)

            c.execute("""
                SELECT COUNT(*)
                FROM interaction_logs
                WHERE user_id = ?
                  AND created_at >= datetime('now', ?)
                  AND response_json LIKE '%"local_llm_used": true%'
            """, (user_id, f"-{int(days)} days"))
            local_used = int((c.fetchone() or [0])[0] or 0)

            c.execute("""
                SELECT COUNT(*)
                FROM interaction_logs
                WHERE user_id = ?
                  AND created_at >= datetime('now', ?)
                  AND response_json LIKE '%"local_parser_enabled": true%'
            """, (user_id, f"-{int(days)} days"))
            parser_enabled = int((c.fetchone() or [0])[0] or 0)
            conn.close()

            return {
                "responses_with_matching_debug": logged,
                "responses_with_local_parser_enabled": parser_enabled,
                "responses_with_local_llm_used": local_used,
                "local_llm_used_rate": round((local_used / logged) * 100, 2) if logged else 0.0,
            }
        except Exception as e:
            logging.error(f"[DBManager] Failed to get matching status: {e}")
            return {
                "responses_with_matching_debug": 0,
                "responses_with_local_parser_enabled": 0,
                "responses_with_local_llm_used": 0,
                "local_llm_used_rate": 0.0,
            }

    @staticmethod
    def get_recent_bad_questions(user_id, limit=200):
        """Return recent bad-labeled user questions for regression guardrails."""
        try:
            lim = max(1, min(int(limit), 2000))
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT question
                FROM interaction_logs
                WHERE user_id = ?
                  AND feedback_label = 'bad'
                  AND question IS NOT NULL
                  AND TRIM(question) != ''
                ORDER BY id DESC
                LIMIT ?
            """, (user_id, lim))
            rows = c.fetchall() or []
            conn.close()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception as e:
            logging.error(f"[DBManager] Failed to get recent bad questions: {e}")
            return []

    @staticmethod
    def get_labeled_route_hint(user_id, question, days=90, limit=500) -> Dict[str, Any]:
        """
        Build a lightweight route hint from recent good/bad labeled interactions.
        Returns: {"route": "ga4|file|mixed", "score": float, "similarity": float} or {}.
        """
        try:
            q = str(question or "").strip().lower()
            if not q:
                return {}

            def tok(s: str):
                return {t for t in re.findall(r"[A-Za-z0-9가-힣_]+", str(s or "").lower()) if len(t) >= 2}

            qtok = tok(q)
            if not qtok:
                return {}

            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT route, question, feedback_label
                FROM interaction_logs
                WHERE user_id = ?
                  AND created_at >= datetime('now', ?)
                  AND feedback_label IN ('good','bad')
                  AND route IN ('ga4','ga4_followup','file','mixed')
                  AND question IS NOT NULL
                  AND TRIM(question) != ''
                ORDER BY id DESC
                LIMIT ?
            """, (str(user_id), f"-{int(days)} days", int(max(10, min(limit, 2000)))))
            rows = c.fetchall() or []
            conn.close()

            if not rows:
                return {}

            route_scores: Dict[str, float] = {"ga4": 0.0, "file": 0.0, "mixed": 0.0}
            best_sim = 0.0
            best_route = None
            for r, q2, lb in rows:
                r0 = "ga4" if str(r) == "ga4_followup" else str(r)
                if r0 not in route_scores:
                    continue
                s2 = tok(str(q2))
                if not s2:
                    continue
                inter = len(qtok & s2)
                union = len(qtok | s2)
                sim = (inter / union) if union else 0.0
                if sim < 0.15:
                    continue
                weight = 1.0 if str(lb) == "good" else -1.0
                route_scores[r0] += (sim * weight)
                if sim > best_sim:
                    best_sim = sim
                    best_route = r0

            ranked = sorted(route_scores.items(), key=lambda x: x[1], reverse=True)
            top_route, top_score = ranked[0]
            if top_score <= 0 and best_route is None:
                return {}
            return {
                "route": best_route or top_route,
                "score": round(float(top_score), 4),
                "similarity": round(float(best_sim), 4)
            }
        except Exception as e:
            logging.error(f"[DBManager] Failed to get labeled route hint: {e}")
            return {}

    @staticmethod
    def list_webhook_presets(user_id, channel=None):
        """List saved webhook presets for a user."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            if channel:
                c.execute("""
                    SELECT id, channel, name, url, created_at, updated_at
                    FROM webhook_presets
                    WHERE user_id = ? AND channel = ?
                    ORDER BY name ASC
                """, (user_id, str(channel)))
            else:
                c.execute("""
                    SELECT id, channel, name, url, created_at, updated_at
                    FROM webhook_presets
                    WHERE user_id = ?
                    ORDER BY channel ASC, name ASC
                """, (user_id,))
            rows = c.fetchall() or []
            conn.close()
            return [
                {
                    "id": int(r[0]),
                    "channel": str(r[1]),
                    "name": str(r[2]),
                    "url": str(r[3]),
                    "created_at": r[4],
                    "updated_at": r[5]
                } for r in rows
            ]
        except Exception as e:
            logging.error(f"[DBManager] Failed to list webhook presets: {e}")
            return []

    @staticmethod
    def save_webhook_preset(user_id, channel, name, url):
        """Save or update a named webhook preset."""
        try:
            if not user_id or not channel or not name or not url:
                return False
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT INTO webhook_presets (user_id, channel, name, url, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id, channel, name)
                DO UPDATE SET
                    url=excluded.url,
                    updated_at=CURRENT_TIMESTAMP
            """, (str(user_id), str(channel), str(name), str(url)))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"[DBManager] Failed to save webhook preset: {e}")
            return False

    @staticmethod
    def delete_webhook_preset(user_id, preset_id):
        """Delete one webhook preset by id (scoped by user)."""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                DELETE FROM webhook_presets
                WHERE id = ? AND user_id = ?
            """, (int(preset_id), str(user_id)))
            deleted = c.rowcount or 0
            conn.commit()
            conn.close()
            return deleted > 0
        except Exception as e:
            logging.error(f"[DBManager] Failed to delete webhook preset: {e}")
            return False

    @staticmethod
    def _default_signal_weights() -> Dict[str, float]:
        return {
            "kpi_drop": 1.0,
            "kpi_rise": 0.8,
            "efficiency_distortion": 1.2,
            "contribution_shift": 1.0,
            "funnel_break": 1.2,
            "opportunity_segment": 0.9,
        }

    @staticmethod
    def ensure_company_signal_weights(company_id: str):
        cid = str(company_id or "default").strip() or "default"
        defaults = DBManager._default_signal_weights()
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            for signal_type, base in defaults.items():
                c.execute(
                    """
                    INSERT OR IGNORE INTO insight_weights
                    (company_id, signal_type, base_weight, weight, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (cid, signal_type, float(base), float(base)),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"[DBManager] Failed to ensure company signal weights: {e}")

    @staticmethod
    def get_company_signal_weights(company_id: str) -> Dict[str, float]:
        cid = str(company_id or "default").strip() or "default"
        DBManager.ensure_company_signal_weights(cid)
        weights: Dict[str, float] = {}
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                SELECT signal_type, weight
                FROM insight_weights
                WHERE company_id = ?
                """,
                (cid,),
            )
            for signal_type, weight in (c.fetchall() or []):
                weights[str(signal_type)] = float(weight or 0.0)
            conn.close()
        except Exception as e:
            logging.error(f"[DBManager] Failed to get company signal weights: {e}")
        if not weights:
            weights = DBManager._default_signal_weights()
        return weights

    @staticmethod
    def get_signal_weight_snapshot(company_id: str) -> List[Dict[str, Any]]:
        cid = str(company_id or "default").strip() or "default"
        DBManager.ensure_company_signal_weights(cid)
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                SELECT signal_type, base_weight, weight, updated_at
                FROM insight_weights
                WHERE company_id = ?
                ORDER BY signal_type ASC
                """,
                (cid,),
            )
            rows = c.fetchall() or []
            conn.close()
            return [
                {
                    "signal_type": str(r[0]),
                    "base_weight": float(r[1] or 0.0),
                    "weight": float(r[2] or 0.0),
                    "updated_at": r[3],
                }
                for r in rows
            ]
        except Exception as e:
            logging.error(f"[DBManager] Failed to get signal weight snapshot: {e}")
            return []

    @staticmethod
    def save_intelligence_run(
        company_id: str,
        user_id: str,
        conversation_id: str,
        source: str,
        payload: Dict[str, Any],
    ) -> Optional[int]:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO intelligence_runs
                (company_id, user_id, conversation_id, source, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    str(company_id or "default"),
                    str(user_id or "anonymous"),
                    str(conversation_id or ""),
                    str(source or "unknown"),
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )
            run_id = c.lastrowid
            conn.commit()
            conn.close()
            return int(run_id)
        except Exception as e:
            logging.error(f"[DBManager] Failed to save intelligence run: {e}")
            return None

    @staticmethod
    def get_intelligence_run(run_id: int) -> Optional[Dict[str, Any]]:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                SELECT id, company_id, user_id, conversation_id, source, payload_json, created_at
                FROM intelligence_runs
                WHERE id = ?
                """,
                (int(run_id),),
            )
            row = c.fetchone()
            conn.close()
            if not row:
                return None
            return {
                "id": int(row[0]),
                "company_id": str(row[1] or "default"),
                "user_id": str(row[2] or "anonymous"),
                "conversation_id": str(row[3] or ""),
                "source": str(row[4] or "unknown"),
                "payload": json.loads(row[5] or "{}"),
                "created_at": row[6],
            }
        except Exception as e:
            logging.error(f"[DBManager] Failed to get intelligence run: {e}")
            return None

    @staticmethod
    def create_action_experiment(
        experiment_id: str,
        company_id: str,
        run_id: int,
        signal_type: str,
        primary_metric: str,
        baseline_value: float,
        expected_direction: str,
        related_dimensions: List[str],
        evaluation_date: str,
    ) -> bool:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                INSERT OR REPLACE INTO action_experiments
                (experiment_id, company_id, run_id, signal_type, primary_metric, baseline_value,
                 expected_direction, related_dimensions_json, evaluation_date, status, result_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    str(experiment_id),
                    str(company_id or "default"),
                    int(run_id) if run_id is not None else None,
                    str(signal_type or ""),
                    str(primary_metric or ""),
                    float(baseline_value or 0.0),
                    str(expected_direction or ""),
                    json.dumps(related_dimensions or [], ensure_ascii=False),
                    str(evaluation_date or ""),
                ),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"[DBManager] Failed to create action experiment: {e}")
            return False

    @staticmethod
    def get_action_experiment(experiment_id: str) -> Optional[Dict[str, Any]]:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                SELECT experiment_id, company_id, run_id, signal_type, primary_metric, baseline_value,
                       expected_direction, related_dimensions_json, evaluation_date, status, result_json,
                       created_at, updated_at
                FROM action_experiments
                WHERE experiment_id = ?
                """,
                (str(experiment_id),),
            )
            row = c.fetchone()
            conn.close()
            if not row:
                return None
            return {
                "experiment_id": str(row[0]),
                "company_id": str(row[1] or "default"),
                "run_id": int(row[2]) if row[2] is not None else None,
                "signal_type": str(row[3] or ""),
                "primary_metric": str(row[4] or ""),
                "baseline_value": float(row[5] or 0.0),
                "expected_direction": str(row[6] or ""),
                "related_dimensions": json.loads(row[7] or "[]"),
                "evaluation_date": str(row[8] or ""),
                "status": str(row[9] or "planned"),
                "result": json.loads(row[10]) if row[10] else None,
                "created_at": row[11],
                "updated_at": row[12],
            }
        except Exception as e:
            logging.error(f"[DBManager] Failed to get action experiment: {e}")
            return None

    @staticmethod
    def update_action_experiment_result(experiment_id: str, result: Dict[str, Any], status: str = "evaluated") -> bool:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                UPDATE action_experiments
                SET result_json = ?, status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE experiment_id = ?
                """,
                (json.dumps(result or {}, ensure_ascii=False), str(status or "evaluated"), str(experiment_id)),
            )
            updated = c.rowcount or 0
            conn.commit()
            conn.close()
            return updated > 0
        except Exception as e:
            logging.error(f"[DBManager] Failed to update action experiment result: {e}")
            return False

    @staticmethod
    def log_insight_feedback(
        company_id: str,
        signal_type: str,
        selected: bool = False,
        rejected: bool = False,
        experiment_success: Optional[bool] = None,
        experiment_id: Optional[str] = None,
        run_id: Optional[int] = None,
    ) -> bool:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO insight_feedback
                (company_id, run_id, experiment_id, signal_type, selected, rejected, experiment_success, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    str(company_id or "default"),
                    int(run_id) if run_id is not None else None,
                    str(experiment_id) if experiment_id else None,
                    str(signal_type or ""),
                    1 if bool(selected) else 0,
                    1 if bool(rejected) else 0,
                    None if experiment_success is None else (1 if bool(experiment_success) else 0),
                ),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"[DBManager] Failed to log insight feedback: {e}")
            return False

    @staticmethod
    def get_run_rejected_signal_types(run_id: int) -> List[str]:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                SELECT DISTINCT signal_type
                FROM insight_feedback
                WHERE run_id = ? AND rejected = 1 AND signal_type IS NOT NULL
                """,
                (int(run_id),),
            )
            rows = c.fetchall() or []
            conn.close()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception as e:
            logging.error(f"[DBManager] Failed to get run rejected signal types: {e}")
            return []

    @staticmethod
    def recalculate_signal_weights(company_id: str) -> List[Dict[str, Any]]:
        cid = str(company_id or "default").strip() or "default"
        DBManager.ensure_company_signal_weights(cid)
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()

            c.execute(
                """
                SELECT signal_type, base_weight
                FROM insight_weights
                WHERE company_id = ?
                """,
                (cid,),
            )
            base_rows = c.fetchall() or []
            for signal_type, base_weight in base_rows:
                st = str(signal_type or "")
                base = float(base_weight or 0.0)
                c.execute(
                    """
                    SELECT
                        COALESCE(SUM(CASE WHEN selected = 1 THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN rejected = 1 THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN experiment_success = 1 THEN 1 ELSE 0 END), 0)
                    FROM insight_feedback
                    WHERE company_id = ? AND signal_type = ?
                    """,
                    (cid, st),
                )
                cnt = c.fetchone() or (0, 0, 0)
                selection_count, rejection_count, success_count = [int(x or 0) for x in cnt]
                new_weight = (
                    base
                    + (selection_count * 0.1)
                    - (rejection_count * 0.1)
                    + (success_count * 0.2)
                )
                c.execute(
                    """
                    UPDATE insight_weights
                    SET weight = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE company_id = ? AND signal_type = ?
                    """,
                    (float(new_weight), cid, st),
                )

            conn.commit()
            conn.close()
            return DBManager.get_signal_weight_snapshot(cid)
        except Exception as e:
            logging.error(f"[DBManager] Failed to recalculate signal weights: {e}")
            return DBManager.get_signal_weight_snapshot(cid)
