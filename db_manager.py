import sqlite3
import json
import logging
import os
import re
from datetime import datetime

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
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"[DBManager] Failed to log interaction: {e}")

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
