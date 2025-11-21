import os
from datetime import datetime
import bcrypt
import urllib.parse
import json
import psycopg2
from psycopg2 import pool 
import logging
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import traceback

load_dotenv()

class PGDB:
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if PGDB._pool is not None:
            return  # Already initialized
            
        self.connection_string = os.getenv('DATABASE_URL')
        
        # Create pool ONCE
        PGDB._pool = pool.SimpleConnectionPool(
            5, 50, self.connection_string
        )
        
        # Create tables ONCE
        self.create_users_table()
        self.create_agents_table()  # NEW: Agents table
        self.create_call_history_table()

    def get_connection(self):
        """Get connection from pool"""
        return PGDB._pool.getconn()
    
    def release_connection(self, conn):
        """Return connection to pool"""
        PGDB._pool.putconn(conn)

    # ==================== NEW: AGENTS TABLE ====================
    def create_agents_table(self):
        """
        Create agents table with voice_type, owner_name, and avatar_url.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS agents (
                        id SERIAL PRIMARY KEY,
                        phone_number VARCHAR(20) UNIQUE NOT NULL,
                        agent_name VARCHAR(100) NOT NULL,
                        system_prompt TEXT NOT NULL,
                        voice_type VARCHAR(20) DEFAULT 'female',
                        owner_name VARCHAR(100),
                        avatar_url TEXT,
                        language VARCHAR(10) DEFAULT 'en',
                        industry VARCHAR(50),
                        admin_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_agents_phone 
                    ON agents(phone_number);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_agents_admin 
                    ON agents(admin_id);
                """)
            conn.commit()
            logging.info("✅ agents table created with avatar_url")
        except Exception as e:
            logging.error(f"Error creating agents table: {e}")
        finally:
            self.release_connection(conn)

    

    def get_agent_by_phone(self, phone_number: str):
        """
        Get specific agent details by phone number
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        id AS agent_id,  -- <--- THIS IS THE FIX
                        agent_name, 
                        industry, 
                        system_prompt, 
                        voice_type,
                        language
                    FROM agents 
                    WHERE phone_number = %s AND is_active = TRUE
                    LIMIT 1
                """, (phone_number,))
                return cursor.fetchone()
        finally:
            self.release_connection(conn)

    def get_agents_by_admin(self, admin_id: int):
        """Get all agents for a specific admin"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM agents 
                    WHERE admin_id = %s
                    ORDER BY created_at DESC
                """, (admin_id,))
                return cursor.fetchall()
        finally:
            self.release_connection(conn)

    

    def delete_agent(self, agent_id: int, admin_id: int):
        """Delete agent (soft delete by setting is_active=False)"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE agents 
                    SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND admin_id = %s
                    RETURNING id;
                """, (agent_id, admin_id))
                row = cursor.fetchone()
            conn.commit()
            return bool(row)
        except Exception as e:
            conn.rollback()
            logging.error(f"Error deleting agent: {e}")
            raise
        finally:
            self.release_connection(conn)

    # ==================== USERS TABLE ====================
    def create_users_table(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(100),
                        email VARCHAR(100) UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        first_name VARCHAR(100),
                        last_name VARCHAR(100),
                        is_admin BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            conn.commit()
        except Exception as e:
            logging.error(f"Error creating users table: {e}")
        finally:
            self.release_connection(conn)

    def register_user(self, user_data):
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check if email already exists
                cursor.execute("SELECT id FROM users WHERE email = %s", (user_data['email'],))
                if cursor.fetchone():
                    raise ValueError("Email already registered.")

                # Hash the password
                hashed_password = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt())

                # Insert user
                cursor.execute("""
                    INSERT INTO users (username, email, password_hash, is_admin)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, username, email, created_at, is_admin;
                """, (
                    user_data['username'],
                    user_data['email'],
                    hashed_password.decode('utf-8'),
                    user_data.get('is_admin', False)
                ))

                row = cursor.fetchone()
                user_id = row["id"]
                conn.commit()

                return {
                    "id": row["id"],
                    "username": row["username"],
                    "email": row["email"],
                    "created_at": row["created_at"]
                }

        except Exception as e:
            conn.rollback()
            logging.error(f"Error in register_user: {e}")
            raise
        finally:
            self.release_connection(conn)

    def login_user(self, user_data):
        """Verify user credentials by username or email and return user info."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, username, email, password_hash, first_name, last_name, created_at, is_admin
                    FROM users
                    WHERE username = %s OR email = %s
                    LIMIT 1
                """, (user_data.get("username"), user_data['email']))

                result = cursor.fetchone()

                if result and bcrypt.checkpw(user_data['password'].encode('utf-8'), result[3].encode('utf-8')):
                    return {
                        "id": result[0],
                        "username": result[1],
                        "email": result[2],
                        "created_at": result[6],
                        "is_admin": result[7]
                    }
                else:
                    raise ValueError("Invalid username or password.")
        except Exception as e:
            logging.error(f"Error during login: {str(e)}")
            raise
        finally:
            self.release_connection(conn)

    def get_user_by_id(self, user_id: int):
        """Get user by ID"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, first_name, last_name, username, email, is_admin, created_at FROM users WHERE id = %s",
                    (user_id,)
                )
                return cursor.fetchone()
        finally:
            self.release_connection(conn)

    # ==================== CALL HISTORY ====================
    def create_call_history_table(self):
        """Create call_history table to store call details"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS call_history (
                        id SERIAL PRIMARY KEY,
                        agent_id INTEGER REFERENCES agents(id) ON DELETE CASCADE,
                        call_id TEXT NOT NULL UNIQUE,
                        caller_number TEXT,
                        status TEXT,
                        duration DOUBLE PRECISION,  
                        transcript JSONB,
                        summary TEXT,
                        recording_url TEXT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        started_at TIMESTAMPTZ NULL,
                        ended_at TIMESTAMPTZ NULL,
                        transcript_url TEXT,
                        transcript_blob TEXT,
                        recording_blob TEXT,
                        events_log JSONB DEFAULT '[]',
                        agent_events JSONB DEFAULT '[]'
                    );
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_history_agent_id ON call_history(agent_id);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_history_events_log ON call_history USING GIN (events_log);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_history_agent_events ON call_history USING GIN (agent_events);")
            conn.commit()
        except Exception as e:
            logging.error(f"Error creating call_history table: {e}")
        finally:
            self.release_connection(conn)

    def generate_presigned_url(blob_path: str, expiration: int = 3600) -> str:
        """
        Generate presigned URL for Hetzner object.
        
        Args:
            blob_path: Object key in bucket (e.g., "avatars/abc.jpg")
            expiration: URL validity in seconds (default 1 hour)
        
        Returns:
            Presigned URL string
        """
        try:
            s3_client = get_s3_client()
            bucket_name = os.getenv("HETZNER_BUCKET_NAME")
            
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': blob_path},
                ExpiresIn=expiration
            )
            
            logging.info(f"✅ Generated presigned URL (expires in {expiration}s): {blob_path}")
            return url
            
        except Exception as e:
            logging.error(f"❌ Failed to generate presigned URL: {e}")
            return None

    def insert_call_history(
        self,
        agent_id: int,
        call_id: str,
        status: str = None,
        caller_number: str = None
    ):
        """Insert a new call history record with initial data"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO call_history (
                        agent_id, call_id, status, caller_number
                    )
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                """, (agent_id, call_id, status, caller_number))

                row = cursor.fetchone()
                conn.commit()
                return row[0] if row else None

        except Exception as e:
            logging.error(f"Error inserting call history: {e}")
            conn.rollback()
            raise
        finally:
            self.release_connection(conn)

    def update_call_history(self, call_id: str, updates: dict):
        """Update specific fields in the call_history record based on the call_id"""
        if not updates:
            logging.warning("update_call_history called with no updates.")
            return None

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                set_clauses = []
                param_values = []
                for key, value in updates.items():
                    if not key.replace('_', '').isalnum():
                        logging.error(f"Invalid column name detected: {key}")
                        raise ValueError(f"Invalid column name: {key}")

                    if key == 'transcript' and value is not None:
                        set_clauses.append(f"{key} = %s")
                        param_values.append(json.dumps(value))
                    else:
                        set_clauses.append(f"{key} = %s")
                        param_values.append(value)

                if not set_clauses:
                    logging.warning("No valid fields to update.")
                    return None

                set_sql = ", ".join(set_clauses)
                sql = f"UPDATE call_history SET {set_sql} WHERE call_id = %s RETURNING id;"
                param_values.append(call_id)

                cursor.execute(sql, tuple(param_values))
                row = cursor.fetchone()
                conn.commit()
                logging.info(f"Updated call_history for call_id {call_id}. Updated fields: {list(updates.keys())}")
                return row[0] if row else None

        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating call history for call_id={call_id}: {e}")
            traceback.print_exc()
            raise
        finally:
            self.release_connection(conn)

    def get_call_history_by_agent(self, agent_id: int, page: int = 1, page_size: int = 10):
        """Get paginated call history for a specific agent"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Count total records
                cursor.execute("SELECT COUNT(*) FROM call_history WHERE agent_id = %s", (agent_id,))
                total = cursor.fetchone()["count"]

                # Count completed
                cursor.execute("""
                    SELECT COUNT(*) FROM call_history 
                    WHERE agent_id = %s AND status = 'completed'
                """, (agent_id,))
                completed_calls = cursor.fetchone()["count"]
                not_completed_calls = total - completed_calls

                # Paginated query
                offset = (page - 1) * page_size
                cursor.execute("""
                    SELECT ch.*, a.agent_name, a.phone_number
                    FROM call_history ch
                    JOIN agents a ON ch.agent_id = a.id
                    WHERE ch.agent_id = %s
                    ORDER BY ch.created_at DESC
                    LIMIT %s OFFSET %s
                """, (agent_id, page_size, offset))

                rows = cursor.fetchall()

                # Parse transcripts
                for row in rows:
                    if isinstance(row["transcript"], str):
                        try:
                            row["transcript"] = json.loads(row["transcript"])
                        except Exception:
                            logging.warning(f"Invalid JSON in transcript for call_id={row['call_id']}")

                return {
                    "calls": rows,
                    "total": total,
                    "completed_calls": completed_calls,
                    "not_completed_calls": not_completed_calls,
                    "page": page,
                    "page_size": page_size
                }
        except Exception as e:
            logging.error(f"Error fetching call history for agent_id={agent_id}: {e}")
            raise
        finally:
            self.release_connection(conn)

    def get_call_history_by_admin(self, admin_id: int, page: int = 1, page_size: int = 10):
        """Get paginated call history for all agents under an admin"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Count total records
                cursor.execute("""
                    SELECT COUNT(*) FROM call_history ch
                    JOIN agents a ON ch.agent_id = a.id
                    WHERE a.admin_id = %s
                """, (admin_id,))
                total = cursor.fetchone()["count"]

                # Count completed
                cursor.execute("""
                    SELECT COUNT(*) FROM call_history ch
                    JOIN agents a ON ch.agent_id = a.id
                    WHERE a.admin_id = %s AND ch.status = 'completed'
                """, (admin_id,))
                completed_calls = cursor.fetchone()["count"]
                not_completed_calls = total - completed_calls

                # Paginated query
                offset = (page - 1) * page_size
                cursor.execute("""
                    SELECT ch.*, a.agent_name, a.phone_number
                    FROM call_history ch
                    JOIN agents a ON ch.agent_id = a.id
                    WHERE a.admin_id = %s
                    ORDER BY ch.created_at DESC
                    LIMIT %s OFFSET %s
                """, (admin_id, page_size, offset))

                rows = cursor.fetchall()

                # Parse transcripts
                for row in rows:
                    if isinstance(row["transcript"], str):
                        try:
                            row["transcript"] = json.loads(row["transcript"])
                        except Exception:
                            pass

                return {
                    "calls": rows,
                    "total": total,
                    "completed_calls": completed_calls,
                    "not_completed_calls": not_completed_calls,
                    "page": page,
                    "page_size": page_size
                }
        except Exception as e:
            logging.error(f"Error fetching call history for admin_id={admin_id}: {e}")
            raise
        finally:
            self.release_connection(conn)

    # def store_recording_blob(self, call_id: str, recording_data: bytes, content_type: str = "audio/ogg"):
    #     """Store actual recording bytes"""
    #     conn = self.get_connection()
    #     try:
    #         with conn.cursor() as cursor:
    #             cursor.execute("""
    #                 UPDATE call_history
    #                 SET recording_blob_data = %s,
    #                     recording_size = %s,
    #                     recording_content_type = %s
    #                 WHERE call_id = %s;
    #             """, (psycopg2.Binary(recording_data), len(recording_data), content_type, call_id))
    #         conn.commit()
    #         logging.info(f"✅ Stored {len(recording_data)} bytes for {call_id}")
    #     except Exception as e:
    #         conn.rollback()
    #         logging.error(f"Error storing recording: {e}")
    #         raise
    #     finally:
    #         self.release_connection(conn)

    # def get_recording_blob(self, call_id: str, agent_id: int = None):
    #     """Retrieve recording bytes from database"""
    #     conn = self.get_connection()
    #     try:
    #         with conn.cursor() as cursor:
    #             if agent_id is not None:
    #                 cursor.execute("""
    #                     SELECT recording_blob_data, recording_content_type, recording_size
    #                     FROM call_history
    #                     WHERE call_id = %s AND agent_id = %s;
    #                 """, (call_id, agent_id))
    #             else:
    #                 cursor.execute("""
    #                     SELECT recording_blob_data, recording_content_type, recording_size
    #                     FROM call_history
    #                     WHERE call_id = %s;
    #                 """, (call_id,))
                
    #             row = cursor.fetchone()
    #             if row and row[0]:
    #                 return row[0], row[1], row[2]
    #             return None, None, None
    #     except Exception as e:
    #         logging.error(f"❌ Error retrieving recording blob: {e}")
    #         return None, None, None
    #     finally:
    #         self.release_connection(conn)

    def get_call_by_id(self, call_id: str, agent_id: int = None):
        """Get a specific call by ID"""
        query = """
            SELECT ch.*, a.agent_name, a.phone_number
            FROM call_history ch
            JOIN agents a ON ch.agent_id = a.id
            WHERE ch.call_id = %s
        """
        params = [call_id]
        
        if agent_id:
            query += " AND ch.agent_id = %s"
            params.append(agent_id)
        
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, tuple(params))
                result = cursor.fetchone()
                
                if result and isinstance(result.get("transcript"), str):
                    try:
                        result["transcript"] = json.loads(result["transcript"])
                    except:
                        pass
                
                return result
        except Exception as e:
            logging.error(f"Error getting call by ID: {e}")
            raise
        finally:
            self.release_connection(conn)


    def get_agent_by_id(self, agent_id: int):
        """Get agent by ID"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM agents 
                    WHERE id = %s
                    LIMIT 1
                """, (agent_id,))
                return cursor.fetchone()
        finally:
            self.release_connection(conn)


    def get_agents_with_analytics(self, admin_id: int):
        """Get all agents with their call statistics"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        a.id,
                        a.phone_number,
                        a.agent_name,
                        a.voice_type,  # Corrected from voice_name
                        a.language,
                        a.industry,
                        a.avatar_url,  
                        a.is_active,
                        a.created_at,
                        a.updated_at,
                        COUNT(ch.id) as total_calls,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls,
                        COUNT(CASE WHEN ch.status = 'unanswered' THEN 1 END) as unanswered_calls,
                        COALESCE(AVG(CASE WHEN ch.duration > 0 THEN ch.duration END), 0) as avg_duration,
                        COALESCE(SUM(ch.duration), 0) as total_duration,
                        MAX(ch.created_at) as last_call_at
                    FROM agents a
                    LEFT JOIN call_history ch ON a.id = ch.agent_id
                    WHERE a.admin_id = %s AND a.is_active = TRUE
                    GROUP BY a.id
                    ORDER BY a.created_at DESC
                """, (admin_id,))
                
                agents = cursor.fetchall()
                
                # Format the results
                for agent in agents:
                    agent["avg_duration"] = round(float(agent["avg_duration"]), 1)
                    agent["total_duration"] = round(float(agent["total_duration"]), 1)
                    if agent["created_at"]:
                        agent["created_at"] = agent["created_at"].isoformat()
                    if agent["updated_at"]:
                        agent["updated_at"] = agent["updated_at"].isoformat()
                    if agent["last_call_at"]:
                        agent["last_call_at"] = agent["last_call_at"].isoformat()
                
                return agents
        except Exception as e:
            logging.error(f"Error fetching agents with analytics: {e}")
            raise
        finally:
            self.release_connection(conn)


    def get_agent_analytics(self, agent_id: int):
        """Get detailed analytics for a specific agent"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_calls,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_calls,
                        COUNT(CASE WHEN status = 'unanswered' THEN 1 END) as unanswered_calls,
                        COUNT(CASE WHEN status = 'initialized' THEN 1 END) as initialized_calls,
                        COUNT(CASE WHEN status = 'connected' THEN 1 END) as connected_calls,
                        COALESCE(AVG(CASE WHEN duration > 0 THEN duration END), 0) as avg_duration,
                        COALESCE(MIN(CASE WHEN duration > 0 THEN duration END), 0) as min_duration,
                        COALESCE(MAX(duration), 0) as max_duration,
                        COALESCE(SUM(duration), 0) as total_duration,
                        MIN(created_at) as first_call_at,
                        MAX(created_at) as last_call_at
                    FROM call_history
                    WHERE agent_id = %s
                """, (agent_id,))
                
                result = cursor.fetchone()
                
                if result:
                    result["avg_duration"] = round(float(result["avg_duration"]), 1)
                    result["min_duration"] = round(float(result["min_duration"]), 1)
                    result["max_duration"] = round(float(result["max_duration"]), 1)
                    result["total_duration"] = round(float(result["total_duration"]), 1)
                    if result["first_call_at"]:
                        result["first_call_at"] = result["first_call_at"].isoformat()
                    if result["last_call_at"]:
                        result["last_call_at"] = result["last_call_at"].isoformat()
                
                return result
        except Exception as e:
            logging.error(f"Error fetching agent analytics: {e}")
            raise
        finally:
            self.release_connection(conn)


    def get_admin_dashboard_analytics(self, admin_id: int):
        """Get overall analytics for admin dashboard"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Total agents (unchanged)
                cursor.execute("""
                    SELECT COUNT(*) as total_agents
                    FROM agents
                    WHERE admin_id = %s AND is_active = TRUE
                """, (admin_id,))
                agents_count = cursor.fetchone()["total_agents"]
                
                # Call statistics (unchanged)
                cursor.execute("""
                    SELECT
                        COUNT(ch.*) as total_calls,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls,
                        COUNT(CASE WHEN ch.status = 'unanswered' THEN 1 END) as unanswered_calls,
                        COALESCE(AVG(CASE WHEN ch.duration > 0 THEN ch.duration END), 0) as avg_duration,
                        COALESCE(SUM(ch.duration), 0) as total_duration
                    FROM call_history ch
                    JOIN agents a ON ch.agent_id = a.id
                    WHERE a.admin_id = %s
                """, (admin_id,))
                call_stats = cursor.fetchone()
                
                # Daily calls (unchanged)
                cursor.execute("""
                    SELECT
                        DATE(ch.created_at) as call_date,
                        COUNT(*) as call_count,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_count
                    FROM call_history ch
                    JOIN agents a ON ch.agent_id = a.id
                    WHERE a.admin_id = %s
                        AND ch.created_at >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY DATE(ch.created_at)
                    ORDER BY call_date DESC
                """, (admin_id,))
                daily_calls = cursor.fetchall()
                
                # Top performing agents - ADDED a.avatar_url
                cursor.execute("""
                    SELECT
                        a.id,
                        a.agent_name,
                        a.phone_number,
                        a.avatar_url,  
                        COUNT(ch.id) as total_calls,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls,
                        COALESCE(AVG(CASE WHEN ch.duration > 0 THEN ch.duration END), 0) as avg_duration
                    FROM agents a
                    LEFT JOIN call_history ch ON a.id = ch.agent_id
                    WHERE a.admin_id = %s AND a.is_active = TRUE
                    GROUP BY a.id
                    ORDER BY completed_calls DESC
                    LIMIT 5
                """, (admin_id,))
                top_agents = cursor.fetchall()
                
                return {
                    "total_agents": agents_count,
                    "total_calls": call_stats["total_calls"],
                    "completed_calls": call_stats["completed_calls"],
                    "unanswered_calls": call_stats["unanswered_calls"],
                    "avg_duration": round(float(call_stats["avg_duration"]), 1),
                    "total_duration": round(float(call_stats["total_duration"]), 1),
                    "daily_calls": [
                        {
                            "date": str(d["call_date"]),
                            "total": d["call_count"],
                            "completed": d["completed_count"]
                        }
                        for d in daily_calls
                    ],
                    "top_agents": [
                        {
                            "id": a["id"],
                            "name": a["agent_name"],
                            "phone": a["phone_number"],
                            "avatar_url": a.get("avatar_url"),  # ADDED: Include in dict
                            "total_calls": a["total_calls"],
                            "completed_calls": a["completed_calls"],
                            "avg_duration": round(float(a["avg_duration"]), 1)
                        }
                        for a in top_agents
                    ]
                }
        except Exception as e:
            logging.error(f"Error fetching dashboard analytics: {e}")
            raise
        finally:
            self.release_connection(conn)



    def get_agents_with_call_stats(self, admin_id: int, page: int = 1, page_size: int = 5):
        """
        Get paginated agents with call statistics for dashboard table.
        Returns agents with total calls, completed calls, avg duration, etc.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Count total agents
                cursor.execute("""
                    SELECT COUNT(*) as total
                    FROM agents
                    WHERE admin_id = %s AND is_active = TRUE
                """, (admin_id,))
                total_agents = cursor.fetchone()["total"]
                
                # Calculate offset
                offset = (page - 1) * page_size
                
                # Get agents with stats
                cursor.execute("""
                    SELECT
                        a.id,
                        a.phone_number,
                        a.agent_name,
                        a.system_prompt,
                        a.voice_type,
                        a.language,
                        a.industry,
                        a.avatar_url, 
                        a.is_active,
                        a.created_at,
                        a.updated_at,
                        a.owner_name,
                        COUNT(ch.id) as total_calls,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls,
                        COUNT(CASE WHEN ch.status = 'unanswered' THEN 1 END) as unanswered_calls,
                        COALESCE(AVG(CASE WHEN ch.duration > 0 THEN ch.duration END), 0) as avg_duration,
                        COALESCE(SUM(ch.duration), 0) as total_duration,
                        MAX(ch.created_at) as last_call_at
                    FROM agents a
                    LEFT JOIN call_history ch ON a.id = ch.agent_id
                    WHERE a.admin_id = %s AND a.is_active = TRUE
                    GROUP BY a.id
                    ORDER BY total_calls DESC, a.created_at DESC
                    LIMIT %s OFFSET %s
                """, (admin_id, page_size, offset))
                
                agents = cursor.fetchall()
                
                # Format response
                for agent in agents:
                    agent["avg_duration"] = round(float(agent["avg_duration"]), 1)
                    agent["total_duration"] = round(float(agent["total_duration"]), 1)
                    if agent["created_at"]:
                        agent["created_at"] = agent["created_at"].isoformat()
                    if agent["updated_at"]:
                        agent["updated_at"] = agent["updated_at"].isoformat()
                    if agent["last_call_at"]:
                        agent["last_call_at"] = agent["last_call_at"].isoformat()
                
                return {
                    "agents": agents,
                    "total": total_agents,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total_agents + page_size - 1) // page_size
                }
                
        except Exception as e:
            logging.error(f"Error fetching agents with stats: {e}")
            raise
        finally:
            self.release_connection(conn)


    def get_top_agents(self, admin_id: int, limit: int = 5):
        """
        Get top performing agents by call count.
        Used for dashboard top 5 agents display.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        a.id,
                        a.agent_name,
                        a.phone_number,
                        a.voice_type,
                        a.language,
                        a.industry,
                        a.owner_name,
                        a.avatar_url,  
                        COUNT(ch.id) as total_calls,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls,
                        COUNT(CASE WHEN ch.status = 'unanswered' THEN 1 END) as unanswered_calls,
                        COALESCE(AVG(CASE WHEN ch.duration > 0 THEN ch.duration END), 0) as avg_duration,
                        MAX(ch.created_at) as last_call_at
                    FROM agents a
                    LEFT JOIN call_history ch ON a.id = ch.agent_id
                    WHERE a.admin_id = %s AND a.is_active = TRUE
                    GROUP BY a.id
                    ORDER BY total_calls DESC, completed_calls DESC
                    LIMIT %s
                """, (admin_id, limit))
                
                agents = cursor.fetchall()
                
                for agent in agents:
                    agent["avg_duration"] = round(float(agent["avg_duration"]), 1)
                    if agent["last_call_at"]:
                        agent["last_call_at"] = agent["last_call_at"].isoformat()
                
                return agents
                
        except Exception as e:
            logging.error(f"Error fetching top agents: {e}")
            raise
        finally:
            self.release_connection(conn)


    def get_agent_detail_with_calls(self, agent_id: int, admin_id: int, calls_page: int = 1, calls_page_size: int = 10):
        """
        Get comprehensive agent details with paginated call history.
        Used for agent detail view.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get agent details
                cursor.execute("""
                    SELECT 
                        a.*
                    FROM agents a
                    WHERE a.id = %s AND a.admin_id = %s
                """, (agent_id, admin_id))
                
                agent = cursor.fetchone()
                
                if not agent:
                    return None
                
                # Format agent data
                if agent["created_at"]:
                    agent["created_at"] = agent["created_at"].isoformat()
                if agent["updated_at"]:
                    agent["updated_at"] = agent["updated_at"].isoformat()
                
                # Get call statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_calls,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_calls,
                        COUNT(CASE WHEN status = 'unanswered' THEN 1 END) as unanswered_calls,
                        COALESCE(AVG(CASE WHEN duration > 0 THEN duration END), 0) as avg_duration,
                        COALESCE(SUM(duration), 0) as total_duration,
                        MIN(created_at) as first_call_at,
                        MAX(created_at) as last_call_at
                    FROM call_history
                    WHERE agent_id = %s
                """, (agent_id,))
                
                stats = cursor.fetchone()
                agent["call_stats"] = {
                    "total_calls": stats["total_calls"],
                    "completed_calls": stats["completed_calls"],
                    "unanswered_calls": stats["unanswered_calls"],
                    "avg_duration": round(float(stats["avg_duration"]), 1),
                    "total_duration": round(float(stats["total_duration"]), 1),
                    "first_call_at": stats["first_call_at"].isoformat() if stats["first_call_at"] else None,
                    "last_call_at": stats["last_call_at"].isoformat() if stats["last_call_at"] else None
                }
                
                # Get paginated call history
                offset = (calls_page - 1) * calls_page_size
                cursor.execute("""
                    SELECT 
                        id,
                        call_id,
                        caller_number,
                        status,
                        duration,
                        created_at,
                        started_at,
                        ended_at,
                        transcript_url,
                        recording_url
                    FROM call_history
                    WHERE agent_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """, (agent_id, calls_page_size, offset))
                
                calls = cursor.fetchall()
                
                for call in calls:
                    if call["created_at"]:
                        call["created_at"] = call["created_at"].isoformat()
                    if call["started_at"]:
                        call["started_at"] = call["started_at"].isoformat()
                    if call["ended_at"]:
                        call["ended_at"] = call["ended_at"].isoformat()
                
                # Get total call count for pagination
                cursor.execute("SELECT COUNT(*) as total FROM call_history WHERE agent_id = %s", (agent_id,))
                total_calls = cursor.fetchone()["total"]
                
                agent["calls"] = {
                    "data": calls,
                    "total": total_calls,
                    "page": calls_page,
                    "page_size": calls_page_size,
                    "total_pages": (total_calls + calls_page_size - 1) // calls_page_size
                }
                
                return agent
                
        except Exception as e:
            logging.error(f"Error fetching agent detail: {e}")
            raise
        finally:
            self.release_connection(conn)


    def create_agent_with_voice_type(self, agent_data: dict):
        """
        Create agent with voice_type, owner_name, and avatar_url.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO agents (
                        phone_number, agent_name, system_prompt,
                        voice_type, language, industry, owner_name, avatar_url, admin_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *;
                """, (
                    agent_data["phone_number"],
                    agent_data["agent_name"],
                    agent_data["system_prompt"],
                    agent_data.get("voice_type", "female"),
                    agent_data.get("language", "en"),
                    agent_data.get("industry"),
                    agent_data.get("owner_name"),
                    agent_data.get("avatar_url"),  # NEW
                    agent_data["admin_id"]
                ))
                result = cursor.fetchone()
            conn.commit()
            logging.info(f"✅ Created agent {result['id']} with avatar")
            return result
        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating agent: {e}")
            raise
        finally:
            self.release_connection(conn)


    def update_agent_with_voice_type(self, agent_id: int, admin_id: int, updates: dict):
        """
        Update agent including avatar_url.
        """
        if not updates:
            return None
        
        conn = self.get_connection()
        try:
            # Verify ownership
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM agents WHERE id = %s AND admin_id = %s",
                    (agent_id, admin_id)
                )
                if not cursor.fetchone():
                    raise ValueError("Agent not found or unauthorized")
            
            # Build update query
            set_clauses = []
            param_values = []
            
            allowed_fields = {
                'agent_name', 'system_prompt', 'voice_type', 
                'language', 'industry', 'phone_number', 'owner_name', 'avatar_url'
            }
            
            for key, value in updates.items():
                if key in allowed_fields:
                    set_clauses.append(f"{key} = %s")
                    param_values.append(value)
            
            if not set_clauses:
                return None
            
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
            set_sql = ", ".join(set_clauses)
            param_values.extend([agent_id, admin_id])
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"UPDATE agents SET {set_sql} WHERE id = %s AND admin_id = %s RETURNING *;",
                    tuple(param_values)
                )
                result = cursor.fetchone()
            
            conn.commit()
            logging.info(f"✅ Updated agent {agent_id}")
            return result
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating agent: {e}")
            raise
        finally:
            self.release_connection(conn)



    def get_agents_by_owner_name(self, admin_id: int, owner_name: str):
        """
        Get all agents for a specific admin filtered by owner name.
        Case-insensitive partial match.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        a.id,
                        a.phone_number,
                        a.agent_name,
                        a.system_prompt,
                        a.voice_type,
                        a.language,
                        a.industry,
                        a.owner_name,
                        a.avatar_url,
                        a.is_active,
                        a.created_at,
                        a.updated_at,
                        COUNT(ch.id) as total_calls,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls,
                        COUNT(CASE WHEN ch.status = 'unanswered' THEN 1 END) as unanswered_calls,
                        COALESCE(AVG(CASE WHEN ch.duration > 0 THEN ch.duration END), 0) as avg_duration,
                        COALESCE(SUM(ch.duration), 0) as total_duration,
                        MAX(ch.created_at) as last_call_at
                    FROM agents a
                    LEFT JOIN call_history ch ON a.id = ch.agent_id
                    WHERE a.admin_id = %s 
                        AND a.is_active = TRUE
                        AND LOWER(a.owner_name) LIKE LOWER(%s)
                    GROUP BY a.id
                    ORDER BY a.created_at DESC
                """, (admin_id, f"%{owner_name}%"))
                
                agents = cursor.fetchall()
                
                # Format response
                for agent in agents:
                    agent["avg_duration"] = round(float(agent["avg_duration"]), 1)
                    agent["total_duration"] = round(float(agent["total_duration"]), 1)
                    if agent["created_at"]:
                        agent["created_at"] = agent["created_at"].isoformat()
                    if agent["updated_at"]:
                        agent["updated_at"] = agent["updated_at"].isoformat()
                    if agent["last_call_at"]:
                        agent["last_call_at"] = agent["last_call_at"].isoformat()
                
                return agents
                
        except Exception as e:
            logging.error(f"Error fetching agents by owner name: {e}")
            raise
        finally:

            self.release_connection(conn)


    def update_user_password(self, email: str, new_password: str):
        """Update user password by email"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Check if user exists
                cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
                if not cursor.fetchone():
                    raise ValueError("User not found")
                
                # Hash new password
                hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
                
                # Update password
                cursor.execute("""
                    UPDATE users 
                    SET password_hash = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE email = %s
                    RETURNING id;
                """, (hashed_password.decode('utf-8'), email))
                
                conn.commit()
                logging.info(f"✅ Password updated for {email}")
                return True
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating password: {e}")
            raise
        finally:
            self.release_connection(conn)