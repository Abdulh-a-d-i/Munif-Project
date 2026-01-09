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
from contextlib import contextmanager


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
        
        # Add SSL mode if not present (for cloud databases like Supabase)
        if self.connection_string and "sslmode" not in self.connection_string:
            separator = "&" if "?" in self.connection_string else "?"
            self.connection_string += f"{separator}sslmode=require"
        
        # Create pool ONCE
        PGDB._pool = pool.SimpleConnectionPool(
            10, 100, self.connection_string
        )
        
        # Create tables ONCE
        self.create_users_table()
        self.create_agents_table()
        self.create_call_history_table()
        self.create_voice_samples_table()
        self.create_subscriptions_table()
        self.create_appointments_table()
        self.create_user_agent_status_table()
        self.add_agent_fields_if_not_exists()

    def get_connection(self):
        """Get connection from pool"""
        return PGDB._pool.getconn()
    
    def release_connection(self, conn):
        """Return connection to pool"""
        PGDB._pool.putconn(conn)

    @contextmanager
    def get_connection_context(self):
        """
        Safe connection context manager that ALWAYS releases connection.
        Use this in ALL database operations!
        """
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.release_connection(conn)

    # ==================== NEW: AGENTS TABLE ====================
    def create_agents_table(self):
        """
        Create agents table with voice_type, owner_name, and avatar_url.
        """
        with self.get_connection_context() as conn:
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

    def add_agent_fields_if_not_exists(self):
        """
        Add new fields to agents table if they don't exist.
        Handles schema migrations for existing databases.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    # Add user_id column (nullable, references users)
                    cursor.execute("""
                        DO $$ 
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='agents' AND column_name='user_id'
                            ) THEN
                                ALTER TABLE agents 
                                ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;
                                
                                CREATE INDEX idx_agents_user ON agents(user_id);
                            END IF;
                        END $$;
                    """)
                conn.commit()
                logging.info("✅ Agent fields migration completed")
            except Exception as e:
                logging.error(f"Error adding agent fields: {e}")


    def get_agent_by_phone(self, phone_number: str):
        """
        Get specific agent details by phone number.
        ✅ Now includes owner_email, business_hours, and minutes.
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        id AS agent_id,
                        agent_name, 
                        industry, 
                        system_prompt, 
                        voice_type,
                        language,
                        owner_name,
                        owner_email,
                        business_hours_start,
                        business_hours_end,
                        allowed_minutes,
                        COALESCE(used_minutes, 0) as used_minutes
                    FROM agents 
                    WHERE phone_number = %s AND is_active = TRUE
                    LIMIT 1
                """, (phone_number,))
                return cursor.fetchone()

    def get_agents_by_admin(self, admin_id: int):
        """Get all agents for a specific admin"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM agents 
                    WHERE admin_id = %s
                    ORDER BY created_at DESC
                """, (admin_id,))
                return cursor.fetchall()

    def delete_agent(self, agent_id: int, admin_id: int):
        """Delete agent (soft delete by setting is_active=False and freeing phone number)"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    # Soft delete + free the phone number for reuse
                    cursor.execute("""
                        UPDATE agents 
                        SET 
                            is_active = FALSE, 
                            phone_number = phone_number || '_deleted_' || EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::TEXT,
                            updated_at = CURRENT_TIMESTAMP
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

    # ==================== USERS TABLE ====================
    def create_users_table(self):
        with self.get_connection_context() as conn:
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

    def register_user(self, user_data):
        with self.get_connection_context() as conn:
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

    def login_user(self, user_data):
        """Verify user credentials by username or email and return user info."""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, username, email, password_hash, first_name, last_name, created_at, is_admin
                        FROM users
                        WHERE username = %s OR email = %s
                        LIMIT 1
                    """, (user_data['email'], user_data['email']))

                    result = cursor.fetchone()

                    if not result:
                        logging.error(f"❌ No user found with email: {user_data['email']}")
                        raise ValueError("Invalid username or password.")
                    
                    logging.info(f"✅ User found: {result[1]} ({result[2]})")
                    logging.info(f"🔐 Checking password...")
                    
                    if bcrypt.checkpw(user_data['password'].encode('utf-8'), result[3].encode('utf-8')):
                        logging.info(f"✅ Password correct for user: {result[2]}")
                        return {
                            "id": result[0],
                            "username": result[1],
                            "email": result[2],
                            "created_at": result[6],
                            "is_admin": result[7]
                        }
                    else:
                        logging.error(f"❌ Password incorrect for user: {result[2]}")
                        raise ValueError("Invalid username or password.")
            except Exception as e:
                logging.error(f"Error during login: {str(e)}")
                raise

    def get_user_by_id(self, user_id: int):
        """Get user by ID"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, first_name, last_name, username, email, is_admin, created_at FROM users WHERE id = %s",
                    (user_id,)
                )
                return cursor.fetchone()

    def get_all_users(self, page: int = 1, page_size: int = 20, search: str = None):
        """
        Get paginated list of all users with optional search.
        Supports search by username or email.
        Returns user info WITHOUT password_hash for security.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Build WHERE clause for search
                    where_clause = ""
                    params = []
                    
                    if search:
                        where_clause = "WHERE username ILIKE %s OR email ILIKE %s"
                        search_pattern = f"%{search}%"
                        params = [search_pattern, search_pattern]
                    
                    # Count total records
                    count_query = f"SELECT COUNT(*) FROM users {where_clause}"
                    cursor.execute(count_query, tuple(params))
                    total = cursor.fetchone()["count"]
                    
                    # Calculate pagination
                    offset = (page - 1) * page_size
                    total_pages = (total + page_size - 1) // page_size
                    
                    # Get paginated users
                    query = f"""
                        SELECT id, username, email, first_name, last_name, is_admin, created_at
                        FROM users
                        {where_clause}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """
                    params.extend([page_size, offset])
                    
                    cursor.execute(query, tuple(params))
                    users = cursor.fetchall()
                    
                    return {
                        "users": users,
                        "pagination": {
                            "page": page,
                            "page_size": page_size,
                            "total": total,
                            "total_pages": total_pages
                        }
                    }
                    
            except Exception as e:
                logging.error(f"Error fetching all users: {e}")
                raise

    def get_all_users_simple(self):
        """
        Get simplified list of all users for dropdowns.
        Returns only id, username, and email.
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, username, email, is_admin
                    FROM users
                    ORDER BY username ASC
                """)
                return cursor.fetchall()


    def update_user_admin_status(self, user_id: int, is_admin: bool, admin_id: int):
        """
        Update user's admin status.
        Validates that admin_id has permission and prevents self-demotion.
        
        Args:
            user_id: ID of user to update
            is_admin: New admin status
            admin_id: ID of admin making the change
            
        Returns:
            Updated user dict
            
        Raises:
            ValueError: If validation fails
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Verify admin_id is actually an admin
                    cursor.execute(
                        "SELECT is_admin FROM users WHERE id = %s",
                        (admin_id,)
                    )
                    admin_user = cursor.fetchone()
                    
                    if not admin_user or not admin_user["is_admin"]:
                        raise ValueError("Only admins can update user admin status")
                    
                    # Prevent self-demotion
                    if user_id == admin_id and not is_admin:
                        raise ValueError("Cannot remove your own admin status")
                    
                    # Check if user exists
                    cursor.execute(
                        "SELECT id, username, email FROM users WHERE id = %s",
                        (user_id,)
                    )
                    target_user = cursor.fetchone()
                    
                    if not target_user:
                        raise ValueError(f"User with ID {user_id} not found")
                    
                    # Update admin status
                    cursor.execute(
                        """
                        UPDATE users 
                        SET is_admin = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, username, email, is_admin, created_at
                        """,
                        (is_admin, user_id)
                    )
                    
                    updated_user = cursor.fetchone()
                    conn.commit()
                    
                    action = "promoted to" if is_admin else "demoted from"
                    logging.info(
                        f"✅ User {target_user['username']} (ID: {user_id}) "
                        f"{action} admin by admin ID: {admin_id}"
                    )
                    
                    return updated_user
                    
            except ValueError as ve:
                conn.rollback()
                raise ve
            except Exception as e:
                conn.rollback()
                logging.error(f"Error updating user admin status: {e}")
                raise

    # ==================== USER AGENT STATUS ====================
    def create_user_agent_status_table(self):
        """Create user_agent_status table to track agent activation per user"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS user_agent_status (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                            agent_id INTEGER REFERENCES agents(id) ON DELETE CASCADE,
                            is_active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(user_id, agent_id)
                        );
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_user_agent_status_user 
                        ON user_agent_status(user_id);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_user_agent_status_agent 
                        ON user_agent_status(agent_id);
                    """)
                conn.commit()
                logging.info("✅ user_agent_status table created")
            except Exception as e:
                logging.error(f"Error creating user_agent_status table: {e}")

    def toggle_agent_status_for_user(self, user_id: int, agent_id: int, is_active: bool):
        """
        Toggle agent active/inactive status for a specific user.
        Creates a new record if it doesn't exist, updates if it does.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # First verify the agent exists and belongs to this user (admin)
                    cursor.execute("""
                        SELECT id FROM agents 
                        WHERE id = %s AND admin_id = %s
                    """, (agent_id, user_id))
                    
                    if not cursor.fetchone():
                        raise ValueError("Agent not found or access denied")
                    
                    # Insert or update the status
                    cursor.execute("""
                        INSERT INTO user_agent_status (user_id, agent_id, is_active, updated_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (user_id, agent_id) 
                        DO UPDATE SET 
                            is_active = EXCLUDED.is_active,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id, is_active;
                    """, (user_id, agent_id, is_active))
                    
                    result = cursor.fetchone()
                conn.commit()
                
                logging.info(
                    f"✅ Agent {agent_id} status updated for user {user_id}: "
                    f"{'active' if is_active else 'inactive'}"
                )
                
                return {
                    "id": result["id"],
                    "user_id": user_id,
                    "agent_id": agent_id,
                    "is_active": result["is_active"]
                }
                
            except ValueError as ve:
                conn.rollback()
                raise ve
            except Exception as e:
                conn.rollback()
                logging.error(f"Error toggling agent status: {e}")
                raise

    def get_agent_status_for_user(self, user_id: int, agent_id: int):
        """Get the activation status of an agent for a specific user"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT is_active, updated_at
                    FROM user_agent_status
                    WHERE user_id = %s AND agent_id = %s
                """, (user_id, agent_id))
                
                result = cursor.fetchone()
                
                # If no record exists, agent is active by default
                if not result:
                    return {"is_active": True, "updated_at": None}
                
                return {
                    "is_active": result["is_active"],
                    "updated_at": result["updated_at"].isoformat() if result["updated_at"] else None
                }

    def get_all_agent_statuses_for_user(self, user_id: int):
        """Get activation status of all agents for a specific user"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        a.id as agent_id,
                        a.agent_name,
                        a.phone_number,
                        COALESCE(uas.is_active, TRUE) as is_active,
                        uas.updated_at
                    FROM agents a
                    LEFT JOIN user_agent_status uas 
                        ON a.id = uas.agent_id AND uas.user_id = %s
                    WHERE a.admin_id = %s AND a.is_active = TRUE
                    ORDER BY a.created_at DESC
                """, (user_id, user_id))
                
                results = cursor.fetchall()
                
                for result in results:
                    if result["updated_at"]:
                        result["updated_at"] = result["updated_at"].isoformat()
                
                return results


    # ==================== CALL HISTORY ====================
    def create_call_history_table(self):
        """Create call_history table to store call details"""
        with self.get_connection_context() as conn:
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
        with self.get_connection_context() as conn:
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

    def update_call_history(self, call_id: str, updates: dict):
        """Update specific fields in the call_history record based on the call_id"""
        if not updates:
            logging.warning("update_call_history called with no updates.")
            return None

        with self.get_connection_context() as conn:
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

    def get_call_history_by_agent(self, agent_id: int, page: int = 1, page_size: int = 10):
        """Get paginated call history for a specific agent"""
        with self.get_connection_context() as conn:
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

    def get_call_history_by_admin(self, admin_id: int, page: int = 1, page_size: int = 10):
        """Get paginated call history for all agents under an admin"""
        with self.get_connection_context() as conn:
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

    def get_call_logs_with_filters(
        self, 
        admin_id: int, 
        page: int = 1, 
        page_size: int = 25,
        search: str = None,
        status_filter: str = None,
        date_from: str = None,
        date_to: str = None
    ):
        """
        Get paginated call logs with search and filters for call logs page.
        Supports:
        - Search by caller number or name
        - Filter by status (completed, unanswered, etc.)
        - Filter by date range
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Build WHERE clauses
                    where_clauses = ["a.admin_id = %s"]
                    params = [admin_id]
                    
                    # Search filter
                    if search:
                        where_clauses.append("(ch.caller_number ILIKE %s OR ch.summary ILIKE %s)")
                        search_pattern = f"%{search}%"
                        params.extend([search_pattern, search_pattern])
                    
                    # Status filter
                    if status_filter and status_filter.lower() != 'all':
                        where_clauses.append("ch.status = %s")
                        params.append(status_filter.lower())
                    
                    # Date range filter
                    if date_from:
                        where_clauses.append("ch.created_at >= %s")
                        params.append(date_from)
                    
                    if date_to:
                        where_clauses.append("ch.created_at <= %s")
                        params.append(date_to)
                    
                    where_sql = " AND ".join(where_clauses)
                    
                    # Count total records with filters
                    count_sql = f"""
                        SELECT COUNT(*) FROM call_history ch
                        JOIN agents a ON ch.agent_id = a.id
                        WHERE {where_sql}
                    """
                    cursor.execute(count_sql, tuple(params))
                    total = cursor.fetchone()["count"]
                    
                    # Count by status
                    cursor.execute(f"""
                        SELECT 
                            COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls,
                            COUNT(CASE WHEN ch.status = 'unanswered' THEN 1 END) as unanswered_calls,
                            COUNT(CASE WHEN ch.status NOT IN ('completed', 'unanswered') THEN 1 END) as other_calls
                        FROM call_history ch
                        JOIN agents a ON ch.agent_id = a.id
                        WHERE {where_sql}
                    """, tuple(params))
                    status_counts = cursor.fetchone()
                    
                    # Paginated query
                    offset = (page - 1) * page_size
                    data_params = params + [page_size, offset]
                    
                    cursor.execute(f"""
                        SELECT 
                            ch.*,
                            a.agent_name,
                            a.phone_number as agent_phone
                        FROM call_history ch
                        JOIN agents a ON ch.agent_id = a.id
                        WHERE {where_sql}
                        ORDER BY ch.created_at DESC
                        LIMIT %s OFFSET %s
                    """, tuple(data_params))
                    
                    rows = cursor.fetchall()
                    
                    # Parse transcripts
                    for row in rows:
                        if isinstance(row.get("transcript"), str):
                            try:
                                row["transcript"] = json.loads(row["transcript"])
                            except Exception:
                                pass
                    
                    return {
                        "calls": rows,
                        "total": total,
                        "completed_calls": status_counts["completed_calls"],
                        "unanswered_calls": status_counts["unanswered_calls"],
                        "other_calls": status_counts["other_calls"],
                        "page": page,
                        "page_size": page_size,
                        "total_pages": (total + page_size - 1) // page_size
                    }
            except Exception as e:
                logging.error(f"Error fetching call logs for admin_id={admin_id}: {e}")
                raise


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
        
        with self.get_connection_context() as conn:
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

    def get_agent_by_id(self, agent_id: int):
        """Get agent by ID"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM agents 
                    WHERE id = %s
                    LIMIT 1
                """, (agent_id,))
                return cursor.fetchone()

    def get_agents_with_analytics(self, admin_id: int):
        """Get all agents with their call statistics"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT
                            a.id,
                            a.phone_number,
                            a.agent_name,
                            a.voice_type,
                            a.language,
                            a.industry,
                            a.avatar_url,  
                            a.is_active,
                            a.created_at,
                            a.updated_at,
                            a.owner_name,
                            a.owner_email,
                            a.business_hours_start,
                            a.business_hours_end,
                            a.allowed_minutes,
                            COALESCE(a.used_minutes, 0) as used_minutes,
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
                        agent["used_minutes"] = round(float(agent["used_minutes"]), 2)
                        
                        if agent["created_at"]:
                            agent["created_at"] = agent["created_at"].isoformat()
                        if agent["updated_at"]:
                            agent["updated_at"] = agent["updated_at"].isoformat()
                        if agent["last_call_at"]:
                            agent["last_call_at"] = agent["last_call_at"].isoformat()
                        
                        # Format time fields
                        if agent.get("business_hours_start"):
                            agent["business_hours_start"] = str(agent["business_hours_start"])
                        if agent.get("business_hours_end"):
                            agent["business_hours_end"] = str(agent["business_hours_end"])
                    
                    return agents
            except Exception as e:
                logging.error(f"Error fetching agents with analytics: {e}")
                raise

    def get_agent_analytics(self, agent_id: int):
        """Get detailed analytics for a specific agent"""
        with self.get_connection_context() as conn:
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

    def get_admin_dashboard_analytics(self, admin_id: int):
        """Get overall analytics for admin dashboard"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Total agents
                    cursor.execute("""
                        SELECT COUNT(*) as total_agents
                        FROM agents
                        WHERE admin_id = %s AND is_active = TRUE
                    """, (admin_id,))
                    agents_count = cursor.fetchone()["total_agents"]
                    
                    # Call statistics
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
                    
                    # Daily calls
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
                    
                    # Top performing agents
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
                                "avatar_url": a.get("avatar_url"),
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



    def get_agents_with_call_stats(self, admin_id: int, page: int = 1, page_size: int = 5):
        """
        Get paginated agents with call statistics for dashboard table.
        Returns agents with total calls, completed calls, avg duration, etc.
        """
        with self.get_connection_context() as conn:
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
                            a.owner_email,
                            a.business_hours_start,
                            a.business_hours_end,
                            a.allowed_minutes,
                            COALESCE(a.used_minutes, 0) as used_minutes,
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
                        agent["used_minutes"] = round(float(agent["used_minutes"]), 2)
                        
                        if agent["created_at"]:
                            agent["created_at"] = agent["created_at"].isoformat()
                        if agent["updated_at"]:
                            agent["updated_at"] = agent["updated_at"].isoformat()
                        if agent["last_call_at"]:
                            agent["last_call_at"] = agent["last_call_at"].isoformat()
                        
                        # Format time fields
                        if agent.get("business_hours_start"):
                            agent["business_hours_start"] = str(agent["business_hours_start"])
                        if agent.get("business_hours_end"):
                            agent["business_hours_end"] = str(agent["business_hours_end"])
                    
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
        


    def get_top_agents(self, admin_id: int, limit: int = 5):
        """
        Get top performing agents by call count.
        Used for dashboard top 5 agents display.
        """
        with self.get_connection_context() as conn:
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
                            a.owner_email,
                            a.business_hours_start,
                            a.business_hours_end,
                            a.allowed_minutes,
                            COALESCE(a.used_minutes, 0) as used_minutes,
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
                        agent["used_minutes"] = round(float(agent["used_minutes"]), 2)
                        
                        if agent["last_call_at"]:
                            agent["last_call_at"] = agent["last_call_at"].isoformat()
                        
                        # Format time fields
                        if agent.get("business_hours_start"):
                            agent["business_hours_start"] = str(agent["business_hours_start"])
                        if agent.get("business_hours_end"):
                            agent["business_hours_end"] = str(agent["business_hours_end"])
                    
                    return agents
                    
            except Exception as e:
                logging.error(f"Error fetching top agents: {e}")
                raise


    def get_agent_detail_with_calls(self, agent_id: int, admin_id: int, calls_page: int = 1, calls_page_size: int = 10):
        """
        Get comprehensive agent details with paginated call history.
        Used for agent detail view.
        """
        with self.get_connection_context() as conn:
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
                    
                    # Get paginated call history - INCLUDE recording_blob and transcript_blob
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
                            transcript,
                            transcript_url,
                            transcript_blob,
                            recording_url,
                            recording_blob
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
           

    def create_agent_with_voice_type(self, agent_data: dict):
        """
        Create agent with new fields: owner_email, business hours, minutes.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO agents (
                            phone_number, agent_name, system_prompt,
                            voice_type, language, industry, 
                            owner_name, owner_email, avatar_url,
                            business_hours_start, business_hours_end,
                            allowed_minutes, used_minutes,
                            admin_id, user_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *;
                    """, (
                        agent_data["phone_number"],
                        agent_data["agent_name"],
                        agent_data["system_prompt"],
                        agent_data.get("voice_type", "female"),
                        agent_data.get("language", "en"),
                        agent_data.get("industry"),
                        agent_data.get("owner_name"),
                        agent_data.get("owner_email"),  # NEW
                        agent_data.get("avatar_url"),
                        agent_data.get("business_hours_start"),  # NEW
                        agent_data.get("business_hours_end"),    # NEW
                        agent_data.get("allowed_minutes", 0),    # NEW
                        0,  # used_minutes starts at 0           # NEW
                        agent_data["admin_id"],
                        agent_data.get("user_id")  # NEW: user assignment
                    ))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Created agent {result['id']} with minutes limit")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error creating agent: {e}")
                raise

        


    def update_agent_with_voice_type(self, agent_id: int, admin_id: int, updates: dict):
        """
        Update agent including new fields.
        """
        if not updates:
            return None
        
        with self.get_connection_context() as conn:
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
                    'language', 'industry', 'phone_number', 
                    'owner_name', 'owner_email', 'avatar_url',
                    'business_hours_start', 'business_hours_end', 
                    'allowed_minutes', 'user_id'  # Can update user assignment
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
       



    def get_agents_by_owner_name(self, admin_id: int, owner_name: str):
        """
        Get all agents for a specific admin filtered by owner name.
        Case-insensitive partial match.
        """
        with self.get_connection_context() as conn:
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
       


    def update_user_password(self, email: str, new_password: str):
        """Update user password by email"""
        with self.get_connection_context() as conn:
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
        




    def create_voice_samples_table(self):
        """Create voice_samples table"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS voice_samples (
                            id SERIAL PRIMARY KEY,
                            voice_name VARCHAR(100) NOT NULL,
                            voice_id VARCHAR(50) NOT NULL UNIQUE,
                            language VARCHAR(10) NOT NULL,
                            country_code VARCHAR(5) NOT NULL,
                            gender VARCHAR(10),
                            audio_blob_path TEXT NOT NULL,
                            duration_seconds FLOAT,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_voice_samples_language 
                        ON voice_samples(language);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_voice_samples_gender 
                        ON voice_samples(gender);
                    """)
                conn.commit()
                logging.info("✅ voice_samples table created")
            except Exception as e:
                logging.error(f"Error creating voice_samples table: {e}")
        

    def insert_voice_sample(self, voice_data: dict):
        """Insert a voice sample record (without updated_at)"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO voice_samples (
                            voice_name, voice_id, language, country_code, 
                            gender, audio_blob_path, duration_seconds
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (voice_id) DO UPDATE SET
                            audio_blob_path = EXCLUDED.audio_blob_path
                        RETURNING *;
                    """, (
                        voice_data["voice_name"],
                        voice_data["voice_id"],
                        voice_data["language"],
                        voice_data["country_code"],
                        voice_data.get("gender"),
                        voice_data["audio_blob_path"],
                        voice_data.get("duration_seconds")
                    ))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Voice sample saved: {voice_data['voice_name']}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error inserting voice sample: {e}")
                raise
        

    def get_all_voice_samples(self):
        """Get all voice samples"""
        with self.get_connection_context() as conn:  # ← CHANGED THIS LINE
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        id, voice_name, voice_id, language, 
                        country_code, gender, audio_blob_path,
                        duration_seconds, created_at
                    FROM voice_samples
                    ORDER BY language, voice_name
                """)
                return cursor.fetchall()

    def get_voice_samples_by_language(self, language: str):
        """Get voice samples filtered by language"""
        with self.get_connection_context() as conn:  # ← CHANGED THIS LINE
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        id, voice_name, voice_id, language, 
                        country_code, gender, audio_blob_path,
                        duration_seconds, created_at
                    FROM voice_samples
                    WHERE language = %s
                    ORDER BY voice_name
                """, (language,))
                return cursor.fetchall()
        
 # ==================== SUBSCRIPTIONS TABLE ====================
    def create_subscriptions_table(self):
        """Create subscriptions table to manage user subscription cycles"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS subscriptions (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                            total_minutes_allocated INTEGER NOT NULL DEFAULT 0,
                            minutes_used DECIMAL(10, 2) DEFAULT 0,
                            cycle_start_date TIMESTAMPTZ NOT NULL,
                            cycle_end_date TIMESTAMPTZ NOT NULL,
                            status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Expired')),
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id 
                        ON subscriptions(user_id);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_subscriptions_status 
                        ON subscriptions(status);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_subscriptions_cycle_dates 
                        ON subscriptions(cycle_start_date, cycle_end_date);
                    """)
                conn.commit()
                logging.info("✅ subscriptions table created")
            except Exception as e:
                logging.error(f"Error creating subscriptions table: {e}")

    def create_subscription(self, subscription_data: dict):
        """Create a new subscription for a user"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO subscriptions (
                            user_id, total_minutes_allocated, minutes_used,
                            cycle_start_date, cycle_end_date, status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING *;
                    """, (
                        subscription_data['user_id'],
                        subscription_data.get('total_minutes_allocated', 0),
                        subscription_data.get('minutes_used', 0),
                        subscription_data['cycle_start_date'],
                        subscription_data['cycle_end_date'],
                        subscription_data.get('status', 'Active')
                    ))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Created subscription {result['id']} for user {subscription_data['user_id']}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error creating subscription: {e}")
                raise

    def get_subscription_by_user(self, user_id: int):
        """Get active subscription for a user"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM subscriptions
                    WHERE user_id = %s AND status = 'Active'
                    ORDER BY cycle_start_date DESC
                    LIMIT 1
                """, (user_id,))
                return cursor.fetchone()

    def update_subscription_minutes(self, subscription_id: int, minutes_to_add: float):
        """Update minutes used in a subscription"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        UPDATE subscriptions
                        SET minutes_used = minutes_used + %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING *;
                    """, (round(minutes_to_add, 2), subscription_id))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Updated subscription {subscription_id} minutes")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error updating subscription minutes: {e}")
                raise

    def expire_subscription(self, subscription_id: int):
        """Mark a subscription as expired"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE subscriptions
                        SET status = 'Expired',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id;
                    """, (subscription_id,))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Expired subscription {subscription_id}")
                return bool(result)
            except Exception as e:
                conn.rollback()
                logging.error(f"Error expiring subscription: {e}")
                raise

    def get_user_plan_usage(self, user_id: int):
        """
        Get user's plan usage statistics for dashboard display.
        Returns total minutes, used minutes, remaining minutes, percentage used,
        and days until reset.
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        id,
                        total_minutes_allocated,
                        minutes_used,
                        cycle_start_date,
                        cycle_end_date,
                        status
                    FROM subscriptions
                    WHERE user_id = %s AND status = 'Active'
                    ORDER BY cycle_start_date DESC
                    LIMIT 1
                """, (user_id,))
                
                subscription = cursor.fetchone()
                
                # If no active subscription, return default values
                if not subscription:
                    return {
                        "has_subscription": False,
                        "total_minutes": 0,
                        "used_minutes": 0,
                        "remaining_minutes": 0,
                        "percentage_used": 0,
                        "days_until_reset": None,
                        "cycle_end_date": None,
                        "status": "No Active Plan"
                    }
                
                # Calculate statistics
                total_minutes = float(subscription["total_minutes_allocated"])
                used_minutes = float(subscription["minutes_used"])
                remaining_minutes = max(0, total_minutes - used_minutes)
                percentage_used = round((used_minutes / total_minutes * 100) if total_minutes > 0 else 0, 1)
                
                # Calculate days until reset
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                cycle_end = subscription["cycle_end_date"]
                
                # Ensure cycle_end is timezone-aware
                if cycle_end.tzinfo is None:
                    cycle_end = cycle_end.replace(tzinfo=timezone.utc)
                
                days_until_reset = (cycle_end - now).days
                
                return {
                    "has_subscription": True,
                    "total_minutes": int(total_minutes),
                    "used_minutes": round(used_minutes, 2),
                    "remaining_minutes": round(remaining_minutes, 2),
                    "percentage_used": percentage_used,
                    "days_until_reset": max(0, days_until_reset),
                    "cycle_end_date": cycle_end.isoformat(),
                    "status": "Active"
                }


    # ==================== APPOINTMENTS TABLE ====================
    def create_appointments_table(self):
        """Create appointments table to manage scheduled calls"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS appointments (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                            call_id TEXT REFERENCES call_history(call_id) ON DELETE SET NULL,
                            customer_name VARCHAR(255) NOT NULL,
                            scheduled_time TIMESTAMPTZ NOT NULL,
                            notes TEXT,
                            status VARCHAR(20) DEFAULT 'Scheduled' CHECK (status IN ('Scheduled', 'Cancelled', 'Completed')),
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_appointments_user_id 
                        ON appointments(user_id);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_appointments_call_id 
                        ON appointments(call_id);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_appointments_status 
                        ON appointments(status);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_appointments_scheduled_time 
                        ON appointments(scheduled_time);
                    """)
                conn.commit()
                logging.info("✅ appointments table created")
            except Exception as e:
                logging.error(f"Error creating appointments table: {e}")

    def create_appointment(self, appointment_data: dict):
        """Create a new appointment"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO appointments (
                            user_id, call_id, customer_name, scheduled_time, notes, status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING *;
                    """, (
                        appointment_data['user_id'],
                        appointment_data.get('call_id'),
                        appointment_data['customer_name'],
                        appointment_data['scheduled_time'],
                        appointment_data.get('notes'),
                        appointment_data.get('status', 'Scheduled')
                    ))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Created appointment {result['id']} for user {appointment_data['user_id']}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error creating appointment: {e}")
                raise

    def get_appointments_by_user(self, user_id: int, status: str = None):
        """Get appointments for a user, optionally filtered by status"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if status:
                    cursor.execute("""
                        SELECT * FROM appointments
                        WHERE user_id = %s AND status = %s
                        ORDER BY scheduled_time DESC
                    """, (user_id, status))
                else:
                    cursor.execute("""
                        SELECT * FROM appointments
                        WHERE user_id = %s
                        ORDER BY scheduled_time DESC
                    """, (user_id,))
                return cursor.fetchall()

    def get_appointment_by_call_id(self, call_id: str):
        """Get appointment associated with a specific call"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM appointments
                    WHERE call_id = %s
                    LIMIT 1
                """, (call_id,))
                result = cursor.fetchone()
                
                if result and result.get("scheduled_time"):
                    result["scheduled_time"] = result["scheduled_time"].isoformat()
                if result and result.get("created_at"):
                    result["created_at"] = result["created_at"].isoformat()
                if result and result.get("updated_at"):
                    result["updated_at"] = result["updated_at"].isoformat()
                
                return result


    def update_appointment_status(self, appointment_id: int, status: str):
        """Update appointment status"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        UPDATE appointments
                        SET status = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING *;
                    """, (status, appointment_id))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Updated appointment {appointment_id} status to {status}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error updating appointment status: {e}")
                raise

    def link_appointment_to_call(self, appointment_id: int, call_id: str):
        """Link an appointment to a call_id"""
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        UPDATE appointments
                        SET call_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING *;
                    """, (call_id, appointment_id))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Linked appointment {appointment_id} to call {call_id}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error linking appointment to call: {e}")
                raise

    def add_agent_fields_if_not_exists(self):
        """
        Add new fields to agents table if they don't exist:
        - owner_email: Business owner's email
        - business_hours_start: Opening time (HH:MM format)
        - business_hours_end: Closing time (HH:MM format)
        - allowed_minutes: Total minutes allocated per billing cycle
        - used_minutes: Minutes consumed in current billing cycle
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    # Add owner_email
                    cursor.execute("""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='agents' AND column_name='owner_email'
                            ) THEN
                                ALTER TABLE agents ADD COLUMN owner_email VARCHAR(255);
                            END IF;
                        END $$;
                    """)
                    
                    # Add business_hours_start (TIME type)
                    cursor.execute("""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='agents' AND column_name='business_hours_start'
                            ) THEN
                                ALTER TABLE agents ADD COLUMN business_hours_start TIME;
                            END IF;
                        END $$;
                    """)
                    
                    # Add business_hours_end
                    cursor.execute("""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='agents' AND column_name='business_hours_end'
                            ) THEN
                                ALTER TABLE agents ADD COLUMN business_hours_end TIME;
                            END IF;
                        END $$;
                    """)
                    
                    # Add allowed_minutes (integer, default 0)
                    cursor.execute("""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='agents' AND column_name='allowed_minutes'
                            ) THEN
                                ALTER TABLE agents ADD COLUMN allowed_minutes INTEGER DEFAULT 0;
                            END IF;
                        END $$;
                    """)
                    
                    # Add used_minutes (DECIMAL for precision)
                    cursor.execute("""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='agents' AND column_name='used_minutes'
                            ) THEN
                                ALTER TABLE agents ADD COLUMN used_minutes DECIMAL(10, 2) DEFAULT 0;
                            END IF;
                        END $$;
                    """)
                    
                    # Create index for faster queries
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_agents_minutes_check 
                        ON agents(used_minutes, allowed_minutes) 
                        WHERE is_active = TRUE;
                    """)
                    
                conn.commit()
                logging.info("✅ Agent fields added/verified successfully")
            except Exception as e:
                conn.rollback()
                logging.error(f"❌ Error adding agent fields: {e}")
                raise

    def check_agent_minutes_available(self, agent_id: int) -> dict:
        """
        Check if agent has available minutes.
        Returns: {
            "available": bool,
            "allowed_minutes": int,
            "used_minutes": float,
            "remaining_minutes": float
        }
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        allowed_minutes,
                        COALESCE(used_minutes, 0) as used_minutes,
                        (allowed_minutes - COALESCE(used_minutes, 0)) as remaining_minutes
                    FROM agents 
                    WHERE id = %s AND is_active = TRUE
                """, (agent_id,))
                
                result = cursor.fetchone()
                
                if not result:
                    return {
                        "available": False,
                        "allowed_minutes": 0,
                        "used_minutes": 0,
                        "remaining_minutes": 0
                    }
                
                return {
                    "available": result["remaining_minutes"] > 0,
                    "allowed_minutes": result["allowed_minutes"],
                    "used_minutes": float(result["used_minutes"]),
                    "remaining_minutes": float(result["remaining_minutes"])
                }

    def update_agent_used_minutes(self, agent_id: int, call_duration_minutes: float):
        """
        Increment used_minutes after a call ends.
        
        Args:
            agent_id: Agent ID
            call_duration_minutes: Duration in minutes (will be rounded to 2 decimals)
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE agents 
                        SET used_minutes = COALESCE(used_minutes, 0) + %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, used_minutes, allowed_minutes;
                    """, (round(call_duration_minutes, 2), agent_id))
                    
                    result = cursor.fetchone()
                conn.commit()
                
                if result:
                    logging.info(
                        f"✅ Agent {agent_id}: Used minutes updated to {result[1]}/{result[2]}"
                    )
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"❌ Error updating used minutes: {e}")
                raise

    def reset_agent_minutes(self, agent_id: int, admin_id: int):
        """
        Reset used_minutes to 0 for billing cycle reset.
        Only owner can reset their agent's minutes.
        
        Args:
            agent_id: Agent ID
            admin_id: Admin user ID (for authorization)
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    # Verify ownership
                    cursor.execute(
                        "SELECT id FROM agents WHERE id = %s AND admin_id = %s",
                        (agent_id, admin_id)
                    )
                    if not cursor.fetchone():
                        raise ValueError("Agent not found or unauthorized")
                    
                    # Reset minutes
                    cursor.execute("""
                        UPDATE agents 
                        SET used_minutes = 0,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id, allowed_minutes;
                    """, (agent_id,))
                    
                    result = cursor.fetchone()
                conn.commit()
                
                logging.info(f"✅ Agent {agent_id} minutes reset (limit: {result[1]} min)")
                return True
            except Exception as e:
                conn.rollback()
                logging.error(f"❌ Error resetting minutes: {e}")
                raise

    def get_agent_with_minutes_check(self, agent_id: int):
        """
        Get agent details with minutes availability check.
        Used before accepting calls.
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        id AS agent_id,
                        agent_name,
                        phone_number,
                        owner_name,
                        owner_email,
                        industry,
                        system_prompt,
                        voice_type,
                        language,
                        allowed_minutes,
                        COALESCE(used_minutes, 0) as used_minutes,
                        (allowed_minutes - COALESCE(used_minutes, 0)) as remaining_minutes,
                        business_hours_start,
                        business_hours_end,
                        CASE 
                            WHEN (allowed_minutes - COALESCE(used_minutes, 0)) > 0 
                            THEN TRUE 
                            ELSE FALSE 
                        END as can_accept_calls
                    FROM agents 
                    WHERE id = %s AND is_active = TRUE
                """, (agent_id,))
                
                return cursor.fetchone()