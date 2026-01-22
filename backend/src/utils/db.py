import os
from datetime import datetime
import time
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
        
        # Parse connection string to add keepalive parameters
        if self.connection_string:
            # Add SSL mode if not present (for cloud databases like Supabase)
            if "sslmode" not in self.connection_string:
                separator = "&" if "?" in self.connection_string else "?"
                self.connection_string += f"{separator}sslmode=require"
            
            # Add keepalive parameters to prevent connection timeout
            if "keepalives" not in self.connection_string:
                self.connection_string += "&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5"
        
        # Create pool ONCE with connection validation
        PGDB._pool = pool.SimpleConnectionPool(
            minconn=10,
            maxconn=100,
            dsn=self.connection_string
        )
        
        # Create tables ONCE
        self.create_users_table()
        self.create_agents_table()
        self.create_call_history_table()
        self.create_appointments_table()
        self.create_user_agent_status_table()
        self.create_google_credentials_table()
        self.ensure_agent_schema_migration()
        self.ensure_user_schema_migration()

    def get_connection(self):
        """Get connection from pool"""
        return PGDB._pool.getconn()
    
    def release_connection(self, conn, close=False):
        """
        Return connection to pool.
        Args:
            conn: The connection object
            close: If True, close the connection (remove from pool). Use for broken connections.
        """
        if PGDB._pool:
            PGDB._pool.putconn(conn, close=close)

    @contextmanager
    def get_connection_context(self):
        """
        Safe connection context manager that ALWAYS releases connection.
        Use this in ALL database operations!
        Includes retry logic to handle stale connections in the pool.
        """
        conn = None
        max_retries = 5
        
        try:
            # Retry loop to get a valid connection
            for attempt in range(max_retries):
                try:
                    conn = self.get_connection()
                    
                    # Validate connection is alive
                    if conn.closed:
                        raise Exception("Connection is closed")
                        
                    with conn.cursor() as test_cursor:
                        test_cursor.execute("SELECT 1")
                        
                    # If we get here, connection is good
                    break
                    
                except Exception as e:
                    logging.warning(f" Connection validation failed (Attempt {attempt+1}/{max_retries}): {e}")
                    
                    if conn:
                        # Remove bad connection from pool
                        self.release_connection(conn, close=True)
                        conn = None
                    
                    # Exponential backoff: 0.2s, 0.4s, 0.8s, 1.6s, 3.2s
                    sleep_time = 0.2 * (2 ** attempt)
                    logging.info(f" Sleeping {sleep_time:.2f}s before retry...")
                    time.sleep(sleep_time)

            # If all retries failed, force pool reset
            if conn is None:
                logging.critical(" CONNECTION POOL EXHAUSTED/BROKEN. RESETTING POOL.")
                try:
                    if PGDB._pool:
                        PGDB._pool.closeall()
                except Exception as pool_e:
                    logging.error(f"Error checking closing pool: {pool_e}")
                
                # Re-create pool
                PGDB._pool = pool.SimpleConnectionPool(
                    minconn=10,
                    maxconn=100,
                    dsn=self.connection_string
                )
                logging.info(" Connection pool re-initialized. Trying one last time...")
                
                # Last ditch attempt
                conn = self.get_connection()
                with conn.cursor() as test_cursor:
                    test_cursor.execute("SELECT 1")
            
            yield conn
            
        except Exception as e:
            logging.error(f"Error in connection context: {e}")
            raise
        finally:
            if conn:
                # Return good connection to pool
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
                            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
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
                logging.info(" agents table created with avatar_url")
            except Exception as e:
                logging.error(f"Error creating agents table: {e}")

    def ensure_agent_schema_migration(self):
        """
        Add new fields to agents table if they don't exist.
        Handles schema migrations for existing databases.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    # Check for user_id column explicitly
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='agents' AND column_name='user_id'")
                    if not cursor.fetchone():
                        logging.info(" Adding user_id column to agents table...")
                        cursor.execute("ALTER TABLE agents ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;")
                        cursor.execute("CREATE INDEX idx_agents_user ON agents(user_id);")
                        conn.commit()
                        logging.info(" user_id column ADDED successfully")
                    else:
                        logging.info(" user_id column already exists (skipping)")

                    # Check for owner_email
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='agents' AND column_name='owner_email'")
                    if not cursor.fetchone():
                        logging.info(" Adding owner_email column to agents table...")
                        cursor.execute("ALTER TABLE agents ADD COLUMN owner_email VARCHAR(100);")
                        conn.commit()
                        logging.info(" owner_email column ADDED successfully")
                    
                    # Check for business_hours_start
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='agents' AND column_name='business_hours_start'")
                    if not cursor.fetchone():
                        logging.info(" Adding business_hours_start column to agents table...")
                        cursor.execute("ALTER TABLE agents ADD COLUMN business_hours_start VARCHAR(5);")
                        conn.commit()
                    
                    # Check for business_hours_end
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='agents' AND column_name='business_hours_end'")
                    if not cursor.fetchone():
                        logging.info(" Adding business_hours_end column to agents table...")
                        cursor.execute("ALTER TABLE agents ADD COLUMN business_hours_end VARCHAR(5);")
                        conn.commit()
                    
                    # Check for allowed_minutes
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='agents' AND column_name='allowed_minutes'")
                    if not cursor.fetchone():
                        logging.info(" Adding allowed_minutes column to agents table...")
                        cursor.execute("ALTER TABLE agents ADD COLUMN allowed_minutes INTEGER DEFAULT 0;")
                        conn.commit()
                    
                    # Check for used_minutes
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='agents' AND column_name='used_minutes'")
                    if not cursor.fetchone():
                        logging.info(" Adding used_minutes column to agents table...")
                        cursor.execute("ALTER TABLE agents ADD COLUMN used_minutes DOUBLE PRECISION DEFAULT 0.0;")
                        conn.commit()
                        logging.info(" used_minutes column ADDED successfully")
                        
            except Exception as e:
                logging.error(f"Error adding agent fields: {e}")
                # Try to rollback if possible
                try:
                    conn.rollback()
                except:
                    pass

    def ensure_user_schema_migration(self):
        """
        Add new fields to users table if they don't exist.
        Handles schema migrations for existing databases.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    # Check for business_details_submitted column
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='business_details_submitted'")
                    if not cursor.fetchone():
                        logging.info(" Adding business_details_submitted column to users table...")
                        cursor.execute("ALTER TABLE users ADD COLUMN business_details_submitted BOOLEAN DEFAULT FALSE;")
                        conn.commit()
                        logging.info(" business_details_submitted column ADDED successfully")
                    else:
                        logging.info("ℹ business_details_submitted column already exists (skipping)")
                    
                    # Check for is_check column
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='is_check'")
                    if not cursor.fetchone():
                        logging.info(" Adding is_check column to users table...")
                        cursor.execute("ALTER TABLE users ADD COLUMN is_check BOOLEAN DEFAULT FALSE;")
                        conn.commit()
                        logging.info(" is_check column ADDED successfully")
                    else:
                        logging.info(" is_check column already exists (skipping)")

                    # Check for agent_id column
                    cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='agent_id'")
                    if not cursor.fetchone():
                        logging.info(" Adding agent_id column to users table...")
                        cursor.execute("ALTER TABLE users ADD COLUMN agent_id INTEGER;")
                        conn.commit()
                        logging.info(" agent_id column ADDED successfully")
                    else:
                        logging.info(" agent_id column already exists (skipping)")
                        
            except Exception as e:
                logging.error(f"Error adding user fields: {e}")
                # Try to rollback if possible
                try:
                    conn.rollback()
                except:
                    pass


    def get_agent_by_phone(self, phone_number: str):
        """
        Get specific agent details by phone number.
        Only returns active agents (those that can receive calls).
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        a.id AS agent_id,
                        a.agent_name, 
                        a.industry, 
                        a.system_prompt, 
                        a.voice_type,
                        a.language,
                        a.owner_name,
                        a.owner_email,
                        a.business_hours_start,
                        a.business_hours_end,
                        a.allowed_minutes,
                        COALESCE(a.used_minutes, 0) as used_minutes
                    FROM agents a
                    WHERE a.phone_number = %s 
                    AND a.is_active = TRUE
                    LIMIT 1
                """, (phone_number,))
                return cursor.fetchone()

    def get_agents_by_admin(self, admin_id: int):
        """Get all agents for a specific admin (agents created by this admin)"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT a.*
                    FROM agents a
                    WHERE a.admin_id = %s
                    ORDER BY a.created_at DESC
                """, (admin_id,))
                return cursor.fetchall()

    def get_agents_for_user(self, user_id: int):
        """Get all agents assigned to a specific user (via user_id field)"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT a.*
                    FROM agents a
                    WHERE (a.user_id = %s OR a.admin_id = %s) AND a.is_active = TRUE
                    ORDER BY a.created_at DESC
                """, (user_id, user_id))
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
                        SELECT id, username, email, password_hash, first_name, last_name, created_at, is_admin, business_details_submitted, is_check
                        FROM users
                        WHERE username = %s OR email = %s
                        LIMIT 1
                    """, (user_data['email'], user_data['email']))

                    result = cursor.fetchone()

                    if not result:
                        logging.error(f" No user found with email: {user_data['email']}")
                        raise ValueError("Invalid username or password.")
                    
                    logging.info(f" User found: {result[1]} ({result[2]})")
                    logging.info(f" Checking password...")
                    
                    if bcrypt.checkpw(user_data['password'].encode('utf-8'), result[3].encode('utf-8')):
                        logging.info(f" Password correct for user: {result[2]}")
                        is_admin = result[7]
                        business_details_submitted = result[8] if result[8] is not None else False
                        is_check = result[9] if result[9] is not None else False
                        return {
                            "id": result[0],
                            "username": result[1],
                            "email": result[2],
                            "created_at": result[6],
                            "is_admin": is_admin,
                            "role": "admin" if is_admin else "user",
                            "business_details_submitted": business_details_submitted,
                            "is_check": is_check
                        }
                    else:
                        logging.error(f" Password incorrect for user: {result[2]}")
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
                user = cursor.fetchone()
                if user:
                    # Add role field based on is_admin
                    user['role'] = 'admin' if user.get('is_admin', False) else 'user'
                return user

    def mark_business_details_submitted(self, user_id: int):
        """
        Mark that user has submitted business details.
        Sets business_details_submitted flag and is_check flag to true.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE users 
                        SET business_details_submitted = TRUE,
                            is_check = TRUE,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id;
                    """, (user_id,))
                    result = cursor.fetchone()
                conn.commit()
                
                if result:
                    logging.info(f" User {user_id} marked as having submitted business details (is_check = true)")
                    return True
                else:
                    logging.warning(f" User {user_id} not found when marking business details")
                    return False
                    
            except Exception as e:
                conn.rollback()
                logging.error(f"Error marking business details submitted: {e}")
                raise

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
        Returns id, username, email, is_admin, and agent_is_active (if user has an active agent).
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        u.id, 
                        u.username, 
                        u.email, 
                        u.is_admin,
                        COALESCE(
                            (SELECT a.is_active 
                             FROM agents a 
                             WHERE a.user_id = u.id 
                             ORDER BY a.created_at DESC 
                             LIMIT 1),
                            FALSE
                        ) AS is_active
                    FROM users u
                    ORDER BY u.username ASC
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
                        f" User {target_user['username']} (ID: {user_id}) "
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
                logging.info(" user_agent_status table created")
            except Exception as e:
                logging.error(f"Error creating user_agent_status table: {e}")

    # def toggle_agent_status_for_user(self, user_id: int, agent_id: int, is_active: bool):
    #     """
    #     Toggle agent active/inactive status.
    #     Updates the agents.is_active column directly to control call reception.
        
    #     Ownership Rules:
    #     - Admins (creator) can toggle agents they created (admin_id match)
    #     - Regular users can toggle agents assigned to them (user_id match)
    #     """
    #     with self.get_connection_context() as conn:
    #         try:
    #             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
    #                 # Verify the agent exists and get ownership info
    #                 cursor.execute("""
    #                     SELECT id, admin_id, user_id FROM agents 
    #                     WHERE id = %s
    #                 """, (agent_id,))
                    
    #                 agent = cursor.fetchone()
    #                 if not agent:
    #                     raise ValueError("Agent not found")
                    
    #                 # Check if user is the admin who created the agent
    #                 is_creator = agent["admin_id"] == user_id
                    
    #                 # Check if user is assigned to this agent
    #                 is_assigned_user = agent.get("user_id") == user_id
                    
    #                 # Allow access if user is either creator OR assigned user
    #                 has_access = is_creator or is_assigned_user
                    
    #                 logging.info(
    #                     f" Toggle attempt - User: {user_id}, Agent: {agent_id}, "
    #                     f"Creator: {is_creator}, Assigned: {is_assigned_user}, "
    #                     f"Admin ID: {agent['admin_id']}, User ID: {agent.get('user_id')}"
    #                 )
                    
    #                 if not has_access:
    #                     raise ValueError("Access denied: You don't own this agent")
                    
    #                 # Update the agents.is_active column directly
    #                 cursor.execute("""
    #                     UPDATE agents 
    #                     SET is_active = %s,
    #                         updated_at = CURRENT_TIMESTAMP
    #                     WHERE id = %s
    #                     RETURNING id, is_active;
    #                 """, (is_active, agent_id))
                    
    #                 result = cursor.fetchone()
    #             conn.commit()
                
    #             logging.info(
    #                 f" Agent {agent_id} status updated: "
    #                 f"{'active' if is_active else 'inactive'}"
    #             )
                
    #             return {
    #                 "agent_id": agent_id,
    #                 "is_active": result["is_active"]
    #             }
                
    #         except ValueError as ve:
    #             conn.rollback()
    #             raise ve
    #         except Exception as e:
    #             conn.rollback()
    #             logging.error(f"Error toggling agent status: {e}")
    #             raise



    # def toggle_agent_status_admin(self, admin_id: int, agent_id: int, is_active: bool):
    #     """
    #     Admin-only global agent activation toggle.

    #     Rules:
    #     - Only the admin who created the agent can toggle it
    #     - Updates agents.is_active directly
    #     """
    #     with self.get_connection_context() as conn:
    #         try:
    #             with conn.cursor(cursor_factory=RealDictCursor) as cursor:
    #                 # 🔍 Verify agent exists and belongs to admin
    #                 cursor.execute("""
    #                     SELECT id, admin_id, is_active
    #                     FROM agents
    #                     WHERE id = %s
    #                 """, (agent_id,))

    #                 agent = cursor.fetchone()
    #                 if not agent:
    #                     raise ValueError("Agent not found")

    #                 if agent["admin_id"] != admin_id:
    #                     raise ValueError("Access denied: You do not own this agent")

    #                 # 🔄 Update global status
    #                 cursor.execute("""
    #                     UPDATE agents
    #                     SET is_active = %s,
    #                         updated_at = CURRENT_TIMESTAMP
    #                     WHERE id = %s
    #                     RETURNING id, is_active;
    #                 """, (is_active, agent_id))

    #                 result = cursor.fetchone()
    #                 conn.commit()

    #                 logging.info(
    #                     f"Admin {admin_id} set Agent {agent_id} "
    #                     f"to {'active' if is_active else 'inactive'}"
    #                 )

    #                 return {
    #                     "agent_id": result["id"],
    #                     "is_active": result["is_active"]
    #                 }

    #         except ValueError:
    #             conn.rollback()
    #             raise
    #         except Exception as e:
    #             conn.rollback()
    #             logging.error(f"Error toggling agent status: {e}")
    #             raise



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
            
            logging.info(f" Generated presigned URL (expires in {expiration}s): {blob_path}")
            return url
            
        except Exception as e:
            logging.error(f" Failed to generate presigned URL: {e}")
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
                    
                    # Count by status categories
                    cursor.execute(f"""
                        SELECT 
                            COUNT(DISTINCT CASE WHEN ch.status = 'spam' THEN ch.id END) as spam_calls,
                            COUNT(DISTINCT CASE WHEN ch.status IN ('unanswered', 'failed', 'busy') THEN ch.id END) as missed_calls,
                            COUNT(DISTINCT CASE WHEN ch.status = 'completed' AND appt.id IS NULL THEN ch.id END) as query_calls,
                            COUNT(DISTINCT CASE WHEN appt.id IS NOT NULL THEN ch.id END) as booked_calls
                        FROM call_history ch
                        JOIN agents a ON ch.agent_id = a.id
                        LEFT JOIN appointments appt ON ch.call_id = appt.call_id
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
                        "booked": status_counts["booked_calls"],
                        "spam": status_counts["spam_calls"],
                        "query": status_counts["query_calls"],
                        "missed": status_counts["missed_calls"],
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
                        WHERE (admin_id = %s OR user_id = %s) AND is_active = TRUE
                    """, (admin_id, admin_id))
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
                        WHERE (a.admin_id = %s OR a.user_id = %s) AND a.is_active = TRUE
                        GROUP BY a.id
                        ORDER BY total_calls DESC, a.created_at DESC
                        LIMIT %s OFFSET %s
                    """, (admin_id, admin_id, page_size, offset))
                    
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
                        WHERE a.id = %s AND (a.admin_id = %s OR a.user_id = %s)
                    """, (agent_id, admin_id, admin_id))
                    
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
                logging.info(f" Created agent {result['id']} with minutes limit")
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
                    'allowed_minutes', 'user_id'  
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
                logging.info(f" Updated agent {agent_id}")
                return result
                
            except Exception as e:
                conn.rollback()
                logging.error(f"Error updating agent: {e}")
                raise
       

    def update_user_agent_id(self, user_id: int, agent_id: int):
        """
        Update user's agent_id column with the newly created agent ID.
        This links the user to their assigned agent.
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE users 
                        SET agent_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        RETURNING id;
                    """, (agent_id, user_id))
                    
                    result = cursor.fetchone()
                    conn.commit()
                    
                    if result:
                        logging.info(f" User {user_id} linked to Agent {agent_id}")
                        return True
                    else:
                        logging.warning(f" User {user_id} not found when linking to agent")
                        return False
                        
            except Exception as e:
                conn.rollback()
                logging.error(f"Error updating user agent_id: {e}")
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
                logging.info(" appointments table created")
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
                logging.info(f" Created appointment {result['id']} for user {appointment_data['user_id']}")
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
                logging.info(f" Updated appointment {appointment_id} status to {status}")
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
                logging.info(f" Linked appointment {appointment_id} to call {call_id}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error linking appointment to call: {e}")
                raise

    def get_user_appointments(self, user_id: int, from_date: str = None):
        """
        Get appointments for a user from local database.
        Optionally filter by date (from_date onwards).
        
        Args:
            user_id: User ID
            from_date: Optional ISO format date string (YYYY-MM-DD)
            
        Returns:
            List of appointment dictionaries
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if from_date:
                    cursor.execute("""
                        SELECT * FROM appointments
                        WHERE user_id = %s AND scheduled_time >= %s
                        ORDER BY scheduled_time ASC
                    """, (user_id, from_date))
                else:
                    cursor.execute("""
                        SELECT * FROM appointments
                        WHERE user_id = %s
                        ORDER BY scheduled_time ASC
                    """, (user_id,))
                
                appointments = cursor.fetchall()
                
                # Format datetime fields
                for apt in appointments:
                    if apt.get("scheduled_time"):
                        apt["scheduled_time"] = apt["scheduled_time"].isoformat()
                    if apt.get("created_at"):
                        apt["created_at"] = apt["created_at"].isoformat()
                    if apt.get("updated_at"):
                        apt["updated_at"] = apt["updated_at"].isoformat()
                
                return appointments

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
                logging.info(" Agent fields added/verified successfully")
            except Exception as e:
                conn.rollback()
                logging.error(f" Error adding agent fields: {e}")
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
                        f" Agent {agent_id}: Used minutes updated to {result[1]}/{result[2]}"
                    )
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f" Error updating used minutes: {e}")
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
                
                logging.info(f" Agent {agent_id} minutes reset (limit: {result[1]} min)")
                return True
            except Exception as e:
                conn.rollback()
                logging.error(f" Error resetting minutes: {e}")
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

    def get_user_call_statistics(self, user_id: int):
        """
        Get call statistics for a user.
        Returns total calls, missed calls, and completed calls for agents
        belonging to or assigned to this user.
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_calls,
                        COUNT(CASE WHEN ch.status = 'unanswered' THEN 1 END) as missed_calls,
                        COUNT(CASE WHEN ch.status = 'completed' THEN 1 END) as completed_calls
                    FROM call_history ch
                    INNER JOIN agents a ON ch.agent_id = a.id
                    WHERE (a.admin_id = %s OR a.user_id = %s)
                """, (user_id, user_id))
                
                result = cursor.fetchone()
                
                return {
                    "total_calls": int(result["total_calls"]) if result else 0,
                    "missed_calls": int(result["missed_calls"]) if result else 0,
                    "completed_calls": int(result["completed_calls"]) if result else 0
                }

    # ==================== GOOGLE CALENDAR INTEGRATION ====================
    
    def create_google_credentials_table(self):
        """
        Create google_credentials table to store OAuth tokens.
        One credential set per user (UNIQUE constraint on user_id).
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS google_credentials (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                            access_token TEXT NOT NULL,
                            refresh_token TEXT,
                            token_expiry TIMESTAMPTZ,
                            scopes TEXT[],
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(user_id)
                        );
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_google_credentials_user_id 
                        ON google_credentials(user_id);
                    """)
                conn.commit()
                logging.info("✅ google_credentials table created")
            except Exception as e:
                logging.error(f"Error creating google_credentials table: {e}")

    def save_google_credentials(self, user_id: int, access_token: str, refresh_token: str = None, 
                                token_expiry: datetime = None, scopes: list = None):
        """
        Save or update Google OAuth credentials for a user (upsert).
        
        Args:
            user_id: User ID
            access_token: OAuth access token
            refresh_token: OAuth refresh token (optional)
            token_expiry: Token expiration datetime (optional)
            scopes: List of OAuth scopes (optional)
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO google_credentials (
                            user_id, access_token, refresh_token, token_expiry, scopes, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET
                            access_token = EXCLUDED.access_token,
                            refresh_token = EXCLUDED.refresh_token,
                            token_expiry = EXCLUDED.token_expiry,
                            scopes = EXCLUDED.scopes,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING *;
                    """, (user_id, access_token, refresh_token, token_expiry, scopes))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Saved Google credentials for user {user_id}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error saving Google credentials: {e}")
                raise

    def get_google_credentials(self, user_id: int):
        """
        Get Google OAuth credentials for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            Dict with access_token, refresh_token, token_expiry, scopes or None
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        access_token,
                        refresh_token,
                        token_expiry,
                        scopes
                    FROM google_credentials
                    WHERE user_id = %s
                """, (user_id,))
                result = cursor.fetchone()
                
                if result and result.get("token_expiry"):
                    # Convert to ISO format string for service
                    result["token_expiry"] = result["token_expiry"].isoformat()
                
                return result

    def delete_google_credentials(self, user_id: int):
        """
        Delete Google OAuth credentials for a user (disconnect).
        
        Args:
            user_id: User ID
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        DELETE FROM google_credentials
                        WHERE user_id = %s
                        RETURNING id;
                    """, (user_id,))
                    result = cursor.fetchone()
                conn.commit()
                
                if result:
                    logging.info(f"✅ Deleted Google credentials for user {user_id}")
                    return True
                else:
                    logging.warning(f"⚠️ No Google credentials found for user {user_id}")
                    return False
            except Exception as e:
                conn.rollback()
                logging.error(f"Error deleting Google credentials: {e}")
                raise

    def create_google_appointment(self, user_id: int, appointment_date: str, start_time: str, 
                                  end_time: str, attendee_email: str, attendee_name: str = None,
                                  title: str = "", description: str = None, notes: str = None,
                                  google_event_id: str = None):
        """
        Save a Google Calendar appointment to local database.
        
        Args:
            user_id: User ID
            appointment_date: Date in YYYY-MM-DD format
            start_time: Start time in HH:MM format
            end_time: End time in HH:MM format
            attendee_email: Attendee's email address
            attendee_name: Attendee's name (optional)
            title: Appointment title
            description: Appointment description (optional)
            notes: Additional notes (optional)
            google_event_id: Google Calendar event ID (optional)
        """
        with self.get_connection_context() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        INSERT INTO google_appointments (
                            user_id, appointment_date, start_time, end_time,
                            attendee_email, attendee_name, title, description,
                            notes, google_event_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *;
                    """, (
                        user_id, appointment_date, start_time, end_time,
                        attendee_email, attendee_name, title, description,
                        notes, google_event_id
                    ))
                    result = cursor.fetchone()
                conn.commit()
                logging.info(f"✅ Created Google appointment {result['id']} for user {user_id}")
                return result
            except Exception as e:
                conn.rollback()
                logging.error(f"Error creating Google appointment: {e}")
                raise

    def get_google_appointments_by_user(self, user_id: int, start_date: str = None, end_date: str = None):
        """
        Get Google Calendar appointments for a user.
        
        Args:
            user_id: User ID
            start_date: Start date filter (YYYY-MM-DD, optional)
            end_date: End date filter (YYYY-MM-DD, optional)
            
        Returns:
            List of appointment dicts
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if start_date and end_date:
                    cursor.execute("""
                        SELECT * FROM google_appointments
                        WHERE user_id = %s 
                        AND appointment_date BETWEEN %s AND %s
                        ORDER BY appointment_date DESC, start_time DESC
                    """, (user_id, start_date, end_date))
                elif start_date:
                    cursor.execute("""
                        SELECT * FROM google_appointments
                        WHERE user_id = %s 
                        AND appointment_date >= %s
                        ORDER BY appointment_date DESC, start_time DESC
                    """, (user_id, start_date))
                else:
                    cursor.execute("""
                        SELECT * FROM google_appointments
                        WHERE user_id = %s
                        ORDER BY appointment_date DESC, start_time DESC
                    """, (user_id,))
                
                appointments = cursor.fetchall()
                
                # Format dates and times
                for apt in appointments:
                    if apt.get("appointment_date"):
                        apt["appointment_date"] = apt["appointment_date"].isoformat()
                    if apt.get("start_time"):
                        apt["start_time"] = str(apt["start_time"])
                    if apt.get("end_time"):
                        apt["end_time"] = str(apt["end_time"])
                    if apt.get("created_at"):
                        apt["created_at"] = apt["created_at"].isoformat()
                    if apt.get("updated_at"):
                        apt["updated_at"] = apt["updated_at"].isoformat()
                
                return appointments

    def get_google_appointments_by_date(self, user_id: int, date: str):
        """
        Get Google Calendar appointments for a specific date.
        
        Args:
            user_id: User ID
            date: Date in YYYY-MM-DD format
            
        Returns:
            List of appointment dicts for that date
        """
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM google_appointments
                    WHERE user_id = %s AND appointment_date = %s
                    ORDER BY start_time ASC
                """, (user_id, date))
                
                appointments = cursor.fetchall()
                
                # Format times
                for apt in appointments:
                    if apt.get("appointment_date"):
                        apt["appointment_date"] = apt["appointment_date"].isoformat()
                    if apt.get("start_time"):
                        apt["start_time"] = str(apt["start_time"])
                    if apt.get("end_time"):
                        apt["end_time"] = str(apt["end_time"])
                    if apt.get("created_at"):
                        apt["created_at"] = apt["created_at"].isoformat()
                    if apt.get("updated_at"):
                        apt["updated_at"] = apt["updated_at"].isoformat()
                
                return appointments


