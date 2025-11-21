import os
from datetime import datetime, timedelta
from jose import jwt, JWTError
import logging

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your_dev_secret_key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 500  

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        logging.error(f"JWT decode error: {e}")
        return None
    

def create_password_reset_token(email: str, expires_delta: timedelta = timedelta(hours=1)):
    """Create a password reset token valid for 1 hour"""
    expire = datetime.utcnow() + expires_delta
    to_encode = {
        "sub": email,
        "type": "password_reset",
        "exp": expire
    }
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_password_reset_token(token: str):
    """Verify password reset token and return email"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        token_type = payload.get("type")
        
        if token_type != "password_reset" or not email:
            return None
        
        return email
    except JWTError:
        return None