"""
API Testing Script for Munif Project Backend
Tests all implemented endpoints including:
- Authentication (register, login)
- Business details submission
- Admin user management
"""

import requests
import json
import sys
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8000/api"
ADMIN_EMAIL = "admin@test.com"
ADMIN_PASSWORD = "admin123"
TEST_USER_EMAIL = "testuser@example.com"
TEST_USER_PASSWORD = "test123"

# Colors for terminal output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_test(name):
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}TEST: {name}{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}")

def print_success(message):
    print(f"{Colors.GREEN}✓ {message}{Colors.END}")

def print_error(message):
    print(f"{Colors.RED}✗ {message}{Colors.END}")

def print_info(message):
    print(f"{Colors.YELLOW}ℹ {message}{Colors.END}")

# Global variables to store tokens and IDs
admin_token = None
test_user_token = None
test_user_id = None

def test_1_register_admin():
    """Test: Register admin user"""
    print_test("1. Register Admin User")
    
    payload = {
        "username": "admin",
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }
    
    try:
        response = requests.post(f"{BASE_URL}/register", json=payload)
        
        if response.status_code == 201:
            print_success(f"Admin registered successfully")
            print_info(f"Response: {response.json()}")
            return True
        elif response.status_code == 400 and "already registered" in response.text.lower():
            print_info("Admin already exists (expected if running tests multiple times)")
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_2_register_test_user():
    """Test: Register test user (non-admin)"""
    global test_user_id
    print_test("2. Register Test User (Non-Admin)")
    
    payload = {
        "username": "testuser",
        "email": TEST_USER_EMAIL,
        "password": TEST_USER_PASSWORD
    }
    
    try:
        response = requests.post(f"{BASE_URL}/register", json=payload)
        
        if response.status_code == 201:
            print_success(f"Test user registered successfully")
            print_info(f"Response: {response.json()}")
            return True
        elif response.status_code == 400 and "already registered" in response.text.lower():
            print_info("Test user already exists")
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_3_login_admin():
    """Test: Login as admin"""
    global admin_token
    print_test("3. Login as Admin")
    
    payload = {
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }
    
    try:
        response = requests.post(f"{BASE_URL}/login", json=payload)
        
        if response.status_code == 200:
            data = response.json()
            admin_token = data.get("access_token")
            print_success(f"Admin login successful")
            print_info(f"Token: {admin_token[:50]}...")
            print_info(f"User: {data.get('user', {}).get('email')}")
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_4_login_non_admin_should_fail():
    """Test: Login as non-admin (should fail)"""
    print_test("4. Login as Non-Admin (Should Fail)")
    
    payload = {
        "email": TEST_USER_EMAIL,
        "password": TEST_USER_PASSWORD
    }
    
    try:
        response = requests.post(f"{BASE_URL}/login", json=payload)
        
        if response.status_code == 403:
            print_success(f"Non-admin login correctly blocked (403)")
            print_info(f"Response: {response.json()}")
            return True
        else:
            print_error(f"Unexpected response: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_5_get_all_users():
    """Test: Get all users (admin only)"""
    global test_user_id
    print_test("5. Get All Users (Admin Only)")
    
    if not admin_token:
        print_error("No admin token available")
        return False
    
    headers = {"Authorization": f"Bearer {admin_token}"}
    
    try:
        response = requests.get(f"{BASE_URL}/admin/users?page=1&page_size=20", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            users = data.get("data", {}).get("users", [])
            print_success(f"Retrieved {len(users)} users")
            
            # Find test user ID
            for user in users:
                if user.get("email") == TEST_USER_EMAIL:
                    test_user_id = user.get("id")
                    print_info(f"Test user ID: {test_user_id}")
                print_info(f"  - {user.get('username')} ({user.get('email')}) - Admin: {user.get('is_admin')}")
            
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_6_search_users():
    """Test: Search users"""
    print_test("6. Search Users")
    
    if not admin_token:
        print_error("No admin token available")
        return False
    
    headers = {"Authorization": f"Bearer {admin_token}"}
    
    try:
        response = requests.get(f"{BASE_URL}/admin/users?search=test", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            users = data.get("data", {}).get("users", [])
            print_success(f"Search returned {len(users)} users")
            for user in users:
                print_info(f"  - {user.get('username')} ({user.get('email')})")
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_7_promote_user_to_admin():
    """Test: Promote test user to admin"""
    print_test("7. Promote Test User to Admin")
    
    if not admin_token or not test_user_id:
        print_error("Missing admin token or test user ID")
        return False
    
    headers = {"Authorization": f"Bearer {admin_token}"}
    payload = {"is_admin": True}
    
    try:
        response = requests.patch(
            f"{BASE_URL}/admin/users/{test_user_id}/admin-status",
            headers=headers,
            json=payload
        )
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"User promoted successfully")
            print_info(f"Message: {data.get('message')}")
            print_info(f"User: {data.get('user')}")
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_8_login_promoted_user():
    """Test: Login as promoted user (should succeed now)"""
    global test_user_token
    print_test("8. Login as Promoted User (Should Succeed)")
    
    payload = {
        "email": TEST_USER_EMAIL,
        "password": TEST_USER_PASSWORD
    }
    
    try:
        response = requests.post(f"{BASE_URL}/login", json=payload)
        
        if response.status_code == 200:
            data = response.json()
            test_user_token = data.get("access_token")
            print_success(f"Promoted user login successful")
            print_info(f"Token: {test_user_token[:50]}...")
            print_info(f"User: {data.get('user', {}).get('email')}")
            print_info(f"Is Admin: {data.get('user', {}).get('is_admin')}")
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_9_submit_business_details():
    """Test: Submit business details"""
    print_test("9. Submit Business Details")
    
    if not test_user_token:
        print_error("No test user token available")
        return False
    
    headers = {"Authorization": f"Bearer {test_user_token}"}
    payload = {
        "agent_name": "Customer Support Bot",
        "business_name": "Test Corporation",
        "business_email": "contact@testcorp.com",
        "industry": "Technology"
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/submit-business-details",
            headers=headers,
            json=payload
        )
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"Business details submitted successfully")
            print_info(f"Message: {data.get('message')}")
            print_info("Check admin email for notification")
            return True
        else:
            print_error(f"Failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_10_self_demotion_should_fail():
    """Test: Try to demote self (should fail)"""
    print_test("10. Self-Demotion (Should Fail)")
    
    if not admin_token:
        print_error("No admin token available")
        return False
    
    # Get admin user ID first
    headers = {"Authorization": f"Bearer {admin_token}"}
    
    try:
        # Get admin user info
        response = requests.get(f"{BASE_URL}/admin/users", headers=headers)
        if response.status_code != 200:
            print_error("Failed to get users list")
            return False
        
        users = response.json().get("data", {}).get("users", [])
        admin_user = next((u for u in users if u.get("email") == ADMIN_EMAIL), None)
        
        if not admin_user:
            print_error("Admin user not found")
            return False
        
        admin_id = admin_user.get("id")
        
        # Try to demote self
        payload = {"is_admin": False}
        response = requests.patch(
            f"{BASE_URL}/admin/users/{admin_id}/admin-status",
            headers=headers,
            json=payload
        )
        
        if response.status_code == 400:
            print_success(f"Self-demotion correctly blocked (400)")
            print_info(f"Response: {response.json()}")
            return True
        else:
            print_error(f"Unexpected response: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_11_non_admin_access_should_fail():
    """Test: Demote test user and verify they can't access admin endpoints"""
    print_test("11. Non-Admin Access to Admin Endpoint (Should Fail)")
    
    if not admin_token or not test_user_id:
        print_error("Missing tokens or IDs")
        return False
    
    # First demote the test user
    headers = {"Authorization": f"Bearer {admin_token}"}
    payload = {"is_admin": False}
    
    try:
        response = requests.patch(
            f"{BASE_URL}/admin/users/{test_user_id}/admin-status",
            headers=headers,
            json=payload
        )
        
        if response.status_code != 200:
            print_error(f"Failed to demote user: {response.text}")
            return False
        
        print_info("Test user demoted successfully")
        
        # Now try to access admin endpoint with old token (should fail)
        test_headers = {"Authorization": f"Bearer {test_user_token}"}
        response = requests.get(f"{BASE_URL}/admin/users", headers=test_headers)
        
        if response.status_code == 403:
            print_success(f"Non-admin access correctly blocked (403)")
            print_info(f"Response: {response.json()}")
            return True
        else:
            print_error(f"Unexpected response: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def run_all_tests():
    """Run all tests"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}Starting API Tests - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}")
    
    tests = [
        test_1_register_admin,
        test_2_register_test_user,
        test_3_login_admin,
        test_4_login_non_admin_should_fail,
        test_5_get_all_users,
        test_6_search_users,
        test_7_promote_user_to_admin,
        test_8_login_promoted_user,
        test_9_submit_business_details,
        test_10_self_demotion_should_fail,
        test_11_non_admin_access_should_fail,
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__doc__, result))
        except Exception as e:
            print_error(f"Test crashed: {str(e)}")
            results.append((test.__doc__, False))
    
    # Summary
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}TEST SUMMARY{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = f"{Colors.GREEN}PASS{Colors.END}" if result else f"{Colors.RED}FAIL{Colors.END}"
        print(f"{status} - {test_name}")
    
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        print(f"{Colors.GREEN}✓ All tests passed!{Colors.END}")
        return 0
    else:
        print(f"{Colors.RED}✗ {total - passed} test(s) failed{Colors.END}")
        return 1

if __name__ == "__main__":
    try:
        # Check if server is running
        try:
            response = requests.get(f"{BASE_URL.replace('/api', '')}/docs", timeout=2)
            print_success("Server is running")
        except:
            print_error("Server is not running!")
            print_info("Please start the server with: python main.py")
            sys.exit(1)
        
        exit_code = run_all_tests()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Tests interrupted by user{Colors.END}")
        sys.exit(1)
