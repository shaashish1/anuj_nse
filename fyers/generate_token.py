import json
import requests
import pyotp
import hashlib
from urllib import parse
import sys
import credentials as cd

# API Endpoints
BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api-t1.fyers.in/api/v3"
URL_VERIFY_CLIENT_ID = BASE_URL + "/send_login_otp"
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"

SUCCESS = 1
ERROR = -1

# Load credentials from credentials.py
CLIENT_ID = cd.user_name
PIN = f"{cd.pin1}{cd.pin2}{cd.pin3}{cd.pin4}"
APP_ID = cd.client_id.split("-")[0]
APP_TYPE = cd.client_id.split("-")[1]
APP_SECRET = cd.secret_key
TOTP_SECRET_KEY = cd.totp_key
REDIRECT_URI = cd.redirect_uri


def verify_client_id(client_id):
    """Step 1: Verify Client ID and Get Request Key"""
    try:
        payload = {"fy_id": client_id, "app_id": "2"}
        result_string = requests.post(url=URL_VERIFY_CLIENT_ID, json=payload)

        if result_string.status_code != 200:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    
    except Exception as e:
        return [ERROR, str(e)]
    

def generate_totp(secret):
    """Step 2: Generate TOTP"""
    try:
        generated_totp = pyotp.TOTP(secret).now()
        return [SUCCESS, generated_totp]
    except Exception as e:
        return [ERROR, str(e)]


def verify_totp(request_key, totp):
    """Step 3: Verify TOTP and Get New Request Key"""
    try:
        payload = {"request_key": request_key, "otp": totp}
        result_string = requests.post(url=URL_VERIFY_TOTP, json=payload)

        if result_string.status_code != 200:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        request_key = result["request_key"]
        return [SUCCESS, request_key]
    
    except Exception as e:
        return [ERROR, str(e)]


def verify_PIN(request_key, pin):
    """Step 4: Verify PIN and Get Access Token"""
    try:
        payload = {"request_key": request_key, "identity_type": "pin", "identifier": pin}
        result_string = requests.post(url=URL_VERIFY_PIN, json=payload)

        if result_string.status_code != 200:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        access_token = result["data"]["access_token"]
        return [SUCCESS, access_token]
    
    except Exception as e:
        return [ERROR, str(e)]


def token(client_id, app_id, redirect_uri, app_type, access_token):
    """Step 5: Get Auth Code for API V3"""
    try:
        payload = {
            "fyers_id": client_id,
            "app_id": app_id,
            "redirect_uri": redirect_uri,
            "appType": app_type,
            "code_challenge": "",
            "state": "sample_state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        headers = {'Authorization': f'Bearer {access_token}'}
        result_string = requests.post(url=URL_TOKEN, json=payload, headers=headers)

        if result_string.status_code != 308:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        url = result["Url"]
        auth_code = parse.parse_qs(parse.urlparse(url).query)['auth_code'][0]

        return [SUCCESS, auth_code]
    
    except Exception as e:
        return [ERROR, str(e)]


def validate_authcode(auth_code):
    """Step 6: Validate Auth Code and Get Final Access Token"""
    try:
        app_id_hash = hashlib.sha256(f"{APP_ID}-{APP_TYPE}:{APP_SECRET}".encode()).hexdigest()
        payload = {"grant_type": "authorization_code", "appIdHash": app_id_hash, "code": auth_code}

        result_string = requests.post(url=URL_VALIDATE_AUTH_CODE, json=payload)

        if result_string.status_code != 200:
            return [ERROR, result_string.text]

        result = json.loads(result_string.text)
        access_token = result["access_token"]
        return [SUCCESS, access_token]
    
    except Exception as e:
        return [ERROR, str(e)]


def main():
    """Execute the entire authentication flow"""
    # Step 1: Get Request Key
    verify_client_id_result = verify_client_id(client_id=CLIENT_ID)
    if verify_client_id_result[0] != SUCCESS:
        print(f"Client ID verification failed: {verify_client_id_result[1]}")
        sys.exit()
    request_key = verify_client_id_result[1]

    # Step 2: Generate TOTP
    generate_totp_result = generate_totp(secret=TOTP_SECRET_KEY)
    if generate_totp_result[0] != SUCCESS:
        print(f"TOTP generation failed: {generate_totp_result[1]}")
        sys.exit()
    totp = generate_totp_result[1]

    # Step 3: Verify TOTP
    verify_totp_result = verify_totp(request_key=request_key, totp=totp)
    if verify_totp_result[0] != SUCCESS:
        print(f"TOTP verification failed: {verify_totp_result[1]}")
        sys.exit()
    request_key_2 = verify_totp_result[1]

    # Step 4: Verify PIN
    verify_pin_result = verify_PIN(request_key=request_key_2, pin=PIN)
    if verify_pin_result[0] != SUCCESS:
        print(f"PIN verification failed: {verify_pin_result[1]}")
        sys.exit()
    access_token = verify_pin_result[1]

    # Step 5: Get Auth Code
    token_result = token(client_id=CLIENT_ID, app_id=APP_ID, redirect_uri=REDIRECT_URI, app_type=APP_TYPE, access_token=access_token)
    if token_result[0] != SUCCESS:
        print(f"Auth Code retrieval failed: {token_result[1]}")
        sys.exit()
    auth_code = token_result[1]

    # Step 6: Validate Auth Code to Get Final Access Token
    validate_authcode_result = validate_authcode(auth_code=auth_code)
    if validate_authcode_result[0] != SUCCESS:
        print(f"Auth Code validation failed: {validate_authcode_result[1]}")
        sys.exit()
    final_access_token = validate_authcode_result[1]

    # Save Access Token to File
    with open("access_token.py", "w") as f:
        f.write(f'client_id = "{cd.client_id}"\n')
        f.write(f'access_token = "{final_access_token}"\n')
    print(f"\nAccess Token Saved: {final_access_token}\n")


if __name__ == "__main__":
    main()
