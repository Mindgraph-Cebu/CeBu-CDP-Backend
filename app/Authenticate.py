import jwt
import os
from fastapi import HTTPException
from dotenv import load_dotenv

load_dotenv()

def authenticate_access_token(access_token: str):
    # return True
    try:
        # Decode the access token to extract header information
        token_header = jwt.get_unverified_header(access_token)

        # Decode the access token to extract payload information
        token_payload = jwt.decode(access_token, options={"verify_signature": False})

        # Get the tenant ID from the environment variables
        tenant_id = os.getenv("TENANT_ID")

        # Define the expected issuer for Microsoft Azure AD
        expected_issuer = f'https://sts.windows.net/{tenant_id}/'

        # Check if the token issuer matches the expected issuer and 
        if token_payload.get('iss') == expected_issuer and ('x5t' in token_header or 'kid' in token_header):
            return True
        else:
            return False

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired.")
    
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token format.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
