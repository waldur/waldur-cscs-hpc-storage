import os
from fastapi.logger import logger
from pydantic import BaseModel

from fastapi_keycloak_middleware import (
    KeycloakConfiguration,
    setup_keycloak_middleware,
)


class User(BaseModel):
    """Model for OIDC user."""

    preferred_username: str


async def user_mapper(userinfo: dict) -> User:
    """Maps user info to a custom user structure."""
    logger.info("Received userinfo in user_mapper: %s", userinfo)
    logger.info(
        "Available userinfo keys: %s", list(userinfo.keys()) if userinfo else "None"
    )

    # Extract preferred_username (should be service-account-hpc-mp-storage-service-account-dci)
    preferred_username = userinfo.get("preferred_username")
    logger.info("Extracted preferred_username: %s", preferred_username)

    # Log additional useful claims for debugging
    logger.info("Client ID: %s", userinfo.get("clientId"))
    logger.info("Subject: %s", userinfo.get("sub"))
    logger.info("Roles: %s", userinfo.get("roles"))
    logger.info("Groups: %s", userinfo.get("groups"))

    if not preferred_username:
        logger.error("Missing 'preferred_username' claim in userinfo: %s", userinfo)
        # Use sub as fallback since it's always present
        fallback_username = userinfo.get("sub", "unknown_user")
        logger.warning("Using fallback username from 'sub': %s", fallback_username)
        preferred_username = fallback_username

    return User(preferred_username=preferred_username)


def mock_user() -> User:
    """Return a mock user when auth is disabled."""
    return User(preferred_username="dev_user")


def setup_auth(app, config):
    CSCS_KEYCLOAK_URL = os.getenv("CSCS_KEYCLOAK_URL")
    if CSCS_KEYCLOAK_URL is None and config.auth:
        CSCS_KEYCLOAK_URL = config.auth.keycloak_url
    if CSCS_KEYCLOAK_URL is None:
        CSCS_KEYCLOAK_URL = "https://auth-tds.cscs.ch/auth/"

    CSCS_KEYCLOAK_REALM = os.getenv("CSCS_KEYCLOAK_REALM")
    if CSCS_KEYCLOAK_REALM is None and config.auth:
        CSCS_KEYCLOAK_REALM = config.auth.keycloak_realm
    if CSCS_KEYCLOAK_REALM is None:
        CSCS_KEYCLOAK_REALM = "cscs"

    CSCS_KEYCLOAK_CLIENT_ID = os.getenv("CSCS_KEYCLOAK_CLIENT_ID")
    if CSCS_KEYCLOAK_CLIENT_ID is None and config.auth:
        CSCS_KEYCLOAK_CLIENT_ID = config.auth.keycloak_client_id

    CSCS_KEYCLOAK_CLIENT_SECRET = os.getenv("CSCS_KEYCLOAK_CLIENT_SECRET")
    if CSCS_KEYCLOAK_CLIENT_SECRET is None and config.auth:
        CSCS_KEYCLOAK_CLIENT_SECRET = config.auth.keycloak_client_secret

    logger.info("Setting up Keycloak authentication")
    logger.info("Keycloak URL: %s", CSCS_KEYCLOAK_URL)
    logger.info("Keycloak Realm: %s", CSCS_KEYCLOAK_REALM)
    logger.info("Keycloak Client ID: %s", CSCS_KEYCLOAK_CLIENT_ID)
    logger.info(
        "Keycloak Client Secret: %s",
        "***REDACTED***" if CSCS_KEYCLOAK_CLIENT_SECRET else "NOT SET",
    )

    if not CSCS_KEYCLOAK_CLIENT_ID or not CSCS_KEYCLOAK_CLIENT_SECRET:
        logger.error(
            "Missing required Keycloak configuration: CLIENT_ID or CLIENT_SECRET not set"
        )
        error_msg = (
            "CSCS_KEYCLOAK_CLIENT_ID and CSCS_KEYCLOAK_CLIENT_SECRET must be set"
        )
        raise ValueError(error_msg)

    keycloak_config = KeycloakConfiguration(
        url=CSCS_KEYCLOAK_URL,
        realm=CSCS_KEYCLOAK_REALM,
        client_id=CSCS_KEYCLOAK_CLIENT_ID,
        client_secret=CSCS_KEYCLOAK_CLIENT_SECRET,
        # Allow missing claims and handle them in user_mapper
        reject_on_missing_claim=False,
        # Specify required claims based on your token structure
        claims=["sub", "preferred_username", "clientId", "roles", "groups"],
        # Decode options for flexibility
        decode_options={
            "verify_signature": True,
            "verify_aud": False,  # Disable audience verification if causing issues
            "verify_exp": True,
        },
    )

    try:
        setup_keycloak_middleware(
            app,
            keycloak_configuration=keycloak_config,
            user_mapper=user_mapper,
        )
        logger.info("Keycloak middleware setup completed successfully")
    except Exception as e:
        logger.error("Failed to setup Keycloak middleware: %s", e)
        raise
