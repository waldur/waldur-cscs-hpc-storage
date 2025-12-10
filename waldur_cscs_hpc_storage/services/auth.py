from fastapi.logger import logger


from fastapi_keycloak_middleware import (
    KeycloakConfiguration,
    setup_keycloak_middleware,
)

from waldur_cscs_hpc_storage.config.auth import AuthConfig


from waldur_cscs_hpc_storage.models import User


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


def setup_auth(app, config: AuthConfig):
    keycloak_config = KeycloakConfiguration(
        url=str(config.keycloak_url),
        realm=config.keycloak_realm,
        client_id=config.keycloak_client_id,
        client_secret=config.keycloak_client_secret,
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
