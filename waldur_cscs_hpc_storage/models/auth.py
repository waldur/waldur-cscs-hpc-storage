from pydantic import BaseModel


class User(BaseModel):
    """Model for OIDC user."""

    preferred_username: str
