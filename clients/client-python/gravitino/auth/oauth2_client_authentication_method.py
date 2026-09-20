from enum import Enum


class OAuth2ClientAuthenticationMethod(Enum):
    CLIENT_SECRET_POST = "client_secret_post"
    CLIENT_SECRET_BASIC = "client_secret_basic"