"""
example usage if plan to authenticate using a non-default profile locally
"""
from dbrxish.session import SeshBuilder


spark, dbutils, wc = SeshBuilder(profile="DEFAULT-PROFILE").get_session()

user_shane = next(
    u for u in wc.users.list(filter="userName eq stephenson.shane.a@gmail.com")
)
