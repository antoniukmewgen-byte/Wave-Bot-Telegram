from telegram.ext import Application
from config import MANAGERS

_app: Application = None
MANAGERS_BY_ID: dict = {v: k for k, v in MANAGERS.items()}
