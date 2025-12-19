"""Telegram integration module using aiogram."""

from .service import TelegramService
from .keyboards import MenuButtons, get_main_menu_keyboard, get_confirmation_keyboard

__all__ = [
    "TelegramService",
    "MenuButtons",
    "get_main_menu_keyboard",
    "get_confirmation_keyboard",
]
