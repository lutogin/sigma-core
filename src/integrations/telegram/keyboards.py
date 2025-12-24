"""Keyboard definitions for Telegram bot."""

from dataclasses import dataclass

from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)


@dataclass(frozen=True)
class MenuButtons:
    """Menu button labels - matches the TypeScript reference."""

    POSITIONS: str = "📈 Positions"
    ENTRY_OBSERVER: str = "📝 Entry Observer"
    EXIT_OBSERVER: str = "📝 Exit Observer"
    OPPORTUNITIES: str = "🔄 Opportunities"
    CLOSE_ALL: str = "⚠️ Close All"
    CLOSE_SYMBOL: str = "🎯 Close Symbol"
    STOP_TRADING: str = "⏹️ Stop Trading"
    GET_BALANCES: str = "💰 Get balances"
    START_TRADING: str = "▶️ Start Trading"

    @classmethod
    def all_buttons(cls) -> list[str]:
        """Return all button labels."""
        return [
            cls.POSITIONS,
            cls.ENTRY_OBSERVER,
            cls.EXIT_OBSERVER,
            cls.OPPORTUNITIES,
            cls.CLOSE_ALL,
            cls.CLOSE_SYMBOL,
            cls.STOP_TRADING,
            cls.GET_BALANCES,
            cls.START_TRADING,
        ]


def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    """
    Create main menu with persistent buttons (ReplyKeyboard).

    Returns:
        ReplyKeyboardMarkup: The main menu keyboard.
    """
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=MenuButtons.POSITIONS),
                KeyboardButton(text=MenuButtons.ENTRY_OBSERVER),
                KeyboardButton(text=MenuButtons.OPPORTUNITIES),
            ],
            [
                KeyboardButton(text=MenuButtons.CLOSE_ALL),
                KeyboardButton(text=MenuButtons.CLOSE_SYMBOL),
                KeyboardButton(text=MenuButtons.EXIT_OBSERVER),
            ],
            [
                KeyboardButton(text=MenuButtons.STOP_TRADING),
                KeyboardButton(text=MenuButtons.GET_BALANCES),
                KeyboardButton(text=MenuButtons.START_TRADING),
            ],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )
    return keyboard


def get_confirmation_keyboard(action: str) -> InlineKeyboardMarkup:
    """
    Create inline keyboard for action confirmation.

    Args:
        action: The action identifier for callback data.

    Returns:
        InlineKeyboardMarkup: Confirmation keyboard with Confirm/Cancel buttons.
    """
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Confirm",
                    callback_data=f"confirm_{action}",
                ),
                InlineKeyboardButton(
                    text="❌ Cancel",
                    callback_data="cancel",
                ),
            ],
        ],
    )
    return keyboard
