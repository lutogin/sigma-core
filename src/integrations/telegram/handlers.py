"""Command and callback handlers for Telegram bot."""

from typing import TYPE_CHECKING, Callable, Awaitable, Any

from aiogram import Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery

from .keyboards import MenuButtons, get_main_menu_keyboard, get_confirmation_keyboard

if TYPE_CHECKING:
    from .telegram import TelegramService


# Create router for handlers
router = Router(name="telegram_handlers")


class TelegramHandlers:
    """
    Handler class for Telegram bot commands and callbacks.

    All button callbacks are stubs that can be connected to actual business logic.
    """

    def __init__(self, service: "TelegramService"):
        """
        Initialize handlers with reference to service.

        Args:
            service: TelegramService instance.
        """
        self.service = service
        self.logger = service.logger
        self.admin_chat_id = service.admin_chat_id

        # State for pending operations
        self.pending_close_symbol: str | None = None

        # Callback storage for external handlers
        self._callbacks: dict[str, Callable[..., Awaitable[Any]]] = {}

    def register_callback(
        self,
        name: str,
        callback: Callable[..., Awaitable[Any]]
    ) -> None:
        """
        Register an external callback for a specific action.

        Args:
            name: Callback name (e.g., 'positions', 'opportunities', 'close_all').
            callback: Async function to call when action is triggered.
        """
        self._callbacks[name] = callback
        self.logger.debug(f"Registered callback: {name}")

    async def _invoke_callback(self, name: str, **kwargs: Any) -> Any:
        """
        Invoke a registered callback if available.

        Args:
            name: Callback name.
            **kwargs: Arguments to pass to callback.

        Returns:
            Callback result or None if not registered.
        """
        if name in self._callbacks:
            return await self._callbacks[name](**kwargs)
        self.logger.debug(f"Callback '{name}' not registered (stub)")
        return None

    def is_owner(self, chat_id: int) -> bool:
        """
        Check if the chat ID belongs to the admin.

        Args:
            chat_id: Telegram chat ID to check.

        Returns:
            True if owner, False otherwise.
        """
        return str(chat_id) == str(self.admin_chat_id)

    def is_waiting_for_symbol_input(self, text: str) -> bool:
        """
        Check if we're waiting for symbol input.

        Args:
            text: Message text to check.

        Returns:
            True if text looks like a symbol input.
        """
        if not text:
            return False

        return (
            text not in MenuButtons.all_buttons()
            and len(text) <= 10
            and text.replace("/", "").isalnum()
        )

    # ==================== Command Handlers ====================

    async def handle_start(self, message: Message) -> None:
        """Handle /start or /menu command - show main menu."""
        if not self.is_owner(message.chat.id):
            self.logger.warning(
                f"Unauthorized access attempt from chat ID: {message.chat.id}"
            )
            return

        await message.answer(
            "🤖 *Trading Bot*\n\nMenu is ready. Type /help to see all available commands.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard(),
        )

    async def handle_help(self, message: Message) -> None:
        """Handle /help command - show available commands."""
        if not self.is_owner(message.chat.id):
            return

        help_message = """📚 *Available Commands*

🔧 *Trade Management:*
  /ban <symbol> - Ban a symbol from trading
    _Example:_ `/ban eth`

📊 *Market Data:*
  /ws - Get WebSocket market data
  /stat - Get balances snapshots
  /ex-stat - Get exchanges statistics

❓ *Help:*
  /help - Show this help message
  /start or /menu - Show main menu

🎛️ *Main Menu Buttons:*
  📈 Positions - View open positions
  🔄 Opportunities - View arbitrage opportunities
  💰 Get balances - View exchange balances
  🎯 Close Symbol - Close positions for a symbol
  ⚠️ Close All - Close all positions
  ⏹️ Stop Trading - Stop automated trading
  ▶️ Start Trading - Start automated trading
"""
        await message.answer(help_message, parse_mode="Markdown")

    async def handle_ban(self, message: Message) -> None:
        """Handle /ban <symbol> command."""
        if not self.is_owner(message.chat.id):
            return

        text = message.text or ""
        parts = text.split(maxsplit=1)

        if len(parts) < 2:
            await message.answer(
                "❌ Please provide a symbol. Example: `/ban eth`",
                parse_mode="Markdown",
            )
            return

        symbol_base = parts[1].strip().upper()
        formatted_symbol = f"{symbol_base}/USDT:USDT"

        # Invoke callback (stub)
        await self._invoke_callback("ban_symbol", symbol=formatted_symbol)

        await message.answer(
            f"✅ Symbol {formatted_symbol} has been banned from trading",
            parse_mode="Markdown",
        )

    async def handle_ws(self, message: Message) -> None:
        """Handle /ws command - get WebSocket market data."""
        if not self.is_owner(message.chat.id):
            return

        await self._invoke_callback("market_data_request")
        await message.answer("📊 Fetching market data...")

    async def handle_stat(self, message: Message) -> None:
        """Handle /stat command - get balances snapshots."""
        if not self.is_owner(message.chat.id):
            return

        await self._invoke_callback("balances_snapshot_request")
        await message.answer("📊 Fetching balances snapshots...")

    async def handle_ex_stat(self, message: Message) -> None:
        """Handle /ex-stat command - get exchanges statistics."""
        if not self.is_owner(message.chat.id):
            return

        await self._invoke_callback("exchanges_stat_request")
        await message.answer("📊 Fetching exchanges statistics...")

    # ==================== Menu Button Handlers ====================

    async def handle_positions(self, message: Message) -> None:
        """Handle Positions button click."""
        if not self.is_owner(message.chat.id):
            return

        await self._invoke_callback("get_positions")
        # Stub response
        self.logger.info("Positions button clicked (stub)")

    async def handle_opportunities(self, message: Message) -> None:
        """Handle Opportunities button click."""
        if not self.is_owner(message.chat.id):
            return

        await self._invoke_callback("get_opportunities")
        self.logger.info("Opportunities button clicked (stub)")

    async def handle_close_all(self, message: Message) -> None:
        """Handle Close All button - show confirmation."""
        if not self.is_owner(message.chat.id):
            return

        await message.answer(
            "⚠️ *Close All Positions*\n\nAre you sure you want to close all open positions?",
            parse_mode="Markdown",
            reply_markup=get_confirmation_keyboard("close_all"),
        )

    async def handle_close_symbol(self, message: Message) -> None:
        """Handle Close Symbol button - request symbol input."""
        if not self.is_owner(message.chat.id):
            return

        await message.answer(
            "🎯 *Close Symbol Positions*\n\n"
            "Please enter the symbol to close (e.g., STRK, BTC, ETH):",
            parse_mode="Markdown",
        )
        self.pending_close_symbol = "awaiting"

    async def handle_stop_trading(self, message: Message) -> None:
        """Handle Stop Trading button - show confirmation."""
        if not self.is_owner(message.chat.id):
            return

        await message.answer(
            "🛑 *Stop Trading*\n\nAre you sure you want to stop trading activity?",
            parse_mode="Markdown",
            reply_markup=get_confirmation_keyboard("stop_trading"),
        )

    async def handle_start_trading(self, message: Message) -> None:
        """Handle Start Trading button - show confirmation."""
        if not self.is_owner(message.chat.id):
            return

        await message.answer(
            "🟢 *Start Trading*\n\nAre you sure you want to start trading activity?",
            parse_mode="Markdown",
            reply_markup=get_confirmation_keyboard("start_trading"),
        )

    async def handle_get_balances(self, message: Message) -> None:
        """Handle Get Balances button click."""
        if not self.is_owner(message.chat.id):
            return

        await self._invoke_callback("get_balances")
        self.logger.info("Get balances button clicked (stub)")

    async def handle_symbol_input(self, message: Message) -> None:
        """Handle symbol input for close symbol action."""
        if not self.is_owner(message.chat.id):
            return

        text = message.text or ""
        clean_symbol = text.strip().upper()

        if not clean_symbol:
            await message.answer("❌ Invalid symbol. Please try again.")
            return

        self.pending_close_symbol = clean_symbol

        await message.answer(
            f"🎯 *Close {clean_symbol} Positions*\n\n"
            f"Are you sure you want to close all {clean_symbol} positions?",
            parse_mode="Markdown",
            reply_markup=get_confirmation_keyboard("close_symbol"),
        )

    # ==================== Callback Query Handlers ====================

    async def handle_callback_confirm_close_all(
        self,
        callback_query: CallbackQuery
    ) -> None:
        """Handle confirmation of Close All action."""
        await callback_query.answer("✅ Closing all positions...")

        await self._invoke_callback("close_all_positions")

        if callback_query.message:
            await callback_query.message.answer("✅ All positions are being closed")
            await callback_query.message.edit_reply_markup(reply_markup=None)

    async def handle_callback_confirm_close_symbol(
        self,
        callback_query: CallbackQuery
    ) -> None:
        """Handle confirmation of Close Symbol action."""
        await callback_query.answer("✅ Closing symbol positions...")

        symbol = self.pending_close_symbol
        if symbol and symbol != "awaiting":
            await self._invoke_callback("close_symbol_positions", symbol=symbol)

            if callback_query.message:
                await callback_query.message.answer(
                    f"✅ Closing positions for {symbol}..."
                )
            self.pending_close_symbol = None
        else:
            if callback_query.message:
                await callback_query.message.answer("❌ No symbol specified")

        if callback_query.message:
            await callback_query.message.edit_reply_markup(reply_markup=None)

    async def handle_callback_confirm_stop_trading(
        self,
        callback_query: CallbackQuery
    ) -> None:
        """Handle confirmation of Stop Trading action."""
        await callback_query.answer("✅ Stopping trading...")

        await self._invoke_callback("stop_trading")

        if callback_query.message:
            await callback_query.message.answer("🛑 Trading stopped")
            await callback_query.message.edit_reply_markup(reply_markup=None)

    async def handle_callback_confirm_start_trading(
        self,
        callback_query: CallbackQuery
    ) -> None:
        """Handle confirmation of Start Trading action."""
        await callback_query.answer("✅ Starting trading...")

        await self._invoke_callback("start_trading")

        if callback_query.message:
            await callback_query.message.answer("🟢 Trading started")
            await callback_query.message.edit_reply_markup(reply_markup=None)

    async def handle_callback_cancel(self, callback_query: CallbackQuery) -> None:
        """Handle Cancel button in confirmation dialog."""
        await callback_query.answer("❌ Cancelled")

        self.pending_close_symbol = None

        if callback_query.message:
            await callback_query.message.answer("❌ Action cancelled")
            await callback_query.message.edit_reply_markup(reply_markup=None)

    async def handle_callback_get_rates(self, callback_query: CallbackQuery) -> None:
        """
        Handle callback for get_rates buttons.

        Format: get_rates_<symbol>:<exchange1>,<exchange2>,...
        """
        data = callback_query.data or ""

        import re
        match = re.match(r"^get_rates_([^:]+):(.+)$", data)

        if not match:
            await callback_query.answer("❌ Invalid callback data format")
            return

        symbol = match.group(1)
        exchanges_str = match.group(2)
        exchanges = [e.strip() for e in exchanges_str.split(",")]

        if not exchanges:
            await callback_query.answer("❌ No exchanges specified")
            return

        self.logger.info(
            f"📊 Requesting rates for {symbol} on {len(exchanges)} exchanges: "
            f"{', '.join(exchanges)}"
        )

        await callback_query.answer("📊 Fetching rates...")

        await self._invoke_callback(
            "get_rates",
            symbol=f"{symbol}/USDT:USDT",
            exchanges=exchanges,
        )


def setup_handlers(handlers: TelegramHandlers) -> Router:
    """
    Setup all handlers on the router.

    Args:
        handlers: TelegramHandlers instance.

    Returns:
        Configured Router.
    """
    # Command handlers
    router.message.register(
        handlers.handle_start,
        CommandStart()
    )
    router.message.register(
        handlers.handle_start,
        Command("menu")
    )
    router.message.register(
        handlers.handle_help,
        Command("help")
    )
    router.message.register(
        handlers.handle_ban,
        Command("ban")
    )
    router.message.register(
        handlers.handle_ws,
        Command("ws")
    )
    router.message.register(
        handlers.handle_stat,
        Command("stat")
    )
    router.message.register(
        handlers.handle_ex_stat,
        Command("ex-stat")
    )

    # Menu button handlers
    router.message.register(
        handlers.handle_positions,
        F.text == MenuButtons.POSITIONS,
    )
    router.message.register(
        handlers.handle_opportunities,
        F.text == MenuButtons.OPPORTUNITIES,
    )
    router.message.register(
        handlers.handle_close_all,
        F.text == MenuButtons.CLOSE_ALL,
    )
    router.message.register(
        handlers.handle_close_symbol,
        F.text == MenuButtons.CLOSE_SYMBOL,
    )
    router.message.register(
        handlers.handle_stop_trading,
        F.text == MenuButtons.STOP_TRADING,
    )
    router.message.register(
        handlers.handle_start_trading,
        F.text == MenuButtons.START_TRADING,
    )
    router.message.register(
        handlers.handle_get_balances,
        F.text == MenuButtons.GET_BALANCES,
    )

    # Callback query handlers
    router.callback_query.register(
        handlers.handle_callback_confirm_close_all,
        F.data == "confirm_close_all",
    )
    router.callback_query.register(
        handlers.handle_callback_confirm_close_symbol,
        F.data == "confirm_close_symbol",
    )
    router.callback_query.register(
        handlers.handle_callback_confirm_stop_trading,
        F.data == "confirm_stop_trading",
    )
    router.callback_query.register(
        handlers.handle_callback_confirm_start_trading,
        F.data == "confirm_start_trading",
    )
    router.callback_query.register(
        handlers.handle_callback_cancel,
        F.data == "cancel",
    )
    router.callback_query.register(
        handlers.handle_callback_get_rates,
        F.data.startswith("get_rates_"),
    )

    return router
