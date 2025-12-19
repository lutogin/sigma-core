"""Telegram service using aiogram framework."""

import asyncio
from typing import Any, Callable, Awaitable

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, Message
from loguru import logger

from .handlers import TelegramHandlers, setup_handlers, router
from .keyboards import get_main_menu_keyboard


class TelegramService:
    """
    Telegram bot service using aiogram.

    Provides async interface for sending messages and handling commands.
    Mirrors the functionality of the TypeScript TelegramService reference.
    """

    def __init__(
        self,
        bot_token: str,
        admin_chat_id: str,
        log_name: str = "TelegramService",
    ):
        """
        Initialize Telegram service.

        Args:
            bot_token: Telegram bot token from BotFather.
            admin_chat_id: Admin chat ID for authorization.
            log_name: Logger name prefix.
        """
        self.bot_token = bot_token
        self.admin_chat_id = admin_chat_id
        self.logger = logger.bind(name=log_name)

        # Bot and dispatcher instances
        self._bot: Bot | None = None
        self._dispatcher: Dispatcher | None = None
        self._handlers: TelegramHandlers | None = None
        self._polling_task: asyncio.Task | None = None

        # Message tracking for auto-delete functionality
        self._last_positions_message_id: int | None = None
        self._last_opportunities_message_id: int | None = None
        self._last_balances_message_id: int | None = None
        self._last_market_data_message_id: int | None = None

        # Initialize bot if token provided
        if bot_token:
            self._initialize_bot()
        else:
            self.logger.warning("Telegram service disabled - no bot token provided")

    def _initialize_bot(self) -> None:
        """Initialize bot and dispatcher."""
        try:
            self._bot = Bot(
                token=self.bot_token,
                default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
            )
            self._dispatcher = Dispatcher()

            # Setup handlers
            self._handlers = TelegramHandlers(self)
            setup_handlers(self._handlers)
            self._dispatcher.include_router(router)

            self.logger.info("Telegram bot initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Telegram bot: {e}")
            self._bot = None
            self._dispatcher = None

    @property
    def bot(self) -> Bot | None:
        """Get bot instance."""
        return self._bot

    @property
    def dispatcher(self) -> Dispatcher | None:
        """Get dispatcher instance."""
        return self._dispatcher

    @property
    def handlers(self) -> TelegramHandlers | None:
        """Get handlers instance."""
        return self._handlers

    def is_enabled(self) -> bool:
        """Check if Telegram service is enabled."""
        return self._bot is not None

    def register_callback(
        self,
        name: str,
        callback: Callable[..., Awaitable[Any]]
    ) -> None:
        """
        Register an external callback for a specific action.

        Args:
            name: Callback name (e.g., 'positions', 'close_all').
            callback: Async function to call when action is triggered.
        """
        if self._handlers:
            self._handlers.register_callback(name, callback)

    async def start_polling(self) -> None:
        """
        Start polling for updates.

        This should be called to start receiving messages.
        Runs in background until stop_polling is called.
        """
        if not self._bot or not self._dispatcher:
            self.logger.warning("Cannot start polling - bot not initialized")
            return

        self.logger.info("Starting Telegram bot polling...")

        try:
            await self._dispatcher.start_polling(
                self._bot,
                handle_signals=False,  # We handle signals ourselves
            )
        except asyncio.CancelledError:
            self.logger.info("Polling cancelled")
        except Exception as e:
            self.logger.error(f"Polling error: {e}")

    async def start_polling_background(self) -> None:
        """Start polling in a background task."""
        if self._polling_task and not self._polling_task.done():
            self.logger.warning("Polling already running")
            return

        self._polling_task = asyncio.create_task(self.start_polling())
        self.logger.info("Telegram polling started in background")

    async def stop_polling(self) -> None:
        """Stop polling for updates."""
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            self._polling_task = None

            # Only stop dispatcher polling if we actually started it
            if self._dispatcher:
                try:
                    await self._dispatcher.stop_polling()
                except RuntimeError:
                    # Polling was not started, ignore
                    pass

        if self._bot:
            await self._bot.session.close()

        self.logger.info("Telegram bot polling stopped")

    # ==================== Message Sending Methods ====================

    async def send_message(self, message: str) -> bool:
        """
        Send a plain text message to admin.

        Args:
            message: Message text to send.

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self._bot:
            self.logger.warning("Telegram service is not available")
            return False

        try:
            await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message: {e}")
            return False

    async def send_message_markdown(self, message: str) -> bool:
        """
        Send a Markdown formatted message to admin.

        Args:
            message: Message text with Markdown formatting.

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self._bot:
            self.logger.warning("Telegram service is not available")
            return False

        try:
            await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message with Markdown: {e}")
            return False

    async def send_message_html(self, message: str) -> bool:
        """
        Send an HTML formatted message to admin.

        Args:
            message: Message text with HTML formatting.

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self._bot:
            self.logger.warning("Telegram service is not available")
            return False

        try:
            await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message with HTML: {e}")
            return False

    async def send_message_with_inline_keyboard(
        self,
        message: str,
        inline_keyboard: InlineKeyboardMarkup,
    ) -> int | None:
        """
        Send a message with inline keyboard.

        Args:
            message: Message text.
            inline_keyboard: InlineKeyboardMarkup with buttons.

        Returns:
            Message ID if sent successfully, None otherwise.
        """
        if not self._bot:
            self.logger.warning("Telegram service is not available")
            return None

        try:
            sent_message = await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=inline_keyboard,
            )
            return sent_message.message_id
        except Exception as e:
            self.logger.error(
                f"Failed to send Telegram message with inline keyboard: {e}"
            )
            return None

    async def send_photo(
        self,
        photo: bytes | str,
        caption: str | None = None,
        parse_mode: str = "Markdown",
    ) -> bool:
        """
        Send a photo to admin.

        Args:
            photo: Photo as bytes (buffer) or file path/URL.
            caption: Optional caption for the photo.
            parse_mode: Parse mode for caption ('Markdown' or 'HTML').

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self._bot:
            self.logger.warning("Telegram service is not available")
            return False

        try:
            from aiogram.types import BufferedInputFile, FSInputFile, URLInputFile
            from typing import Union

            input_file: Union[BufferedInputFile, FSInputFile, str]
            if isinstance(photo, bytes):
                input_file = BufferedInputFile(photo, filename="image.png")
            elif isinstance(photo, str) and photo.startswith("http"):
                input_file = photo
            else:
                input_file = FSInputFile(str(photo))

            pm = ParseMode.MARKDOWN if parse_mode == "Markdown" else ParseMode.HTML

            await self._bot.send_photo(
                chat_id=self.admin_chat_id,
                photo=input_file,
                caption=caption,
                parse_mode=pm,
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to send Telegram photo: {e}")
            return False

    async def send_photo_with_inline_keyboard(
        self,
        photo: bytes | str,
        caption: str,
        inline_keyboard: InlineKeyboardMarkup,
    ) -> int | None:
        """
        Send a photo with inline keyboard.

        Args:
            photo: Photo as bytes or file path.
            caption: Caption for the photo.
            inline_keyboard: InlineKeyboardMarkup with buttons.

        Returns:
            Message ID if sent successfully, None otherwise.
        """
        if not self._bot:
            self.logger.warning("Telegram service is not available")
            return None

        try:
            from aiogram.types import BufferedInputFile, FSInputFile
            from typing import Union

            input_file: Union[BufferedInputFile, FSInputFile, str]
            if isinstance(photo, bytes):
                input_file = BufferedInputFile(photo, filename="image.png")
            elif isinstance(photo, str) and photo.startswith("http"):
                input_file = photo
            else:
                input_file = FSInputFile(str(photo))

            sent_message = await self._bot.send_photo(
                chat_id=self.admin_chat_id,
                photo=input_file,
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=inline_keyboard,
            )
            return sent_message.message_id
        except Exception as e:
            self.logger.error(
                f"Failed to send Telegram photo with inline keyboard: {e}"
            )
            return None

    async def delete_message(self, message_id: int) -> bool:
        """
        Delete a message by ID.

        Args:
            message_id: ID of the message to delete.

        Returns:
            True if deleted successfully, False otherwise.
        """
        if not self._bot:
            return False

        try:
            await self._bot.delete_message(
                chat_id=self.admin_chat_id,
                message_id=message_id,
            )
            return True
        except Exception as e:
            self.logger.warning(f"Failed to delete message {message_id}: {e}")
            return False

    # ==================== Auto-delete Message Methods ====================

    async def send_positions_message(
        self,
        message: str,
        inline_keyboard: InlineKeyboardMarkup | None = None,
    ) -> None:
        """
        Send positions message, deleting the previous one if exists.

        Args:
            message: Message text.
            inline_keyboard: Optional inline keyboard.
        """
        if self._last_positions_message_id:
            await self.delete_message(self._last_positions_message_id)

        if inline_keyboard:
            self._last_positions_message_id = await self.send_message_with_inline_keyboard(
                message, inline_keyboard
            )
        else:
            if await self.send_message_markdown(message):
                # We don't have message ID from send_message_markdown
                self._last_positions_message_id = None

    async def send_opportunities_message(self, message: str) -> bool:
        """
        Send opportunities message, deleting the previous one if exists.

        Args:
            message: Message text.

        Returns:
            True if sent successfully.
        """
        if self._last_opportunities_message_id:
            await self.delete_message(self._last_opportunities_message_id)

        if not self._bot:
            return False

        try:
            sent_message = await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
            )
            self._last_opportunities_message_id = sent_message.message_id
            return True
        except Exception as e:
            self.logger.error(f"Failed to send opportunities message: {e}")
            return False

    async def send_balances_message(self, message: str) -> bool:
        """
        Send balances message, deleting the previous one if exists.

        Args:
            message: Message text with Markdown.

        Returns:
            True if sent successfully.
        """
        if self._last_balances_message_id:
            await self.delete_message(self._last_balances_message_id)

        if not self._bot:
            return False

        try:
            sent_message = await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN,
            )
            self._last_balances_message_id = sent_message.message_id
            return True
        except Exception as e:
            self.logger.error(f"Failed to send balances message: {e}")
            return False

    async def send_market_data_message(self, message: str) -> bool:
        """
        Send market data message, deleting the previous one if exists.

        Args:
            message: Message text.

        Returns:
            True if sent successfully.
        """
        if self._last_market_data_message_id:
            await self.delete_message(self._last_market_data_message_id)

        if not self._bot:
            return False

        try:
            sent_message = await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text=message,
            )
            self._last_market_data_message_id = sent_message.message_id
            return True
        except Exception as e:
            self.logger.error(f"Failed to send market data message: {e}")
            return False

    async def show_main_menu(self) -> bool:
        """
        Send main menu message with keyboard.

        Returns:
            True if sent successfully.
        """
        if not self._bot:
            return False

        try:
            await self._bot.send_message(
                chat_id=self.admin_chat_id,
                text="🤖 *Trading Bot*\n\nMenu is ready.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_main_menu_keyboard(),
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to show main menu: {e}")
            return False
