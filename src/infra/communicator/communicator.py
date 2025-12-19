"""
Communicator Service.

Listens to trade lifecycle events and sends notifications via Telegram.
"""

from typing import TYPE_CHECKING, Any

from src.infra.event_emitter.events import (
    EventType,
    ExitReason,
    SpreadSide,
    TradeOpenedEvent,
    TradeClosedEvent,
    TradeFailedEvent,
    TradeCloseErrorEvent,
)

if TYPE_CHECKING:
    from src.infra.event_emitter import EventEmitter
    from src.integrations.telegram import TelegramService


class CommunicatorService:
    """
    Communicator listens to trade events and sends notifications.

    Subscribes to:
    - TRADE_OPENED → Spread trade successfully opened
    - TRADE_CLOSED → Spread trade fully closed
    - TRADE_FAILED → Failed to open spread
    - TRADE_CLOSE_ERROR → Error closing spread

    Usage:
        communicator = CommunicatorService(
            event_emitter=event_emitter,
            telegram_service=telegram_service,
            logger=logger,
        )
        communicator.start()
    """

    def __init__(
        self,
        event_emitter: "EventEmitter",
        telegram_service: "TelegramService",
        logger: Any,
    ):
        """
        Initialize communicator service.

        Args:
            event_emitter: EventEmitter instance to subscribe to events.
            telegram_service: TelegramService instance for sending messages.
            logger: Logger instance.
        """
        self._event_emitter = event_emitter
        self._telegram = telegram_service
        self._logger = logger
        self._started = False

    def start(self) -> None:
        """Start listening to events."""
        if self._started:
            self._logger.warning("CommunicatorService already started")
            return

        # Subscribe to trade lifecycle events
        self._event_emitter.on(EventType.TRADE_OPENED, self._on_trade_opened)
        self._event_emitter.on(EventType.TRADE_CLOSED, self._on_trade_closed)
        self._event_emitter.on(EventType.TRADE_FAILED, self._on_trade_failed)
        self._event_emitter.on(EventType.TRADE_CLOSE_ERROR, self._on_trade_close_error)

        self._started = True
        self._logger.info("📡 CommunicatorService started - listening to trade events")

    def stop(self) -> None:
        """Stop listening to events."""
        if not self._started:
            return

        # Unsubscribe from trade lifecycle events
        self._event_emitter.off(EventType.TRADE_OPENED, self._on_trade_opened)
        self._event_emitter.off(EventType.TRADE_CLOSED, self._on_trade_closed)
        self._event_emitter.off(EventType.TRADE_FAILED, self._on_trade_failed)
        self._event_emitter.off(EventType.TRADE_CLOSE_ERROR, self._on_trade_close_error)

        self._started = False
        self._logger.info("📡 CommunicatorService stopped")

    @property
    def is_started(self) -> bool:
        """Check if communicator is started."""
        return self._started

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_trade_opened(self, event: TradeOpenedEvent) -> None:
        """Handle trade opened event."""
        try:
            side_emoji = "📈" if event.spread_side == SpreadSide.LONG else "📉"
            side_text = "LONG" if event.spread_side == SpreadSide.LONG else "SHORT"

            # Describe the trade direction
            if event.spread_side == SpreadSide.LONG:
                action_text = (
                    f"🟢 *BUY:* `{event.coin_symbol}`\n"
                    f"🔴 *SELL:* `{event.primary_symbol}`"
                )
                explanation = "Spread low → expecting mean reversion up"
            else:
                action_text = (
                    f"🔴 *SELL:* `{event.coin_symbol}`\n"
                    f"🟢 *BUY:* `{event.primary_symbol}`"
                )
                explanation = "Spread high → expecting mean reversion down"

            message = f"""{side_emoji} *TRADE OPENED: {side_text}*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`

🎯 *Actions:*
{action_text}
_{explanation}_

📊 *Signal Data:*
• Z-Score: `{event.z_score:.4f}`
• Beta: `{event.beta:.4f}`
• Correlation: `{event.correlation:.4f}`
• Hurst: `{event.hurst:.4f}`

💰 *Position Sizes:*
• COIN: `{event.coin_size_usdt:.2f}` USDT @ `{event.coin_price:.4f}`
• PRIMARY: `{event.primary_size_usdt:.2f}` USDT @ `{event.primary_price:.4f}`

🎯 *Thresholds:*
• TP: |Z| ≤ `{event.z_tp_threshold:.2f}`
• SL: |Z| ≥ `{event.z_sl_threshold:.2f}`

📋 *Orders:*
• COIN: `{event.coin_order_id}`
• PRIMARY: `{event.primary_order_id}`

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent trade opened notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send trade opened notification: {e}")

    async def _on_trade_closed(self, event: TradeClosedEvent) -> None:
        """Handle trade closed event."""
        try:
            # Determine emoji and reason text
            reason_map = {
                ExitReason.TAKE_PROFIT: ("✅", "TAKE PROFIT"),
                ExitReason.STOP_LOSS: ("🛑", "STOP LOSS"),
                ExitReason.CORRELATION_DROP: ("📉", "CORRELATION DROP"),
                ExitReason.TIMEOUT: ("⏰", "TIMEOUT"),
                ExitReason.HURST_TRENDING: ("📈", "HURST TRENDING"),
                ExitReason.MANUAL: ("🔧", "MANUAL"),
            }
            reason_emoji, reason_text = reason_map.get(
                event.exit_reason, ("❓", event.exit_reason.value.upper())
            )

            side_text = "LONG" if event.spread_side == SpreadSide.LONG else "SHORT"

            # Calculate Z-score change
            z_change = event.exit_z_score - event.entry_z_score
            z_change_emoji = "📈" if z_change > 0 else "📉"

            # Describe closing action
            if event.spread_side == SpreadSide.LONG:
                action_text = (
                    f"🔴 *SELL:* `{event.coin_symbol}`\n"
                    f"🟢 *BUY:* `{event.primary_symbol}`"
                )
            else:
                action_text = (
                    f"🟢 *BUY:* `{event.coin_symbol}`\n"
                    f"🔴 *SELL:* `{event.primary_symbol}`"
                )

            message = f"""{reason_emoji} *TRADE CLOSED: {reason_text}*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`
*Position:* {side_text}

🎯 *Actions:*
{action_text}

📊 *Z-Score:*
• Entry: `{event.entry_z_score:.4f}`
• Exit: `{event.exit_z_score:.4f}`
• Change: {z_change_emoji} `{z_change:+.4f}`

💰 *Prices:*
• COIN: `{event.coin_entry_price:.4f}` → `{event.coin_exit_price:.4f}`
• PRIMARY: `{event.primary_entry_price:.4f}` → `{event.primary_exit_price:.4f}`

📈 *Position Sizes:*
• COIN: `{event.coin_size_usdt:.2f}` USDT
• PRIMARY: `{event.primary_size_usdt:.2f}` USDT

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent trade closed notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send trade closed notification: {e}")

    async def _on_trade_failed(self, event: TradeFailedEvent) -> None:
        """Handle trade failed event."""
        try:
            rollback_text = (
                "✅ Rollback performed" if event.rollback_performed else "⚠️ No rollback"
            )

            message = f"""🚨 *TRADE FAILED*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`

❌ *Error:* `{event.error_message}`
*Failed Leg:* `{event.failed_leg}`
*Rollback:* {rollback_text}

⚠️ _Check exchange positions manually!_

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent trade failed notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send trade failed notification: {e}")

    async def _on_trade_close_error(self, event: TradeCloseErrorEvent) -> None:
        """Handle trade close error event."""
        try:
            status_coin = "✅ closed" if event.coin_closed else "❌ FAILED"
            status_primary = "✅ closed" if event.primary_closed else "❌ FAILED"

            message = f"""🚨 *TRADE CLOSE ERROR*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`
*Exit Reason:* `{event.exit_reason.value}`

❌ *Error:* `{event.error_message}`

📋 *Status:*
• COIN ({event.coin_symbol}): {status_coin}
• PRIMARY ({event.primary_symbol}): {status_primary}

⚠️ _Positions may remain open! Manual intervention required!_

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent trade close error notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send trade close error notification: {e}")
