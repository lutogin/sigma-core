"""
Communicator Service.

Listens to trade lifecycle events and sends notifications via Telegram.
Also provides formatted output for screener results on demand.
"""

from typing import TYPE_CHECKING, Any, Dict, Optional
import numpy as np

from src.infra.event_emitter.events import (
    EventType,
    ExitReason,
    SpreadSide,
    PendingEntrySignalEvent,
    EntrySignalEvent,
    ExitSignalEvent,
    WatchCancelledEvent,
    TradeOpenedEvent,
    TradeClosedEvent,
    TradeFailedEvent,
    TradeCloseErrorEvent,
)

if TYPE_CHECKING:
    from src.infra.event_emitter import EventEmitter
    from src.integrations.telegram import TelegramService
    from src.domain.screener import ScreenerService
    from src.integrations.exchange.binance import BinanceClient


class CommunicatorService:
    """
    Communicator listens to trade events and sends notifications.

    Subscribes to:
    - PENDING_ENTRY_SIGNAL → Entry candidate detected, watching for reversal
    - ENTRY_SIGNAL → Entry confirmed after reversal
    - EXIT_SIGNAL → Exit signal detected
    - WATCH_CANCELLED → Watch cancelled without entry
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
        screener_service: "ScreenerService",
        binance_client: "BinanceClient",
        logger: Any,
    ):
        """
        Initialize communicator service.

        Args:
            event_emitter: EventEmitter instance to subscribe to events.
            telegram_service: TelegramService instance for sending messages.
            screener_service: ScreenerService instance for accessing scan results.
            binance_client: BinanceClient instance for accessing positions.
            logger: Logger instance.
        """
        self._event_emitter = event_emitter
        self._telegram = telegram_service
        self._screener = screener_service
        self._binance = binance_client
        self._logger = logger
        self._started = False

    def start(self) -> None:
        """Start listening to events."""
        if self._started:
            self._logger.warning("CommunicatorService already started")
            return

        # Subscribe to trade lifecycle events
        self._event_emitter.on(EventType.PENDING_ENTRY_SIGNAL, self._on_pending_entry_signal)
        self._event_emitter.on(EventType.ENTRY_SIGNAL, self._on_entry_signal)
        self._event_emitter.on(EventType.EXIT_SIGNAL, self._on_exit_signal)
        self._event_emitter.on(EventType.WATCH_CANCELLED, self._on_watch_cancelled)
        self._event_emitter.on(EventType.TRADE_OPENED, self._on_trade_opened)
        self._event_emitter.on(EventType.TRADE_CLOSED, self._on_trade_closed)
        self._event_emitter.on(EventType.TRADE_FAILED, self._on_trade_failed)
        self._event_emitter.on(EventType.TRADE_CLOSE_ERROR, self._on_trade_close_error)

        self._started = True
        self._logger.info("📡 CommunicatorService started - listening to all trade events")

    def stop(self) -> None:
        """Stop listening to events."""
        if not self._started:
            return

        # Unsubscribe from trade lifecycle events
        self._event_emitter.off(EventType.PENDING_ENTRY_SIGNAL, self._on_pending_entry_signal)
        self._event_emitter.off(EventType.ENTRY_SIGNAL, self._on_entry_signal)
        self._event_emitter.off(EventType.EXIT_SIGNAL, self._on_exit_signal)
        self._event_emitter.off(EventType.WATCH_CANCELLED, self._on_watch_cancelled)
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
    # Public Methods - On-demand notifications
    # =========================================================================

    async def send_opportunities(self) -> None:
        """
        Send last scan opportunities to Telegram.

        Formats and sends the last scan results from ScreenerService.
        If no scan data available, sends appropriate message.
        """
        try:
            state = self._screener.last_scan_state

            if state.is_empty():
                await self._telegram.send_message_markdown(
                    "❌ *No scan data available*\n\n"
                    "_Run a scan first to see opportunities._"
                )
                return

            # Format the results
            message = self._format_opportunities(state)
            await self._telegram.send_message_markdown(message)
            self._logger.debug("Sent opportunities to Telegram")

        except Exception as e:
            self._logger.error(f"Failed to send opportunities: {e}")
            await self._telegram.send_message_markdown(
                f"❌ *Error fetching opportunities*\n\n`{str(e)}`"
            )

    async def send_positions(self) -> None:
        """
        Send open positions to Telegram.

        Fetches and formats all open positions from Binance.
        """
        try:
            positions = await self._binance.get_positions(skip_zero=True)

            if not positions:
                await self._telegram.send_message_markdown(
                    "✅ *No open positions*\n\n"
                    "_All positions are closed._"
                )
                return

            # Format the results
            message = self._format_positions(positions)
            await self._telegram.send_message_markdown(message)
            self._logger.debug("Sent positions to Telegram")

        except Exception as e:
            self._logger.error(f"Failed to send positions: {e}")
            await self._telegram.send_message_markdown(
                f"❌ *Error fetching positions*\n\n`{str(e)}`"
            )

    async def send_balance(self) -> None:
        """
        Send futures account balance to Telegram.

        Fetches and formats account balance from Binance.
        """
        try:
            # Get USDT balance (main futures trading asset)
            usdt_balance = await self._binance.get_balance("USDT")

            # Get all balances to show other assets if any
            all_balances = await self._binance.get_balances()

            # Filter non-zero balances (excluding USDT which we show separately)
            other_balances = [
                b for b in all_balances
                if b.asset != "USDT" and (b.free > 0 or b.used > 0)
            ]

            # Format the results
            message = self._format_balance(usdt_balance, other_balances)
            await self._telegram.send_message_markdown(message)
            self._logger.debug("Sent balance to Telegram")

        except Exception as e:
            self._logger.error(f"Failed to send balance: {e}")
            await self._telegram.send_message_markdown(
                f"❌ *Error fetching balance*\n\n`{str(e)}`"
            )

    async def close_all_positions(self) -> None:
        """
        Close all open positions and send results to Telegram.

        Uses BinanceClient.close_all_positions() to close all positions
        and formats the results for Telegram display.
        """
        try:
            # Send initial message
            await self._telegram.send_message_markdown(
                "⚠️ *Closing all positions...*\n\n_Please wait..._"
            )

            # Close all positions
            result = await self._binance.close_all_positions()

            # Format the results
            message = self._format_close_all_result(result)
            await self._telegram.send_message_markdown(message)
            self._logger.info(
                f"Close all positions completed: "
                f"{result['closed_successfully']}/{result['total_positions']} closed"
            )

        except Exception as e:
            self._logger.error(f"Failed to close all positions: {e}")
            await self._telegram.send_message_markdown(
                f"❌ *Error closing positions*\n\n`{str(e)}`"
            )

    def _format_opportunities(self, state) -> str:
        """
        Format scan results for Telegram display.

        Args:
            state: LastScanState with scan results.

        Returns:
            Formatted markdown string.
        """
        result = state.scan_result
        age_min = state.get_age_seconds() / 60
        hurst_values = state.hurst_values or {}
        dynamic_thresholds = state.dynamic_thresholds or {}

        # Get static thresholds from screener (used as floor)
        z_entry_floor = self._screener.z_score_service.z_entry_threshold
        z_sl = self._screener.z_score_service.z_sl_threshold

        # Check market safety
        market_status = "🟢 Safe" if (
            result.volatility_result and result.volatility_result.is_safe
        ) else "🔴 Unsafe"

        # Header
        lines = [
            f"📊 *Scan Results* (age: {age_min:.1f} min)",
            f"Market: {market_status}",
            f"Scanned: {result.symbols_scanned} → Filtered: {len(result.filtered_results)}",
            "",
        ]

        if not result.filtered_results:
            lines.append("_No opportunities found_")
            return "\n".join(lines)

        # Build table data
        data = []
        for symbol, res in result.filtered_results.items():
            z = res.current_z_score
            hurst = hurst_values.get(symbol)
            # Use dynamic threshold for this symbol, fallback to static floor
            dyn_threshold = dynamic_thresholds.get(symbol, z_entry_floor)

            # Determine signal using dynamic threshold
            if np.isnan(z):
                signal = "—"
            elif z >= dyn_threshold and z <= z_sl:
                signal = "🔴 SHORT"
            elif z <= -dyn_threshold and z >= -z_sl:
                signal = "🟢 LONG"
            elif abs(z) >= 1.5:
                signal = "⚠️ WATCH"
            else:
                signal = "—"

            data.append({
                "symbol": symbol.replace("/USDT:USDT", ""),
                "z": z,
                "corr": res.current_correlation,
                "beta": res.current_beta,
                "hurst": hurst,
                "dyn_th": dyn_threshold,
                "signal": signal,
            })

        # Sort by absolute z-score
        data.sort(key=lambda x: abs(x["z"]) if not np.isnan(x["z"]) else 0, reverse=True)

        # Format as compact table (Telegram has message limits)
        lines.append("```")
        lines.append(f"{'Sym':<6} {'Z':>5} {'Th':>4} {'H':>4} {'Signal':<10}")
        lines.append("-" * 32)

        for row in data[:15]:  # Limit to 15 rows for space
            z_str = f"{row['z']:.2f}" if not np.isnan(row['z']) else "N/A"
            th_str = f"{row['dyn_th']:.1f}"
            h_str = f"{row['hurst']:.2f}" if row['hurst'] is not None else "—"

            lines.append(
                f"{row['symbol']:<6} {z_str:>5} {th_str:>4} {h_str:>4} {row['signal']:<10}"
            )

        lines.append("```")

        if len(data) > 15:
            lines.append(f"_...and {len(data) - 15} more_")

        return "\n".join(lines)

    def _format_positions(self, positions) -> str:
        """
        Format positions for Telegram display.

        Args:
            positions: List of Position objects from BinanceClient.

        Returns:
            Formatted markdown string.
        """
        # Header
        lines = [
            f"📈 *Open Positions* ({len(positions)})",
            "",
        ]

        # Calculate totals
        total_unrealized = sum(pos.unrealized_pnl for pos in positions)
        total_margin = sum(abs(pos.contracts * pos.mark_price / pos.leverage) for pos in positions)

        lines.extend([
            f"💰 Total PnL: `{total_unrealized:+.2f}` USDT",
            f"💳 Total Margin: `{total_margin:.2f}` USDT",
            "",
        ])

        # Sort by absolute PnL (biggest losses/gains first)
        positions_sorted = sorted(positions, key=lambda p: abs(p.unrealized_pnl), reverse=True)

        # Format as compact table
        lines.append("```")
        lines.append(f"{'Sym':<6} {'Side':<4} {'Size':<8} {'PnL':<8}")
        lines.append("-" * 28)

        for pos in positions_sorted[:20]:  # Limit to 20 positions
            symbol_short = pos.symbol.replace("USDT", "")
            side_short = pos.side.upper()[:4]
            size_str = f"{pos.contracts:.2f}" if pos.contracts < 1000 else f"{pos.contracts:.0f}"
            pnl_str = f"{pos.unrealized_pnl:+.2f}"

            lines.append(
                f"{symbol_short:<6} {side_short:<4} {size_str:<8} {pnl_str:<8}"
            )

            # Add details on next line
            entry_price = f"Entry: {pos.entry_price:.4f}"
            mark_price = f"Mark: {pos.mark_price:.4f}"
            leverage = f"Lev: {pos.leverage}x"
            lines.append(f"--- {entry_price} | {mark_price} | {leverage}")

        lines.append("```")

        if len(positions) > 20:
            lines.append(f"_...and {len(positions) - 20} more_")

        return "\n".join(lines)

    def _format_balance(self, usdt_balance, other_balances) -> str:
        """
        Format account balance for Telegram display.

        Args:
            usdt_balance: Balance object for USDT.
            other_balances: List of Balance objects for other assets.

        Returns:
            Formatted markdown string.
        """
        # Header
        lines = [
            "💰 *Futures Account Balance*",
            "",
        ]

        # USDT (main trading asset)
        lines.extend([
            "🏦 *USDT Balance:*",
            f"• Available: `{usdt_balance.free:.2f}` USDT",
            f"• In Orders: `{usdt_balance.used:.2f}` USDT",
            f"• **Total: `{usdt_balance.total:.2f}` USDT**",
            "",
        ])

        # Other assets (if any)
        if other_balances:
            lines.extend([
                "🪙 *Other Assets:*",
            ])

            for balance in other_balances[:10]:  # Limit to 10 assets
                if balance.total > 0:
                    lines.append(
                        f"• {balance.asset}: `{balance.total:.6f}` "
                        f"(Free: `{balance.free:.6f}`, Used: `{balance.used:.6f}`)"
                    )

            if len(other_balances) > 10:
                lines.append(f"_...and {len(other_balances) - 10} more assets_")

            lines.append("")

        # Account summary
        total_value_usdt = usdt_balance.total
        if other_balances:
            lines.extend([
                "📊 *Summary:*",
                f"• Primary Asset (USDT): `{usdt_balance.total:.2f}`",
                f"• Other Assets: `{len(other_balances)}` types",
                f"• **Account Value: ~`{total_value_usdt:.2f}` USDT**",
                "",
                "_Note: Other assets not converted to USDT value_"
            ])
        else:
            lines.extend([
                "📊 *Summary:*",
                f"• **Account Value: `{total_value_usdt:.2f}` USDT**",
                "",
                "_All funds in USDT - ready for trading_"
            ])

        return "\n".join(lines)

    def _format_close_all_result(self, result) -> str:
        """
        Format close all positions result for Telegram display.

        Args:
            result: Dictionary with close all results from BinanceClient.

        Returns:
            Formatted markdown string.
        """
        total = result["total_positions"]
        success = result["closed_successfully"]
        failed = result["failed"]
        results = result["results"]

        # Header with summary
        if total == 0:
            return "✅ *No positions to close*\n\n_All positions were already closed._"

        if failed == 0:
            header_emoji = "✅"
            header_text = "ALL POSITIONS CLOSED"
        elif success == 0:
            header_emoji = "❌"
            header_text = "FAILED TO CLOSE POSITIONS"
        else:
            header_emoji = "⚠️"
            header_text = "PARTIALLY CLOSED"

        lines = [
            f"{header_emoji} *{header_text}*",
            "",
            f"📊 *Summary:*",
            f"• Total positions: `{total}`",
            f"• Successfully closed: `{success}`",
            f"• Failed: `{failed}`",
            "",
        ]

        if results:
            # Group by status
            successful = [r for r in results if r["status"] == "success"]
            failed_results = [r for r in results if r["status"] == "failed"]

            # Show successful closes
            if successful:
                lines.extend([
                    "✅ *Successfully Closed:*",
                ])
                for result_item in successful[:10]:  # Limit to 10 for space
                    symbol_short = result_item["symbol"].replace("/USDT:USDT", "")
                    side_short = result_item["side"].upper()
                    contracts = result_item["contracts"]
                    order_id = result_item["order_id"]

                    lines.append(
                        f"• {symbol_short} {side_short}: `{contracts:.2f}` (#{order_id})"
                    )

                if len(successful) > 10:
                    lines.append(f"_...and {len(successful) - 10} more_")
                lines.append("")

            # Show failed closes
            if failed_results:
                lines.extend([
                    "❌ *Failed to Close:*",
                ])
                for result_item in failed_results[:5]:  # Limit to 5 for space
                    symbol_short = result_item["symbol"].replace("/USDT:USDT", "")
                    side_short = result_item["side"].upper()
                    contracts = result_item["contracts"]
                    error = result_item["error"]

                    lines.append(f"• {symbol_short} {side_short}: `{contracts:.2f}`")
                    lines.append(f"  _Error: {error}_")

                if len(failed_results) > 5:
                    lines.append(f"_...and {len(failed_results) - 5} more errors_")
                lines.append("")

        # Footer
        if failed > 0:
            lines.extend([
                "⚠️ *Manual intervention may be required for failed positions!*",
                "",
                "_Check your exchange account to verify position status._"
            ])
        else:
            lines.append("🎉 _All positions successfully closed!_")

        return "\n".join(lines)

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_pending_entry_signal(self, event: PendingEntrySignalEvent) -> None:
        """Handle pending entry signal event - entry candidate detected."""
        try:
            side_emoji = "📈" if event.spread_side == SpreadSide.LONG else "📉"
            side_text = "LONG" if event.spread_side == SpreadSide.LONG else "SHORT"

            # Describe what we're watching for
            if event.spread_side == SpreadSide.LONG:
                explanation = "Spread low → watching for reversal up"
                target_action = "BUY coin, SELL primary"
            else:
                explanation = "Spread high → watching for reversal down"
                target_action = "SELL coin, BUY primary"

            message = f"""👀 *WATCHING FOR ENTRY: {side_text}*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`

🎯 *Strategy:*
{explanation}
*If reversal confirmed:* {target_action}

📊 *Signal Data:*
• Z-Score: `{event.z_score:.4f}` (crossed `{event.z_entry_threshold:.2f}`)
• Beta: `{event.beta:.4f}`
• Correlation: `{event.correlation:.4f}`
• Hurst: `{event.hurst:.4f}`

💰 *Current Prices:*
• COIN: `{event.coin_price:.4f}`
• PRIMARY: `{event.primary_price:.4f}`

🎯 *Thresholds:*
• Entry: |Z| ≥ `{event.z_entry_threshold:.2f}`
• TP: |Z| ≤ `{event.z_tp_threshold:.2f}`
• SL: |Z| ≥ `{event.z_sl_threshold:.2f}`

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`

_Monitoring for pullback confirmation..._
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent pending entry notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send pending entry notification: {e}")

    async def _on_entry_signal(self, event: EntrySignalEvent) -> None:
        """Handle entry signal event - entry confirmed after reversal."""
        try:
            side_emoji = "📈" if event.spread_side == SpreadSide.LONG else "📉"
            side_text = "LONG" if event.spread_side == SpreadSide.LONG else "SHORT"

            # Describe the trade direction
            if event.spread_side == SpreadSide.LONG:
                action_text = (
                    f"🟢 *BUY:* `{event.coin_symbol}`\n"
                    f"🔴 *SELL:* `{event.primary_symbol}`"
                )
                explanation = "Reversal confirmed → entering mean reversion trade"
            else:
                action_text = (
                    f"🔴 *SELL:* `{event.coin_symbol}`\n"
                    f"🟢 *BUY:* `{event.primary_symbol}`"
                )
                explanation = "Reversal confirmed → entering mean reversion trade"

            message = f"""{side_emoji} *ENTRY SIGNAL CONFIRMED: {side_text}*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`

✅ *Actions:*
{action_text}
_{explanation}_

📊 *Signal Data:*
• Z-Score: `{event.z_score:.4f}`
• Beta: `{event.beta:.4f}`
• Correlation: `{event.correlation:.4f}`
• Hurst: `{event.hurst:.4f}`

💰 *Current Prices:*
• COIN: `{event.coin_price:.4f}`
• PRIMARY: `{event.primary_price:.4f}`

🎯 *Thresholds:*
• TP: |Z| ≤ `{event.z_tp_threshold:.2f}`
• SL: |Z| ≥ `{event.z_sl_threshold:.2f}`

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`

_Sending to TradingService for execution..._
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent entry signal notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send entry signal notification: {e}")

    async def _on_exit_signal(self, event: ExitSignalEvent) -> None:
        """Handle exit signal event."""
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

            message = f"""{reason_emoji} *EXIT SIGNAL: {reason_text}*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`

📊 *Current Metrics:*
• Z-Score: `{event.current_z_score:.4f}`
• Correlation: `{event.current_correlation:.4f}`

💰 *Current Prices:*
• COIN: `{event.coin_price:.4f}`
• PRIMARY: `{event.primary_price:.4f}`

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`

_Sending to TradingService for position closure..._
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent exit signal notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send exit signal notification: {e}")

    async def _on_watch_cancelled(self, event: WatchCancelledEvent) -> None:
        """Handle watch cancelled event."""
        try:
            # Determine emoji and reason text
            reason_map = {
                "timeout": ("⏰", "TIMEOUT"),
                "false_alarm": ("❌", "FALSE ALARM"),
                "sl_hit": ("🛑", "SL HIT"),
                "max_watches_reached": ("🚫", "MAX WATCHES"),
            }
            reason_emoji, reason_text = reason_map.get(
                event.reason.value, ("❓", event.reason.value.upper())
            )

            duration_min = event.watch_duration_seconds / 60

            message = f"""{reason_emoji} *WATCH CANCELLED: {reason_text}*

*Pair:* `{event.coin_symbol}` / `{event.primary_symbol}`

📊 *Stats:*
• Max Z reached: `{event.max_z_reached:.4f}`
• Final Z: `{event.final_z:.4f}`
• Watch duration: `{duration_min:.1f}` minutes

⏱️ *Time:* `{event.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC`

_No entry executed - continuing to monitor other pairs._
"""
            await self._telegram.send_message_markdown(message)
            self._logger.debug(
                f"Sent watch cancelled notification for {event.coin_symbol}"
            )

        except Exception as e:
            self._logger.error(f"Failed to send watch cancelled notification: {e}")

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
