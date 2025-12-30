# Trailing Entry System - Документация

## 🎯 Обзор

Реализована система **Trailing Entry** для стат-арбитражного бота, которая решает проблему "прострелов" при входе в сделки.

### Проблема

Бот входил сразу при Z-Score >= threshold, но часто попадал в "прострел" (2.1→2.5→3.0→3.5) и шел в минус.

### Решение

**Trailing Entry** - входить только при развороте от максимума Z-Score.

Пример:

- Свеча 1: Z = 2.2 (Пробили порог, готовимся)
- Свеча 2: Z = 2.8 (Ждем)
- Свеча 3: Z = 2.6 (ВХОД! Пик пройден, пошел возврат на 0.2 пунктов)

## 🔄 Алгоритм

```
def process_trailing_entry(ticker, live_z_score):
    # 1. Если мы еще не следим, но порог пробит
    if ticker not in state and abs(live_z_score) > Z_ENTRY_THRESHOLD:
        state[ticker] = {
            'status': 'WATCHING',
            'max_z': abs(live_z_score),
            'side': 'SHORT_SPREAD' if live_z_score > 0 else 'LONG_SPREAD'
        }
        return

    # 2. Если мы уже следим
    if ticker in state:
        current_abs_z = abs(live_z_score)
        cached = state[ticker]

        # А. Спред продолжает расширяться (Нож падает)
        if current_abs_z > cached['max_z']:
            cached['max_z'] = current_abs_z

        # Б. Спред начал схлопываться (Разворот!)
        elif current_abs_z <= (cached['max_z'] - PULLBACK):
            execute_trade(ticker, cached['side'])
            del state[ticker]

        # В. Z-Score вернулся к нулю без входа (Ложная тревога)
        elif current_abs_z < Z_ENTRY_THRESHOLD:
             del state[ticker]

        # Г. Таймаут - 60 минут прошло без входа
        elif watch_started > 60 minutes ago:
             del state[ticker]
```

## ⚙️ Конфигурация

### Переменные окружения (.env)

```bash
# Trailing Entry параметры
TRAILING_ENTRY_PULLBACK=0.2              # Откат от максимума Z-score для входа (0.2-0.5)
TRAILING_ENTRY_TIMEOUT_MINUTES=210        # Таймаут мониторинга (3 часа по 15 мин свечам)
```

### Settings.py

```python
# Trailing Entry settings
TRAILING_ENTRY_PULLBACK: float = 0.2       # Откат от максимума
TRAILING_ENTRY_TIMEOUT_MINUTES: int = 60   # Таймаут мониторинга
```

## 🏗️ Архитектура

### Новые компоненты

#### 1. События (src/infra/event_emitter/events.py)

**PendingEntrySignalEvent** - сигнал для начала мониторинга:

```python
@dataclass
class PendingEntrySignalEvent(BaseEvent):
    coin_symbol: str          # "LINK/USDT:USDT"
    primary_symbol: str       # "ETH/USDT:USDT"
    spread_side: SpreadSide   # LONG или SHORT
    z_score: float            # Текущий Z-score
    beta: float               # Коэффициент хеджирования
    spread_mean: float        # Среднее спреда (для расчёта live Z)
    spread_std: float         # Стд. отклонение спреда (для расчёта live Z)
    ...
```

**WatchCancelledEvent** - отмена мониторинга:

```python
class WatchCancelReason(str, Enum):
    TIMEOUT = "timeout"                    # Таймаут 60 мин
    RETURNED_TO_NORMAL = "returned_to_normal"  # Z вернулся < entry_threshold
    STOP_LOSS = "stop_loss"               # Z превысил SL порог
    POSITION_OPENED = "position_opened"    # Позиция уже открыта
```

#### 2. EntryObserverService (src/domain/entry_observer/)

Основной сервис для trailing entry:

```python
class EntryObserverService:
    """
    Сервис мониторинга trailing entry.

    - Слушает PendingEntrySignalEvent
    - Подписывается на WebSocket для live цен
    - Отслеживает максимум Z-score
    - Входит при откате на PULLBACK от максимума
    - Отменяет при таймауте или возврате к норме
    """
```

#### 3. WatchCandidate (src/domain/entry_observer/models.py)

Модель состояния мониторинга:

```python
@dataclass
class WatchCandidate:
    coin_symbol: str
    primary_symbol: str
    spread_side: SpreadSide
    max_z: float
    spread_mean: float
    spread_std: float
    beta: float
    coin_price: float
    primary_price: float
    started_at: datetime
    z_entry_threshold: float
    z_tp_threshold: float
    z_sl_threshold: float
    correlation: float
    hurst: float

    @property
    def current_z_score(self) -> float:
        """Рассчитывает Z-score в реальном времени."""
        current_spread = math.log(self.coin_price) - self.beta * math.log(self.primary_price)
        return (current_spread - self.spread_mean) / self.spread_std
```

### Измененные компоненты

#### OrchestratorService

Теперь публикует `PendingEntrySignalEvent` вместо `EntrySignalEvent`:

```python
# Раньше:
self._event_emitter.emit(EventType.ENTRY_SIGNAL, entry_event)

# Теперь:
self._event_emitter.emit(EventType.PENDING_ENTRY_SIGNAL, pending_event)
```

#### PlannerService

Запускает и останавливает EntryObserverService:

```python
async def run(self):
    if self._entry_observer:
        await self._entry_observer.start()
    ...

async def stop(self):
    if self._entry_observer:
        await self._entry_observer.stop()
```

#### Container

Инициализирует EntryObserverService с зависимостями:

```python
@property
def entry_observer_service(self):
    return EntryObserverService(
        event_emitter=self.event_emitter,
        exchange_client=self.exchange_client,
        redis_cache=self.redis_cache,
        logger=self.logger,
        primary_symbol=self._settings.PRIMARY_PAIR,
        z_entry_threshold=self._settings.Z_ENTRY_THRESHOLD,
        z_sl_threshold=self._settings.Z_SL_THRESHOLD,
        pullback=self._settings.TRAILING_ENTRY_PULLBACK,
        watch_timeout_seconds=self._settings.TRAILING_ENTRY_TIMEOUT_MINUTES * 60,
    )
```

## 📊 Поток событий

```
Orchestrator → [Z_ENTRY_THRESHOLD <= |Z| <= Z_SL_THRESHOLD] → PendingEntrySignalEvent
                                                                        ↓
EntryObserver → [WebSocket мониторинг] → [отслеживание max_z] → [откат >= PULLBACK]
                                                                        ↓
                                                              EntrySignalEvent
                                                                        ↓
                                                             TradingService → сделка
```

### Варианты завершения мониторинга

1. **✅ Успешный вход**: Z откатился на PULLBACK от max → EntrySignalEvent
2. **❌ Таймаут**: 45 минут прошло без входа → WatchCancelledEvent (TIMEOUT)
3. **❌ Ложная тревога**: Z вернулся < entry_threshold → WatchCancelledEvent (RETURNED_TO_NORMAL)
4. **❌ Stop Loss**: Z превысил SL порог → WatchCancelledEvent (STOP_LOSS)

## 🔧 Техническая реализация

### WebSocket подключение

```python
async def _subscribe_to_prices(self, coin_symbol: str) -> None:
    """Подписка на book ticker для coin и primary."""
    # Переиспользуем подключение для ETH (всегда в связке)
    await self._exchange_client.subscribe_book_ticker(
        symbols=[coin_symbol, self._primary_symbol],
        callback=self._on_price_update
    )
```

### Debounce (1 сек)

```python
def _on_price_update(self, symbol: str, bid: float, ask: float):
    """Обработка обновления цены с debounce."""
    now = time.monotonic()
    if now - self._last_process_time < 1.0:  # debounce 1 sec
        return
    self._last_process_time = now
    asyncio.create_task(self._process_price_update(symbol, bid, ask))
```

### Live Z-Score расчёт

```python
def calculate_live_z(candidate: WatchCandidate) -> float:
    """
    Z = (spread - mean) / std
    где spread = log(coin_price) - beta * log(primary_price)
    """
    spread = math.log(candidate.coin_price) - candidate.beta * math.log(candidate.primary_price)
    return (spread - candidate.spread_mean) / candidate.spread_std
```

### Redis persistence

```python
REDIS_KEY_PREFIX = "entry_observer:watches:"

async def _save_to_redis(self, candidate: WatchCandidate):
    key = f"{REDIS_KEY_PREFIX}{candidate.coin_symbol}"
    await self._redis.set(key, candidate.to_json(), ex=3600)

async def _load_from_redis(self) -> Dict[str, WatchCandidate]:
    keys = await self._redis.keys(f"{REDIS_KEY_PREFIX}*")
    ...
```

## 🧪 Тестирование

### Логи

```
👀 LINK/USDT:USDT broke threshold 2.1. Starting watch. Current Z: 2.25
📈 LINK/USDT:USDT new peak Z: 2.80. Updating target.
📈 LINK/USDT:USDT new peak Z: 3.10. Updating target.
✅ LINK/USDT:USDT reversal confirmed! Peak: 3.10, Current: 2.75
📡 Emitted ENTRY_SIGNAL | LINK/USDT:USDT | entry_z=2.75 | max_z=3.10
```

### Сценарии

1. **Успешный вход после разворота**:

   - Z = 2.2 → 2.8 → 3.1 → 2.8 (pullback 0.3) → ENTRY

2. **Ложная тревога**:

   - Z = 2.2 → 2.5 → 2.0 (< entry_threshold) → CANCEL

3. **Таймаут**:
   - Z = 2.2 → 2.8 → 2.7 → 2.6 → ... (45 min) → CANCEL

## 🚀 Преимущества

1. **Снижение убытков**: Вход только при подтверждении разворота
2. **Лучшие цены**: Откат от максимума дает более выгодные entry точки
3. **Улучшение Sharpe Ratio**: Один из лучших способов для Mean Reversion стратегий
4. **Гибкость**: Настраиваемые параметры pullback и timeout
5. **Надежность**: Таймаут защищает от дрейфа рынка
6. **Масштабируемость**: Redis для состояния, поддержка множественных символов
7. **Неблокирующий**: Асинхронная работа не блокирует event loop

## 📈 Ожидаемые результаты

- Снижение количества убыточных сделок от "прострелов"
- Улучшение общего P&L за счет лучших entry точек
- Более стабильная работа бота в волатильных рыночных условиях
- Защита от дрейфа (таймаут 45 минут = 3 свечи)

## 📁 Структура файлов

```
src/
├── domain/
│   └── entry_observer/
│       ├── __init__.py          # Экспорты
│       ├── models.py            # WatchCandidate dataclass
│       └── entry_observer.py    # EntryObserverService
├── infra/
│   ├── event_emitter/
│   │   └── events.py            # PendingEntrySignalEvent, WatchCancelledEvent
│   └── container.py             # DI для entry_observer_service
└── config/
    └── settings.py              # TRAILING_ENTRY_PULLBACK, TRAILING_ENTRY_TIMEOUT_MINUTES
```
