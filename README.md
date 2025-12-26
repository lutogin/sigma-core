# Statistical Arbitrage Bot — Полное описание архитектуры

## Общая концепция

Бот реализует стратегию **статистического арбитража** на бессрочных фьючерсах Binance. Основная идея — торговля спредом между альткоином (COIN) и ETH как индексом экосистемы. Когда спред отклоняется от исторической нормы (измеряется Z-score), бот открывает дельта-нейтральную позицию в ожидании возврата к среднему.

**Торгуемые пары:** LINK, AAVE, MANA, ONDO, ETHFI, UNI — все против ETH/USDT:USDT как PRIMARY.

---

## 1. Планировщик (PlannerService)

**Запуск:** Cron `*/15 * * * *` — каждые 15 минут (в 00, 15, 30, 45 минуту каждого часа).

**Последовательность:**

1. Проверка таймаутов позиций — если позиция открыта дольше **MAX_POSITION_BARS = 96 баров (~24 часов для 15m)**, она принудительно закрывается с причиной TIMEOUT
2. Запуск OrchestratorService.run()

---

## 2. Оркестратор (OrchestratorService)

Координирует весь цикл сканирования. Последовательность:

### 2.1 Screener Pipeline

#### Загрузка данных

- Загружает OHLCV данные за **LOOKBACK_WINDOW_DAYS × 2 + 1 = 7 дней**
- Источник: кэш MongoDB + Binance API для недостающих свечей
- Таймфрейм: **15m**

#### Volatility Filter (Фильтр волатильности рынка)

- Проверяет волатильность ETH и изменение цены за 4 часа
- Если рынок "небезопасен" — сканирование прерывается, emit `MarketUnsafeEvent`
- Защита от торговли во время резких движений рынка

#### Correlation Service (Расчёт корреляции)

- Для каждого COIN рассчитывает:
  - **Rolling Correlation** с ETH за окно LOOKBACK_WINDOW_DAYS = 3 дня
  - **Rolling Beta (β)** — коэффициент хеджирования через OLS регрессию: `β = Cov(COIN, ETH) / Var(ETH)`
- Фильтрация:
  - `correlation >= MIN_CORRELATION (0.8)` — отсекаем декоррелированные пары
  - `MIN_BETA (0.5) <= β <= MAX_BETA (2.0)` — отсекаем экстремальные беты

#### Z-Score Service (Расчёт Z-Score)

- **Spread** = log(COIN_price) - β × log(ETH_price)
- **Z-Score** = (Spread - Mean) / Std — rolling за LOOKBACK_WINDOW_DAYS

##### Dynamic Entry Threshold (Динамический порог входа)

Вместо статического порога Z_ENTRY_THRESHOLD = 2.1, для каждого символа рассчитывается адаптивный порог:

1. Берём последние **DYNAMIC_THRESHOLD_WINDOW_BARS = 440 баров (~4.5 дня)**
2. Рассчитываем **ADAPTIVE_PERCENTILE = 95-й перцентиль** от |Z|
3. Применяем floor: `max(Z_ENTRY_THRESHOLD, percentile)`
4. Сглаживаем через **EMA с α = THRESHOLD_EMA_ALPHA = 0.1** (10% новое значение, 90% предыдущее)
5. Результат: `dynamic_entry_threshold` для каждого символа

Это предотвращает ложные входы на парах с исторически высокой волатильностью Z-score.

---

### 2.2 Exit Conditions Check (Проверка условий выхода)

Для каждой открытой позиции проверяются условия выхода **в порядке приоритета**:

| Условие              | Порог                           | Описание                      |
| -------------------- | ------------------------------- | ----------------------------- |
| **TAKE_PROFIT**      | \|Z\| <= Z_TP_THRESHOLD (0.25)  | Спред вернулся к среднему     |
| **STOP_LOSS**        | \|Z\| >= Z_SL_THRESHOLD (4.0)   | Спред разошёлся ещё сильнее   |
| **CORRELATION_DROP** | corr < MIN_CORRELATION (0.8)    | Пара декоррелировалась        |
| **HURST_TRENDING**   | H >= HURST_THRESHOLD (0.45)     | Спред стал трендовым          |
| **TIMEOUT**          | bars >= MAX_POSITION_BARS (144) | Позиция слишком долго открыта |

При срабатывании — emit `ExitSignalEvent`.

---

### 2.3 Watched Pairs Check (Проверка наблюдаемых пар)

Если пара находится в EntryObserver (ожидает trailing entry), но:

- Корреляция упала ниже 0.8 → удаляем из наблюдения (CORRELATION_DROP)
- Hurst >= 0.45 → удаляем из наблюдения (HURST_TRENDING)

---

### 2.4 Entry Conditions Check (Проверка условий входа)

Для каждого символа из отфильтрованных результатов:

**Все условия должны выполняться:**

1. `|Z| >= dynamic_entry_threshold` — сигнал достаточно сильный
2. `|Z| < Z_SL_THRESHOLD (4.0)` — но не экстремальный
3. Нет открытой позиции по этому символу
4. Символ не в cooldown
5. `active_positions < MAX_OPEN_SPREADS (3)`

**Hurst Filter (финальный гейт):**

- Рассчитывается Hurst exponent для спреда за **HURST_LOOKBACK_CANDLES = 300 свечей**
- Если `H < HURST_THRESHOLD (0.45)` — спред mean-reverting, вход разрешён
- Если `H >= 0.45` — спред trending, вход запрещён

**Определение стороны:**

- Z < 0 → **LONG spread** (Buy COIN, Sell ETH) — ожидаем рост Z к нулю
- Z > 0 → **SHORT spread** (Sell COIN, Buy ETH) — ожидаем падение Z к нулю

**Funding Filter (фильтр фандинга):**

Проверяет, не будет ли фандинг "съедать" прибыль от сделки. Блокирует вход если чистые расходы на фандинг превышают порог.

- **MAX_FUNDING_COST_THRESHOLD = -0.0005 (-0.05% за 8 часов)**
- Стандартная ставка Binance: 0.01%. Порог = 5× стандартной ставки против нас.

**Расчёт Net Funding Cost (нормализован к 8 часам):**

На Binance разные интервалы фандинга (1h, 2h, 4h, 8h). Все ставки нормализуются к 8-часовому эквиваленту:

```
rate_8h = rate × (8 / funding_interval_hours)
```

Знак имеет значение:

- Rate > 0: Long платит, Short получает
- Rate < 0: Long получает, Short платит

| Сценарий     | Позиции              | Net Cost (%)         |
| ------------ | -------------------- | -------------------- |
| LONG_SPREAD  | Long COIN, Short ETH | ETH_Rate - COIN_Rate |
| SHORT_SPREAD | Short COIN, Long ETH | COIN_Rate - ETH_Rate |

Если `Net Cost < -0.05%` за 8 часов → вход заблокирован (TOXIC_FUNDING).

При прохождении всех фильтров — emit `PendingEntrySignalEvent` с:

- symbol, side, z_score, beta, correlation, hurst
- spread_mean, spread_std (для live расчёта Z)
- dynamic_entry_threshold, z_tp_threshold, z_sl_threshold

---

## 3. Entry Observer (Trailing Entry)

**Цель:** Не входить сразу при пробое порога, а дождаться подтверждения разворота (pullback от пика).

### Параметры:

- **TRAILING_ENTRY_PULLBACK = 0.2** — откат Z-score от максимума для подтверждения
- **TRAILING_ENTRY_TIMEOUT_MINUTES = 60** — максимальное время наблюдения
- **Max watches = 5** — максимум одновременных наблюдений

### Логика:

1. **Получение PendingEntrySignalEvent** → создание WatchCandidate
2. **Подписка на WebSocket** book ticker для COIN и ETH
3. **Real-time мониторинг** (debounce 1 секунда):

```
Loop каждую секунду:
    Рассчитать live Z-score из текущих цен

    IF watch_duration > 60 минут:
        → Cancel: TIMEOUT (применить cooldown)

    ELSE IF |Z| < entry_threshold:
        → Cancel: FALSE_ALARM (Z вернулся к норме)

    ELSE IF |Z| >= Z_SL_THRESHOLD (4.0):
        → Cancel: SL_HIT (Z ушёл слишком далеко)

    ELSE IF |Z| > max_z:
        → Обновить max_z (спред ещё расширяется)

    ELSE IF |Z| <= max_z - PULLBACK (0.2):
        → ✅ REVERSAL CONFIRMED!
        → Emit EntrySignalEvent
```

### Dynamic TP при входе:

При подтверждении разворота рассчитывается:

```
z_tp_threshold = max(Z_TP_THRESHOLD, |current_Z| × 0.1)
```

Это даёт более агрессивный TP для сильных сигналов.

---

## 4. Trading Service (Исполнение сделок)

### Pre-Trade Validation:

1. `allow_trading` flag включён
2. `can_open_position()` — проверка cooldown, overlap, max_spreads
3. Достаточный баланс: `free >= COIN_size + PRIMARY_size × β`

### Position Sizing:

- **COIN leg:** POSITION_SIZE_USDT = 300 USDT
- **PRIMARY leg:** COIN_size × |β| USDT
- Leverage: настраивается (по умолчанию из settings)

### Atomic Execution (ACID):

```
Parallel:
    Open COIN position (LONG spread → BUY, SHORT spread → SELL)
    Open PRIMARY position (LONG spread → SELL, SHORT spread → BUY)

IF both succeeded:
    → Register position in PositionStateService
    → Emit TradeOpenedEvent

ELSE IF one failed:
    → ROLLBACK: Close successful leg
    → Emit TradeFailedEvent
```

### Stored Position Data:

- coin_symbol, primary_symbol, side
- entry_z_score, entry_beta, entry_correlation, entry_hurst
- spread_mean, spread_std (для live Z расчёта)
- coin_size_usdt, primary_size_usdt, coin_contracts, primary_contracts
- coin_entry_price, primary_entry_price
- z_tp_threshold, z_sl_threshold
- leverage, opened_at, is_active

---

## 5. Exit Observer (Real-Time TP/SL)

**Цель:** Мониторинг открытых позиций в реальном времени для быстрого выхода по TP/SL.

### Логика:

1. **Получение TradeOpenedEvent** → создание ExitWatch
2. **Подписка на WebSocket** для COIN и ETH
3. **Real-time мониторинг** (debounce 1 секунда):

### Dynamic TP (Time-Based Coefficient):

Чем дольше позиция открыта, тем агрессивнее TP:

| Время в позиции | Коэффициент | Эффективный TP (при base 0.25) |
| --------------- | ----------- | ------------------------------ |
| 0-4 часа        | 1.0         | 0.25                           |
| 4-12 часов      | 3.0         | 0.75                           |
| 12-24 часа      | 5.0         | 1.25                           |
| 24+ часов       | 8.0         | 2.0                            |

```
dynamic_TP = z_tp_threshold × time_based_coefficient
```

### Exit Conditions:

```
IF |Z| <= dynamic_TP:
    → Emit ExitSignalEvent(TAKE_PROFIT)

ELSE IF |Z| >= z_sl_threshold:
    → Emit ExitSignalEvent(STOP_LOSS)
```

---

## 6. Close Trade (Закрытие позиции)

### Execution:

```
Parallel:
    Close COIN entirely (flash_close_position)
    Close PRIMARY partially (только contracts этого спреда)
```

**Важно:** PRIMARY закрывается частично, т.к. может быть несколько спредов с общим ETH хеджем.

### Cooldown Application:

| Exit Reason      | Cooldown                    |
| ---------------- | --------------------------- |
| TAKE_PROFIT      | ❌ Нет                      |
| STOP_LOSS        | ✅ COOLDOWN_BARS = 16 (~4h) |
| CORRELATION_DROP | ✅ COOLDOWN_BARS = 16 (~4h) |
| TIMEOUT          | ✅ COOLDOWN_BARS = 16 (~4h) |
| HURST_TRENDING   | ✅ COOLDOWN_BARS = 16 (~4h) |
| WATCH_TIMEOUT    | ✅ COOLDOWN_BARS = 16 (~4h) |

Cooldown предотвращает немедленный re-entry после неудачной сделки.

### Position Deactivation:

- `is_active = false` в MongoDB
- Позиция остаётся в базе для истории (soft delete)

---

## 7. Startup / Restore

При перезапуске бота:

1. **EntryObserver:** Восстанавливает watches из Redis (TTL = timeout + 5 min buffer)
2. **PositionStateService:**
   - Создаёт индексы MongoDB
   - Очищает expired cooldowns
3. **ExitObserver:**
   - Получает active positions из MongoDB
   - Создаёт ExitWatch для каждой
   - Подписывается на WebSocket

---

## Сводка параметров

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| TIMEFRAME                      | 15m       | Таймфрейм свечей                         |
| LOOKBACK_WINDOW_DAYS           | 3         | Окно для rolling расчётов                |
| MIN_CORRELATION                | 0.8       | Минимальная корреляция                   |
| MIN_BETA / MAX_BETA            | 0.5 / 2.0 | Диапазон допустимых бет                  |
| Z_ENTRY_THRESHOLD              | 2.1       | Базовый порог входа                      |
| Z_TP_THRESHOLD                 | 0.25      | Базовый порог TP                         |
| Z_SL_THRESHOLD                 | 4.0       | Порог SL                                 |
| ADAPTIVE_PERCENTILE            | 95        | Перцентиль для dynamic threshold         |
| DYNAMIC_THRESHOLD_WINDOW_BARS  | 440       | Окно для dynamic threshold (~4.5 дня)    |
| THRESHOLD_EMA_ALPHA            | 0.1       | Сглаживание dynamic threshold            |
| HURST_THRESHOLD                | 0.45      | Порог Hurst (< = mean-reverting)         |
| HURST_LOOKBACK_CANDLES         | 300       | Окно для Hurst расчёта                   |
| MAX_FUNDING_COST_THRESHOLD     | -0.0005   | Макс. расход на фандинг (-0.05% за 8h)   |
| TRAILING_ENTRY_PULLBACK        | 0.2       | Откат Z для подтверждения разворота      |
| TRAILING_ENTRY_TIMEOUT_MINUTES | 60        | Таймаут trailing entry                   |
| POSITION_SIZE_USDT             | 300       | Размер позиции COIN leg                  |
| MAX_OPEN_SPREADS               | 3         | Максимум одновременных спредов           |
| COOLDOWN_BARS                  | 16        | Cooldown после неудачного выхода (~4h)   |
| MAX_POSITION_BARS              | 144       | Максимальная длительность позиции (~36h) |

---

## Event Flow Summary

```
PlannerService (cron 15m)
    │
    ├─► check_and_close_timeouts() → ExitSignalEvent (TIMEOUT)
    │
    └─► OrchestratorService.run()
            │
            ├─► ScreenerService.scan()
            │       ├─► VolatilityFilter
            │       ├─► CorrelationService
            │       ├─► ZScoreService (+ dynamic threshold)
            │       └─► HurstFilter
            │
            ├─► Check Exit Conditions → ExitSignalEvent
            │
            ├─► Check Watched Pairs → remove from EntryObserver
            │
            └─► Check Entry Conditions
                    │
                    ├─► FundingFilter (block if toxic funding)
                    │
                    └─► PendingEntrySignalEvent
                                              │
                                              ▼
                                    EntryObserverService
                                    (WebSocket monitoring)
                                              │
                                              ├─► Cancel (TIMEOUT/FALSE_ALARM/SL_HIT)
                                              │
                                              └─► EntrySignalEvent (reversal confirmed)
                                                        │
                                                        ▼
                                              TradingService
                                              (atomic execution)
                                                        │
                                                        └─► TradeOpenedEvent
                                                                  │
                                                                  ▼
                                                        ExitObserverService
                                                        (WebSocket monitoring)
                                                                  │
                                                                  └─► ExitSignalEvent (TP/SL)
                                                                            │
                                                                            ▼
                                                                  TradingService
                                                                  (close spread)
                                                                            │
                                                                            └─► TradeClosedEvent
                                                                                      │
                                                                                      └─► Apply Cooldown (if adverse)
```
