# Statistical Arbitrage Bot — Полное описание архитектуры

## Общая концепция

Бот реализует стратегию **статистического арбитража** на бессрочных фьючерсах Binance. Основная идея — торговля спредом между альткоином (COIN) и ETH как индексом экосистемы. Когда спред отклоняется от исторической нормы (измеряется Z-score), бот открывает дельта-нейтральную позицию в ожидании возврата к среднему.

**Торгуемые пары:** LINK, UNI, AAVE, RENDER, TURBO, ENS, FET, MORPHO, SPX — все против ETH/USDT:USDT как PRIMARY.

**Источник пар:** Пары загружаются из MongoDB (`TradingPairRepository`). Если MongoDB недоступна или пуста — fallback на `CONSISTENT_PAIRS` из конфигурации.

---

## Архитектура выходов (Exit Architecture)

### Разделение ответственности:

| Компонент               | Частота               | Проверяет                        | Причины выхода                                     |
| ----------------------- | --------------------- | -------------------------------- | -------------------------------------------------- |
| **ExitObserverService** | Real-time (WebSocket) | TP/SL/TIMEOUT + **Trailing SL** | TAKE_PROFIT, STOP_LOSS (trailing), **TIMEOUT**     |
| **OrchestratorService** | Каждые 15 минут       | Структурные сломы            | CORRELATION_DROP, HURST_TRENDING, TIMEOUT (backup) |

### Почему такое разделение?

**Проблема "Moving Goalposts":** Если пересчитывать beta/spread_mean/spread_std на каждом 15m сканировании, Z-score будет "плавать" — позиция может никогда не достичь TP, потому что параметры постоянно меняются.

**Решение:**

- При входе "замораживаем" параметры (beta, spread_mean, spread_std)
- ExitObserver использует **frozen параметры** для расчёта Z-score
- Orchestrator проверяет только **структурные условия** (корреляция, Hurst, таймаут как fallback)

---

## 1. Планировщик (PlannerService)

**Запуск:** Cron `*/15 * * * *` — каждые 15 минут.

**Последовательность:**

1. Запуск OrchestratorService.run()

---

## 2. Оркестратор (OrchestratorService)

Координирует цикл сканирования. **Не проверяет TP/SL** — это делает ExitObserver.

### 2.1 Screener Pipeline

#### Загрузка данных

- OHLCV за **LOOKBACK_WINDOW_DAYS × 3 + 2 дня** (для rolling расчётов + dynamic threshold)
- Таймфрейм: **15m**

#### Volatility Filter

- Проверяет волатильность ETH
- Если рынок "небезопасен" — сканирование прерывается, все активные watches отменяются
- Emit `MarketUnsafeEvent` → EntryObserver очищает все watches

#### Correlation Service

- Rolling Correlation с ETH
- Rolling Beta (β) через OLS регрессию

#### Z-Score Service

- **Spread** = log(COIN_price) - β × log(ETH_price)
- **Z-Score** = (Spread - Mean) / Std
- **Dynamic Entry Threshold** = EMA-сглаженный 95-й перцентиль |Z| (не ниже Z_ENTRY_THRESHOLD)

---

### 2.2 Correlation Hysteresis (Гистерезис корреляции)

Для предотвращения преждевременных выходов при небольших флуктуациях корреляции используются **разные пороги** для входа и выхода:

| Действие              | Порог                            | Описание                                   |
| --------------------- | -------------------------------- | ------------------------------------------ |
| **Вход в позицию**    | MIN_CORRELATION (0.80)           | Строгий порог для новых позиций            |
| **Выход из позиции**  | CORRELATION_EXIT_THRESHOLD (0.70) | Смягчённый порог для открытых позиций      |
| **Удаление из watch** | CORRELATION_WATCH_THRESHOLD (0.75) | Смягчённый порог для наблюдаемых пар       |

**Логика:** Вход требует высокой корреляции, но небольшое временное снижение не должно вызывать немедленный выход.

---

### 2.3 Structural Exit Conditions (Проверка структурных выходов)

Orchestrator проверяет **только структурные условия**:

| Условие              | Порог                              | Описание                        |
| -------------------- | ---------------------------------- | ------------------------------- |
| **CORRELATION_DROP** | corr < CORRELATION_EXIT_THRESHOLD  | Пара декоррелировалась          |
| **HURST_TRENDING**   | H >= HURST_THRESHOLD + tolerance   | Спред стал трендовым            |
| **TIMEOUT**          | bars >= MAX_POSITION_BARS          | Позиция > 24 часов (fallback)   |

**TP/SL НЕ проверяются здесь** — это делает ExitObserver в реальном времени.

---

### 2.4 Entry Conditions Check

**Условия входа:**

1. `|Z| >= dynamic_entry_threshold`
2. `|Z| < Z_SL_THRESHOLD (4.0)`
3. Нет открытой позиции по символу
4. Символ не в cooldown
5. `active_positions < MAX_OPEN_SPREADS`

**Фильтры качества спреда:**

| Фильтр          | Условие               | Описание                           |
| --------------- | --------------------- | ---------------------------------- |
| **Hurst**       | H < 0.45              | Mean-reverting spread              |
| **Half-Life**   | HL <= 48 bars         | Быстрый возврат к среднему         |
| **ADF**         | p-value < 0.08        | Стационарность спреда              |
| **Funding**     | net cost > -0.05%/8h  | Приемлемый расход на фандинг       |

При прохождении → emit `PendingEntrySignalEvent`

---

## 3. Entry Observer (Trailing Entry)

**Цель:** Дождаться подтверждения разворота перед входом.

### Параметры:

- **TRAILING_ENTRY_PULLBACK = 0.2** — откат Z от максимума
- **TRAILING_ENTRY_TIMEOUT_MINUTES = 45** — максимальное время наблюдения
- **FALSE_ALARM_HYSTERESIS = 0.2** — отмена только если Z упал ниже threshold - hysteresis

### Логика мониторинга (WebSocket):

```
При получении PendingEntrySignalEvent:
    → Подписка на WebSocket (COIN + ETH)
    → Мониторинг каждую секунду:

    IF timeout > 45 min → Cancel: TIMEOUT
    IF |Z| < threshold - hysteresis → Cancel: FALSE_ALARM
    IF |Z| >= SL_threshold → Cancel: SL_HIT
    IF |Z| > max_z → Обновить max_z (новый пик)
    IF |Z| <= max_z - pullback → ✅ REVERSAL CONFIRMED → EntrySignalEvent
```

### Re-Validate & Reset (каждые 15 минут)

При получении нового `PendingEntrySignalEvent` для уже отслеживаемого символа:

```
1. Обновить параметры расчёта (beta, spread_mean, spread_std, halflife)
2. Пересчитать Z с новыми параметрами
3. Проверить CORRELATION_WATCH_THRESHOLD (гистерезис)
4. Проверить HURST с tolerance
5. IF new_|Z| < entry_threshold - hysteresis → Cancel: PARAM_INVALIDATED
6. ELSE → Reset max_z = new_|Z| (предотвращает "Parameter Jump")
```

**Зачем Reset max_z?** При изменении spread_std текущий Z-score меняется. Если std вырос — Z упал. Без сброса max_z бот увидит "откат" которого не было, и войдёт ложно. Reset гарантирует, что pullback измеряется от **нового** пика после пересчёта параметров.

### Volatility Filter

При получении `MarketUnsafeEvent` (высокая волатильность ETH):
- **Все активные watches отменяются**
- Cooldown применяется к каждому символу

---

## 4. Trading Service

### При входе сохраняет "frozen" параметры:

- `entry_beta` — beta на момент входа
- `spread_mean` — среднее спреда
- `spread_std` — стандартное отклонение спреда
- `z_tp_threshold`, `z_sl_threshold`
- `entry_halflife` — half-life для логирования

Эти параметры используются ExitObserver для расчёта Z-score.

### Dynamic Position Sizing (Half-Life Based):

Размер позиции рассчитывается динамически на основе Half-Life спреда с использованием **корня**:

```
Size = BaseSize × sqrt(TargetHalfLife / CurrentHalfLife)
```

Использование `sqrt` сглаживает экстремальные значения:

| Half-Life (bars) | sqrt Multiplier | Описание                           |
| ---------------- | --------------- | ---------------------------------- |
| 3 bars (45min)   | 2.0x (capped)   | Очень быстрая ревёрсия             |
| 6 bars (1.5h)    | 1.41x           | Быстрая ревёрсия                   |
| 12 bars (3h)     | 1.0x            | Эталон (TARGET_HALFLIFE_BARS)      |
| 24 bars (6h)     | 0.71x           | Медленная ревёрсия                 |
| 48 bars (12h)    | 0.5x (floor)    | Очень медленная ревёрсия           |

**Лимиты:** MIN_SIZE_MULTIPLIER (0.5x) – MAX_SIZE_MULTIPLIER (2.0x)

**Логика:** Чем быстрее спред возвращается к среднему, тем больше можно рисковать. Sqrt сглаживает крайние значения.

### Atomic Execution:

```
Parallel:
    Open COIN position
    Open PRIMARY position (hedge)

IF both succeeded → TradeOpenedEvent
ELSE → Rollback
```

---

## 5. Exit Observer (Real-Time TP/SL/TIMEOUT)

**Главный компонент для выходов.** Работает в реальном времени через WebSocket.

### Ключевая особенность: Frozen Parameters

ExitObserver использует **параметры, сохранённые при входе**:

```python
# Расчёт Z-score с frozen параметрами:
current_spread = log(coin_price) - frozen_beta × log(primary_price)
z_score = (current_spread - frozen_spread_mean) / frozen_spread_std
```

Это гарантирует, что TP/SL рассчитываются относительно **того же риск-профиля**, что был при входе.

### Exit Conditions (порядок проверки):

1. **TIMEOUT** — позиция открыта >= `max_position_minutes`
2. **TAKE_PROFIT** — |Z| <= dynamic_TP
3. **STOP_LOSS** — |Z| >= trailing_sl_threshold (starts at z_sl, tightens with profit)
4. **Trailing SL Update** — если SL не сработал, обновляем min_z и trailing SL

### Dynamic TP (Time-Based):

Чем дольше позиция открыта, тем легче достичь TP:

| Время в позиции | Коэффициент | Пример (entry Z=3.0) |
| --------------- | ----------- | -------------------- |
| 0-4 часа        | 1×          | TP при Z ≤ 0.3       |
| 4-12 часов      | 3×          | TP при Z ≤ 0.9       |
| 12-24 часа      | 5×          | TP при Z ≤ 1.5       |
| 24+ часов       | 8×          | TP при Z ≤ 2.4       |

### TIMEOUT check (Real-time):

ExitObserver теперь проверяет TIMEOUT на каждом тике WebSocket:

```python
if watch.watch_duration_minutes >= max_position_minutes:
    emit ExitSignalEvent(TIMEOUT)
```

Это гарантирует, что позиция закроется **точно** в момент истечения таймаута, а не на следующем 15m сканировании.

### Trailing Stop Loss (Smart SL):

Если Z-score пошёл в нашу сторону, а потом развернулся — глупо ждать стопа на 4.0.

**Логика:**

1. Отслеживаем `min_z_reached` — минимальный |Z| достигнутый во время жизни позиции
2. Активация trailing SL происходит когда Z восстановился >= `TRAILING_SL_ACTIVATION` от входа
3. Новый SL = max(z_entry_threshold, min_z_reached + TRAILING_SL_OFFSET)
4. SL может только ужесточаться (уменьшаться), никогда не ослабляться

**Пример:**

| Событие | Entry Z | Current Z | min_z | SL |
| ------- | ------- | --------- | ----- | --- |
| Вход | 3.0 | 3.0 | 3.0 | 4.0 |
| Z идёт в нашу сторону | 3.0 | 2.5 | 2.5 | 4.0 (не активирован) |
| Z продолжает | 3.0 | 1.8 | 1.8 | 4.0 → 3.3 (активирован!) |
| Z достиг минимума | 3.0 | 1.0 | 1.0 | 3.3 → 2.5 |
| Z развернулся | 3.0 | 2.6 | 1.0 | 2.5 (SL hit!) |

**Конфигурация:**

```python
TRAILING_SL_OFFSET: float = 1.5      # Добавляется к min_z
TRAILING_SL_ACTIVATION: float = 1.0  # Min Z recovery для активации
```

### Restore при перезапуске:

- Загружает active positions из MongoDB
- Восстанавливает frozen параметры
- Подписывается на WebSocket

---

## 6. Close Trade

### Execution:

```
Parallel:
    Close COIN entirely
    Close PRIMARY partially (только contracts этого спреда)
```

### Cooldown:

| Exit Reason      | Cooldown         |
| ---------------- | ---------------- |
| TAKE_PROFIT      | ❌ Нет           |
| STOP_LOSS        | ✅ 16 bars (~4h) |
| CORRELATION_DROP | ✅ 16 bars       |
| TIMEOUT          | ✅ 16 bars       |
| HURST_TRENDING   | ✅ 16 bars       |

---

## Event Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PlannerService (cron 15m)                         │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         OrchestratorService.run()                           │
│                                                                             │
│  1. ScreenerService.scan()                                                  │
│     ├─► VolatilityFilter ──[unsafe]──► MarketUnsafeEvent                    │
│     │                                         │                             │
│     ├─► CorrelationService                    ▼                             │
│     ├─► ZScoreService (+ dynamic threshold)   EntryObserverService          │
│     └─► Filter Chain:                         ├─► Cancel all watches        │
│         • Hurst (H < 0.45)                    └─► Clear state               │
│         • HalfLife (HL <= 48)                                               │
│         • ADF (p < 0.08)                                                    │
│                                                                             │
│  2. Check STRUCTURAL Exits (open positions):                                │
│     • CORRELATION_DROP (corr < CORRELATION_EXIT_THRESHOLD)                  │
│     • HURST_TRENDING (H >= 0.45 + tolerance)                                │
│     • TIMEOUT (bars >= 96) [backup, ExitObserver handles real-time]         │
│     [NO TP/SL here - that's ExitObserver's job]                             │
│                                                                             │
│  3. Check Watches (EntryObserver):                                          │
│     • CORRELATION_DROP (corr < CORRELATION_WATCH_THRESHOLD)                 │
│     • HURST_TRENDING (H >= 0.45 + tolerance)                                │
│                                                                             │
│  4. Check Entry Conditions ──► PendingEntrySignalEvent                      │
│                                                                             │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
    ┌───────────────────────────────┐   ┌───────────────────────────────────┐
    │    EntryObserverService       │   │  (For existing watches)           │
    │    (WebSocket trailing entry) │   │  Re-Validate & Reset:             │
    │                               │   │  • Update beta, spread_std, HL    │
    │  New watch:                   │   │  • Recalc Z with new params       │
    │  • Subscribe to COIN + ETH    │   │  • Check CORRELATION hysteresis   │
    │  • Track max_z                │   │  • Check HURST with tolerance     │
    │  • Wait for pullback          │   │  • IF invalid → Cancel            │
    │                               │   │  • ELSE → Reset max_z             │
    │  Exit conditions:             │   │                                   │
    │  • Timeout → Cancel           │   └───────────────────────────────────┘
    │  • False alarm → Cancel       │
    │  • SL hit → Cancel            │
    │  • Pullback confirmed → Entry │
    │                               │
    └───────────────┬───────────────┘
                    │ (pullback confirmed)
                    ▼
    ┌───────────────────────────────┐
    │       EntrySignalEvent        │
    └───────────────┬───────────────┘
                    │
                    ▼
    ┌───────────────────────────────┐
    │       TradingService          │
    │  • Save frozen params         │
    │  • Calculate HL multiplier    │
    │  • Open COIN + PRIMARY legs   │
    │  • Atomic execution           │
    └───────────────┬───────────────┘
                    │
                    ▼
    ┌───────────────────────────────┐
    │      TradeOpenedEvent         │
    └───────────────┬───────────────┘
                    │
                    ▼
    ┌───────────────────────────────────────────────────────────────────────┐
    │                      ExitObserverService                              │
    │              (WebSocket TP/SL/TIMEOUT + Trailing SL)                  │
    │                                                                       │
    │  Uses FROZEN parameters from entry:                                   │
    │  • frozen_beta, frozen_spread_mean, frozen_spread_std                 │
    │                                                                       │
    │  Exit conditions (checked in order):                                  │
    │  1. TIMEOUT: duration >= max_position_minutes                         │
    │  2. TAKE_PROFIT: |Z| <= dynamic_TP (time-based coefficient)           │
    │  3. STOP_LOSS: |Z| >= trailing_sl (tightens as Z recovers)            │
    │  4. Update trailing SL: track min_z, tighten SL if Z recovered        │
    │                                                                       │
    │  Dynamic TP based on time in position:                                │
    │  • 0-4h: 1× coefficient                                               │
    │  • 4-12h: 3× coefficient                                              │
    │  • 12-24h: 5× coefficient                                             │
    │  • 24h+: 8× coefficient                                               │
    │                                                                       │
    └───────────────────────────────────┬───────────────────────────────────┘
                                        │
                                        ▼
                          ┌───────────────────────────────┐
                          │       ExitSignalEvent         │
                          │  (TP/SL/TIMEOUT or Structural)│
                          └───────────────┬───────────────┘
                                          │
                                          ▼
                          ┌───────────────────────────────┐
                          │   TradingService.close()      │
                          │  • Close COIN entirely        │
                          │  • Close PRIMARY partially    │
                          │  • Apply cooldown if needed   │
                          └───────────────────────────────┘
```

---

## Backtesting

Бекстесты (`backtests/run_backtest.py`) эмулируют продакшен логику:

### Режимы работы:

| Параметр             | Описание                                              |
| -------------------- | ----------------------------------------------------- |
| `use_trailing_entry` | Эмуляция EntryObserver на 1m свечах                   |
| `use_live_exit`      | Эмуляция ExitObserver (TP/SL/TIMEOUT) на 1m свечах    |

### Комбинации:

| trailing_entry | live_exit | Entry            | Exit              | Скорость   |
| -------------- | --------- | ---------------- | ----------------- | ---------- |
| True           | True      | 1m trailing      | 1m TP/SL/TIMEOUT  | Медленно   |
| True           | False     | 1m trailing      | 15m TP/SL         | Средне     |
| False          | True      | Immediate        | 1m TP/SL/TIMEOUT  | Средне     |
| False          | False     | Immediate        | 15m TP/SL         | Быстро     |

### Walk-Forward тесты:

`run_walk_forward_backtest.py` и `run_coin_walk_forward.py` используют быстрый режим (оба `false`) для ускорения.

---

## Сводка параметров

| Параметр                       | Значение  | Описание                                     |
| ------------------------------ | --------- | -------------------------------------------- |
| TIMEFRAME                      | 15m       | Таймфрейм свечей                             |
| LOOKBACK_WINDOW_DAYS           | 3         | Окно для rolling расчётов                    |
| MIN_CORRELATION                | 0.8       | Минимальная корреляция для входа             |
| CORRELATION_EXIT_THRESHOLD     | 0.7       | Порог выхода из позиции (гистерезис)         |
| CORRELATION_WATCH_THRESHOLD    | 0.75      | Порог удаления из watch (гистерезис)         |
| MIN_BETA / MAX_BETA            | 0.5 / 2.0 | Диапазон допустимых бет                      |
| Z_ENTRY_THRESHOLD              | 2.0       | Базовый порог входа (dynamic override)       |
| Z_TP_THRESHOLD                 | 0.25      | Базовый порог TP                             |
| Z_SL_THRESHOLD                 | 4.0       | Порог SL                                     |
| ADAPTIVE_PERCENTILE            | 95        | Перцентиль для dynamic threshold             |
| DYNAMIC_THRESHOLD_WINDOW_BARS  | 440       | Окно для dynamic threshold                   |
| THRESHOLD_EMA_ALPHA            | 0.1       | Сглаживание dynamic threshold                |
| HURST_THRESHOLD                | 0.45      | Порог Hurst (вход)                           |
| HURST_WATCH_TOLERANCE          | 0.005     | Tolerance для watches/позиций                |
| HALFLIFE_MAX_BARS              | 48        | Макс. Half-Life (~12h)                       |
| TARGET_HALFLIFE_BARS           | 12        | Эталон Half-Life для sizing                  |
| MIN_SIZE_MULTIPLIER            | 0.5       | Мин. множитель размера позиции               |
| MAX_SIZE_MULTIPLIER            | 2.0       | Макс. множитель размера позиции              |
| ADF_PVALUE_THRESHOLD           | 0.08      | Макс. p-value для стационарности             |
| MAX_FUNDING_COST_THRESHOLD     | -0.0005   | Макс. расход на фандинг (-0.05% за 8h)       |
| TRAILING_ENTRY_PULLBACK        | 0.2       | Откат Z для подтверждения разворота          |
| TRAILING_ENTRY_TIMEOUT_MINUTES | 45        | Таймаут trailing entry                       |
| FALSE_ALARM_HYSTERESIS         | 0.2       | Гистерезис для отмены watch                  |
| TRAILING_SL_OFFSET             | 1.5       | Offset от min_z для trailing SL              |
| TRAILING_SL_ACTIVATION         | 1.0       | Min Z recovery для активации trailing SL     |
| POSITION_SIZE_USDT             | 100       | Размер позиции COIN leg                      |
| MAX_OPEN_SPREADS               | 5         | Максимум спредов                             |
| COOLDOWN_BARS                  | 16        | Cooldown после неудачного выхода (~4h)       |
| MAX_POSITION_BARS              | 96        | Макс. длительность позиции (~24h)            |
| EXCHANGE_DEFAULT_LEVERAGE      | 5         | Плечо                                        |

---

## Структура проекта

```
src/
├── config/
│   └── settings.py           # Все конфигурационные параметры
├── domain/
│   ├── data_loader/          # Загрузка OHLCV данных
│   ├── entry_observer/       # Trailing entry логика
│   ├── exit_observer/        # Real-time TP/SL/TIMEOUT мониторинг
│   ├── orchestrator/         # Координация цикла сканирования
│   ├── planner/              # Cron планировщик
│   ├── position_state/       # Управление состоянием позиций
│   ├── screener/             # Фильтры и расчёт Z-score
│   │   ├── correlation.py
│   │   ├── z_score/
│   │   ├── hurst_filter.py
│   │   ├── halflife_filter.py
│   │   ├── adf_filter.py
│   │   ├── volatility_filter.py
│   │   └── funding_filter.py
│   ├── trading/              # Исполнение сделок
│   └── trading_pairs/        # Репозиторий пар из MongoDB
├── infra/
│   ├── container.py          # Dependency Injection
│   ├── event_emitter/        # Pub/Sub события
│   └── mongo.py              # MongoDB подключение
└── integrations/
    └── exchange/             # Binance API клиент

backtests/
├── run_backtest.py           # Основной бектест
├── run_walk_forward_backtest.py
└── run_coin_walk_forward.py
```
