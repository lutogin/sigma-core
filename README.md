# Statistical Arbitrage Bot — Полное описание архитектуры

## Общая концепция

Бот реализует стратегию **статистического арбитража** на бессрочных фьючерсах Binance. Основная идея — торговля спредом между альткоином (COIN) и ETH как индексом экосистемы. Когда спред отклоняется от исторической нормы (измеряется Z-score), бот открывает дельта-нейтральную позицию в ожидании возврата к среднему.

**Торгуемые пары:** LINK, AAVE, ONDO, RENDER, MANA, ETHFI — все против ETH/USDT:USDT как PRIMARY.

**Источник пар:** Пары загружаются из MongoDB (`TradingPairRepository`). Если MongoDB недоступна или пуста — fallback на `CONSISTENT_PAIRS` из конфигурации.

---

## Полный цикл работы бота (Pipeline)

### Визуальная схема цикла

```
  CRON */15 * * * *
        │
        ▼
  ┌─────────────┐
  │  Planner    │
  └──────┬──────┘
         │
         ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                    ScreenerService.scan()                        │
  │                                                                  │
  │  [1] Volatility Filter (ETH) ─── unsafe? ──► STOP + MarketUnsafe│
  │         │ safe                                                   │
  │         ▼                                                        │
  │  [2] Load OHLCV (3d × 3 + 2d lookback, 15m candles)            │
  │         │                                                        │
  │         ▼                                                        │
  │  [3] Correlation + Beta (rolling OLS vs ETH)                    │
  │         │                                                        │
  │         ▼                                                        │
  │  [4] Z-Score + Dynamic Threshold (EMA 95th percentile)          │
  │         │                                                        │
  │         ▼                                                        │
  │  [5] Filter: Correlation ≥ 0.80, Beta ∈ [0.5, 2.0]             │
  │         │                                                        │
  │         ▼ (только entry-кандидаты: |Z| ≥ threshold)             │
  │  [6] Filter: Hurst < 0.45  (mean-reverting spread)              │
  │         │                                                        │
  │         ▼                                                        │
  │  [7] Filter: Half-Life ≤ 48 bars (fast reversion)               │
  │         │                                                        │
  │         ▼                                                        │
  │  [8] Filter: ADF p-value < 0.15 (stationarity)                 │
  │         │                                                        │
  └─────────┼────────────────────────────────────────────────────────┘
            │
            ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                  OrchestratorService.run()                       │
  │                                                                  │
  │  [A] Check STRUCTURAL exits (open positions):                   │
  │      • CORRELATION_DROP (corr < 0.75)                           │
  │      • HURST_TRENDING (H ≥ 0.46, 2× confirmed)                 │
  │      → ExitSignalEvent                                          │
  │                                                                  │
  │  [B] Check watched pairs (EntryObserver):                       │
  │      • CORRELATION_DROP (corr < 0.77)                           │
  │      • HURST_TRENDING (H ≥ 0.46)                                │
  │      → Remove watch                                             │
  │                                                                  │
  │  [C] Check ENTRY conditions:                                    │
  │      • |Z| ≥ dynamic_threshold AND |Z| < 6.0 (extreme level)   │
  │      • Нет открытой позиции                                     │
  │      • Не в cooldown                                            │
  │      • active_positions < MAX_OPEN_SPREADS (6)                  │
  │      • Funding cost ≤ -0.10%/8h                                 │
  │      → PendingEntrySignalEvent                                  │
  │                                                                  │
  └──────────────────────────────────┬───────────────────────────────┘
                                     │
                                     ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │               EntryObserverService (WebSocket)                   │
  │                                                                  │
  │  • Подписка на book ticker (COIN + ETH) в реальном времени      │
  │  • Frozen Anchor: beta, spread_mean, spread_std НЕ обновляются  │
  │  • Track max_z в реальном времени                               │
  │                                                                  │
  │  Условия выхода из наблюдения:                                  │
  │  ✅ |Z| ≤ max_z - pullback(0.07/0.6) → ENTRY CONFIRMED         │
  │  ❌ Timeout > 120 мин → CANCEL + cooldown                       │
  │  ❌ |Z| < threshold - 0.45 → FALSE ALARM                        │
  │  ❌ |Z| ≥ 6.0 (extreme) → SL HIT                               │
  │                                                                  │
  └──────────────────────────┬───────────────────────────────────────┘
                             │ (reversal confirmed)
                             ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                    TradingService                                │
  │                                                                  │
  │  • Dynamic sizing: sqrt(16 / halflife) × 1000 USDT              │
  │  • Atomic open: COIN + PRIMARY in parallel                      │
  │  • Freeze params: beta, spread_mean, spread_std, TP, SL         │
  │  • Leverage: 10x cross margin                                   │
  │                                                                  │
  └──────────────────────────┬───────────────────────────────────────┘
                             │
                             ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │              ExitObserverService (WebSocket)                      │
  │                                                                  │
  │  Frozen Z-score: log(coin) - β×log(eth) - mean) / std          │
  │                                                                  │
  │  Порядок проверок на каждом тике:                               │
  │  1. TIMEOUT ≥ 48h (192 bars × 15m)                             │
  │  2. TAKE_PROFIT: |Z| ≤ TP × time_coef(1/3/5/8)                │
  │  3. STOP_LOSS: |Z| ≥ trailing_SL (or base SL=4.0)             │
  │  4. Update trailing SL (activation=1.4, offset=1.0)            │
  │                                                                  │
  └──────────────────────────┬───────────────────────────────────────┘
                             │
                             ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                      Close Trade                                 │
  │                                                                  │
  │  • Close COIN entirely + PRIMARY partially (contracts)          │
  │  • Cooldown 16 bars (~4h) на SL/CORRELATION/TIMEOUT/HURST      │
  │  • TP → без cooldown                                            │
  └──────────────────────────────────────────────────────────────────┘
```

---

## Архитектура выходов (Exit Architecture)

### Разделение ответственности:

| Компонент               | Частота               | Проверяет                       | Причины выхода                                 |
| ----------------------- | --------------------- | ------------------------------- | ---------------------------------------------- |
| **ExitObserverService** | Real-time (WebSocket) | TP/SL/TIMEOUT + **Trailing SL** | TAKE_PROFIT, STOP_LOSS (trailing), **TIMEOUT** |
| **OrchestratorService** | Каждые 15 минут       | Структурные сломы               | CORRELATION_DROP, HURST_TRENDING               |

### Почему такое разделение?

**Проблема "Moving Goalposts":** Если пересчитывать beta/spread_mean/spread_std на каждом 15m сканировании, Z-score будет "плавать" — позиция может никогда не достичь TP, потому что параметры постоянно меняются.

**Решение:**

- При входе "замораживаем" параметры (beta, spread_mean, spread_std)
- ExitObserver использует **frozen параметры** для расчёта Z-score
- Orchestrator проверяет только **структурные условия** (корреляция, Hurst)
- **TIMEOUT проверяется только в ExitObserver** — в реальном времени, точно в момент истечения

**Важно:** Orchestrator больше НЕ проверяет TP/SL/TIMEOUT. Это полностью ответственность ExitObserver.

---

## 1. Планировщик (PlannerService)

**Запуск:** Cron `*/15 * * * *` — каждые 15 минут.

**Последовательность:**

1. Запуск OrchestratorService.run()

---

## 2. Screener Pipeline (Фильтры поиска пар)

ScreenerService выполняет последовательную фильтрацию. Каждый шаг сужает множество кандидатов.

### Шаг 1: Volatility Filter (безопасность рынка)

Проверяет волатильность ETH (proxy для всего рынка).

| Параметр                   | Значение | Описание                            |
| -------------------------- | -------- | ----------------------------------- |
| VOLATILITY_WINDOW          | 24 bars  | Окно скользящей волатильности (6h)  |
| VOLATILITY_THRESHOLD       | 0.018    | Макс. допустимая волатильность      |
| VOLATILITY_CRASH_WINDOW    | 16 bars  | Окно для детекции краша (4h)        |
| VOLATILITY_CRASH_THRESHOLD | 0.05     | Порог падения (5% за 4h)           |

Если рынок "небезопасен" — сканирование прерывается, `MarketUnsafeEvent` → EntryObserver очищает все watches.

### Шаг 2: Загрузка данных

- OHLCV за **LOOKBACK_WINDOW_DAYS × 3 + 2 дня** (для rolling расчётов + dynamic threshold)
- Таймфрейм: **15m**
- Источник: Binance API через AsyncDataLoader

### Шаг 3: Correlation + Beta

- Rolling Correlation с ETH
- Rolling Beta (β) через OLS регрессию
- **Фильтр:** corr ≥ MIN_CORRELATION (0.80), β ∈ [0.5, 2.0]

Символы с низкой корреляцией или выходящей за пределы бетой отсеиваются — они декоррелировались от ETH.

### Шаг 4: Z-Score + Dynamic Threshold

- **Spread** = log(COIN_price) - β × log(ETH_price)
- **Z-Score** = (Spread - Mean) / Std
- **Dynamic Entry Threshold** = max(Z_ENTRY_THRESHOLD, EMA-сглаженный ADAPTIVE_PERCENTILE-й перцентиль |Z|)

Параметры динамического порога:

| Параметр                      | Значение | Описание                                    |
| ----------------------------- | -------- | ------------------------------------------- |
| Z_ENTRY_THRESHOLD             | 2.1      | Минимальный базовый порог входа              |
| ADAPTIVE_PERCENTILE           | 95       | Перцентиль для dynamic threshold             |
| DYNAMIC_THRESHOLD_WINDOW_BARS | 440      | Окно расчёта (440 bars ≈ 4.6 дней для 15m)  |
| THRESHOLD_EMA_ALPHA_UP        | 0.01     | Медленный рост (не теряем сигналы при спайке)|
| THRESHOLD_EMA_ALPHA_DOWN      | 0.05     | Быстрое падение (ловим больше входов)        |

### Шаг 5: Hurst Filter (только для entry-кандидатов)

Проверяется **только** для символов, у которых |Z| ≥ dynamic_threshold (потенциальные входы). Не-кандидаты проходят без проверки.

| Параметр              | Значение  | Описание                               |
| --------------------- | --------- | -------------------------------------- |
| HURST_THRESHOLD       | 0.45      | Вход: H < 0.45 (mean-reverting)       |
| HURST_LOOKBACK_CANDLES| 300       | 300 последних свечей для расчёта       |

Spread = log(COIN) - β × log(ETH). Hurst < 0.5 → mean-reverting, Hurst > 0.5 → trending.

### Шаг 6: Half-Life Filter (только для прошедших Hurst)

| Параметр                | Значение  | Описание                                  |
| ----------------------- | --------- | ----------------------------------------- |
| HALFLIFE_MAX_BARS       | 48 bars   | Макс. допустимый half-life (~12h для 15m) |
| HALFLIFE_LOOKBACK_CANDLES| 300      | 300 последних свечей для расчёта          |

Half-Life — время (в барах) за которое спред возвращается к среднему наполовину. Чем меньше — тем быстрее ревёрсия.

### Шаг 7: ADF Filter (только для прошедших Half-Life)

| Параметр             | Значение | Описание                                |
| -------------------- | -------- | --------------------------------------- |
| ADF_PVALUE_THRESHOLD | 0.15     | Макс. p-value (0.15 — ослабленный)     |
| ADF_LOOKBACK_CANDLES | 300      | 300 последних свечей для расчёта        |

Augmented Dickey-Fuller тест на стационарность спреда. p-value < threshold → спред стационарен → можно торговать mean-reversion.

### Итого: Последовательность фильтрации

```
Все пары (CONSISTENT_PAIRS / MongoDB)
  │
  ├─[Volatility]──── unsafe? → STOP
  │
  ├─[Correlation ≥ 0.80 + Beta ∈ [0.5, 2.0]]──── low? → skip
  │
  ├─[Z-Score + Dynamic Threshold]──── |Z| < threshold? → not a candidate
  │
  ├─[Hurst < 0.45]──── trending? → skip candidate
  │
  ├─[Half-Life ≤ 48 bars]──── too slow? → skip candidate
  │
  ├─[ADF p-value < 0.15]──── non-stationary? → skip candidate
  │
  └─► filtered_results (пары прошедшие все фильтры)
```

---

## 3. Оркестратор (OrchestratorService)

Координирует цикл сканирования. **Не проверяет TP/SL** — это делает ExitObserver.

### 3.1 Structural Exit Conditions (проверка открытых позиций)

Позиция, не прошедшая фильтры скринера (т.е. не попавшая в filtered_results), анализируется отдельно:

| Условие              | Порог                                | Подтверждение         | Описание                                  |
| -------------------- | ------------------------------------ | --------------------- | ----------------------------------------- |
| **HURST_TRENDING**   | H ≥ 0.46 (HURST_TRENDING_FOR_EXIT)  | 2 скана подряд        | Спред стал трендовым                      |
| **CORRELATION_DROP** | corr < 0.75 (CORRELATION_EXIT_THRESHOLD) | мгновенно         | Пара декоррелировалась                    |

**TP/SL/TIMEOUT НЕ проверяются здесь** — это делает ExitObserver в реальном времени.

### 3.2 Watch Validation (проверка наблюдаемых пар)

Для каждой пары в EntryObserver проверяется, не упала ли она из фильтров:

| Условие              | Порог                                    | Описание                              |
| -------------------- | ---------------------------------------- | ------------------------------------- |
| **HURST_TRENDING**   | H ≥ 0.46 (HURST_WATCH_THRESHOLD)        | Смягчённый порог для watches          |
| **CORRELATION_DROP** | corr < 0.77 (CORRELATION_WATCH_THRESHOLD)| Смягчённый порог для watches          |

> ADF и Half-Life нарушения **замьючены** — пара остаётся в наблюдении даже если ADF/HL ухудшились.

### 3.3 Correlation Hysteresis (Гистерезис корреляции)

| Действие              | Порог                                 | Описание                              |
| --------------------- | ------------------------------------- | ------------------------------------- |
| **Вход в позицию**    | MIN_CORRELATION (0.80)                | Строгий порог для новых позиций       |
| **Выход из позиции**  | CORRELATION_EXIT_THRESHOLD (0.75)     | Смягчённый порог для открытых позиций |
| **Удаление из watch** | CORRELATION_WATCH_THRESHOLD (0.77)    | Смягчённый порог для наблюдаемых пар  |

### 3.4 Entry Conditions Check

**Условия входа (после фильтрации):**

1. `|Z| >= dynamic_entry_threshold` (адаптивный для каждого символа)
2. `|Z| < Z_EXTREME_LEVEL (6.0)` — не слишком экстремальный сигнал
3. Нет открытой позиции по символу
4. Символ не в cooldown
5. `active_positions < MAX_OPEN_SPREADS (6)`
6. Funding filter: net cost ≤ -0.10%/8h (`MAX_FUNDING_COST_THRESHOLD = -0.0010`)

При прохождении → emit `PendingEntrySignalEvent`

---

## 4. Entry Observer (Trailing Entry)

**Цель:** Дождаться подтверждения разворота перед входом. Не входить "вслепую" на пике.

### Параметры (prod):

| Параметр                        | Значение | Описание                                        |
| ------------------------------- | -------- | ----------------------------------------------- |
| TRAILING_ENTRY_PULLBACK         | 0.07     | Откат Z от максимума (normal signals)           |
| TRAILING_ENTRY_PULLBACK_EXTREME | 0.6      | Откат Z для extreme сигналов (|Z| > z_sl)       |
| TRAILING_ENTRY_TIMEOUT_MINUTES  | 120      | Макс. время наблюдения (2 часа)                 |
| FALSE_ALARM_HYSTERESIS          | 0.45     | Отмена только если Z упал ниже threshold - 0.45 |
| Z_EXTREME_LEVEL                 | 6.0      | Макс. Z для watch (выше → SL HIT)              |

### Логика мониторинга (WebSocket):

```
При получении PendingEntrySignalEvent:
    → Подписка на book ticker (COIN + ETH)
    → Мониторинг каждую ~1 секунду:

    1. FIRST: IF |Z| ≤ max_z - pullback → ✅ REVERSAL CONFIRMED → EntrySignalEvent
    2. IF timeout > 120 мин → Cancel: TIMEOUT + cooldown
    3. IF |Z| < threshold - 0.45 → Cancel: FALSE_ALARM
    4. IF |Z| ≥ 6.0 (extreme level) → Cancel: SL_HIT
    5. IF |Z| > max_z → Обновить max_z (спред ещё расширяется)
```

### Frozen Anchor модель (для existing watches)

При повторном получении `PendingEntrySignalEvent` для уже наблюдаемого символа:

```
1. Якорные параметры (beta, spread_mean, spread_std, z_entry_threshold) НЕ обновляются
2. max_z НЕ обновляется со скана (только из WebSocket)
3. Обновляется только halflife (влияет на sizing, не на trailing)
4. Liveness check: if |Z_new_scan| < frozen_threshold - hysteresis → Cancel
5. Иначе → keep watching with frozen anchor
```

**Зачем Frozen Anchor?** Trailing entry — реакция на КОНКРЕТНЫЙ экстремум. Обновление параметров каждые 15m вызывает "moving goalposts", где цель входа постоянно сдвигается и трейды не исполняются.

### Два режима pullback:

| Условие                 | Pullback | Описание                             |
| ----------------------- | -------- | ------------------------------------ |
| max_z ≤ Z_SL_THRESHOLD  | 0.07     | Normal signal — малый откат          |
| max_z > Z_SL_THRESHOLD  | 0.6      | Extreme signal — ждём существенный откат |

### Volatility Filter

При `MarketUnsafeEvent`:
- **Все активные watches отменяются**
- Cooldown применяется к каждому символу

---

## 5. Trading Service

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
BaseSize = POSITION_SIZE_USDT (1000 USDT)
TargetHalfLife = TARGET_HALFLIFE_BARS (16 bars = 4h)
```

| Half-Life (bars) | sqrt Multiplier | Описание                      |
| ---------------- | --------------- | ----------------------------- |
| 4 bars (1h)      | 2.0x (capped)   | Очень быстрая ревёрсия        |
| 8 bars (2h)      | 1.41x           | Быстрая ревёрсия              |
| 16 bars (4h)     | 1.0x            | Эталон (TARGET_HALFLIFE_BARS) |
| 32 bars (8h)     | 0.71x           | Медленная ревёрсия            |
| 48 bars (12h)    | 0.58x           | Очень медленная ревёрсия      |

**Лимиты:** MIN_SIZE_MULTIPLIER (0.5x) – MAX_SIZE_MULTIPLIER (2.1x)

### Extreme Entry SL Extension:

Если entry Z > Z_SL_THRESHOLD (4.0), стандартный SL будет немедленно сработан. Поэтому:

```
IF |entry_z| > Z_SL_THRESHOLD:
    SL = |entry_z| + Z_SL_EXTREME_OFFSET (0.5)
    # Пример: entry_z=4.3 → SL=4.8
```

### Atomic Execution:

```
Parallel:
    Open COIN position (limit, fallback to market)
    Open PRIMARY position (hedge)

IF both succeeded → TradeOpenedEvent → ExitObserver starts monitoring
ELSE → Rollback (flash_close_position)
```

**Leverage:** 10x cross margin.

---

## 6. Exit Observer (Real-Time TP/SL/TIMEOUT)

**Главный компонент для выходов.** Работает в реальном времени через WebSocket.

### Ключевая особенность: Frozen Parameters

ExitObserver использует **параметры, сохранённые при входе**:

```python
# Расчёт Z-score с frozen параметрами:
current_spread = log(coin_price) - frozen_beta × log(primary_price)
z_score = (current_spread - frozen_spread_mean) / frozen_spread_std
```

Это гарантирует, что TP/SL рассчитываются относительно **того же риск-профиля**, что был при входе.

### Exit Conditions (порядок проверки на каждом тике):

1. **TIMEOUT** — позиция открыта ≥ MAX_POSITION_BARS × 15 мин = **48 часов** (192 bars)
2. **TAKE_PROFIT** — |Z| ≤ z_tp × time_based_coefficient
3. **STOP_LOSS** — |Z| ≥ trailing_sl_threshold (начинается с z_sl=4.0, ужесточается)
4. **Trailing SL Update** — если SL не сработал, обновляем min_z и trailing SL

### Dynamic TP (Time-Based):

Чем дольше позиция открыта, тем легче достичь TP:

| Время в позиции | Коэффициент | Пример (TP_base=0.25, entry Z=3.0) |
| --------------- | ----------- | ----------------------------------- |
| 0-4 часа        | 1×          | TP при |Z| ≤ 0.25                   |
| 4-12 часов      | 3×          | TP при |Z| ≤ 0.75                   |
| 12-24 часа      | 5×          | TP при |Z| ≤ 1.25                   |
| 24+ часов       | 8×          | TP при |Z| ≤ 2.0                    |

### TIMEOUT check (Real-time):

ExitObserver проверяет TIMEOUT на каждом тике WebSocket:

```python
if watch.watch_duration_minutes >= max_position_minutes:  # 192 × 15 = 2880 мин = 48h
    emit ExitSignalEvent(TIMEOUT)
```

### Trailing Stop Loss (Smart SL):

Если Z-score пошёл в нашу сторону, а потом развернулся — глупо ждать стопа на 4.0.

**Логика:**

1. Отслеживаем `min_z_reached` — минимальный |Z| достигнутый во время жизни позиции
2. Активация trailing SL происходит когда Z восстановился ≥ `TRAILING_SL_ACTIVATION` (1.4) от entry
3. Новый SL = max(z_entry_threshold, min_z_reached + TRAILING_SL_OFFSET (1.0))
4. SL может только ужесточаться (уменьшаться), никогда не ослабляться

**Пример:**

| Событие               | Entry Z | Current Z | min_z | SL                       |
| --------------------- | ------- | --------- | ----- | ------------------------ |
| Вход                  | 3.0     | 3.0       | 3.0   | 4.0                      |
| Z идёт в нашу сторону | 3.0     | 2.5       | 2.5   | 4.0 (не активирован)     |
| Z продолжает          | 3.0     | 1.5       | 1.5   | 4.0 → 2.5 (активирован! 3.0-1.5=1.5 ≥ 1.4) |
| Z достиг минимума     | 3.0     | 1.0       | 1.0   | 2.5 → 2.1 (min_z+offset=2.0, но floor=entry_thr) |
| Z развернулся         | 3.0     | 2.2       | 1.0   | 2.1 (SL hit!)            |

**Конфигурация (prod):**

```python
TRAILING_SL_OFFSET: float = 1.0      # Добавляется к min_z
TRAILING_SL_ACTIVATION: float = 1.4  # Min Z recovery для активации
```

### Restore при перезапуске:

- Загружает active positions из MongoDB
- Восстанавливает frozen параметры
- Подписывается на WebSocket
- min_z_reached инициализируется как entry_z (неизвестны промежуточные значения)

---

## 7. Close Trade

### Execution:

```
Parallel:
    Close COIN entirely (flash_close_position)
    Close PRIMARY partially (только contracts этого спреда, с указанием стороны)
```

### Cooldown:

| Exit Reason      | Cooldown              |
| ---------------- | --------------------- |
| TAKE_PROFIT      | ❌ Нет                |
| STOP_LOSS        | ✅ 16 bars (~4h)      |
| CORRELATION_DROP | ✅ 16 bars            |
| TIMEOUT          | ✅ 16 bars            |
| HURST_TRENDING   | ✅ 16 bars            |
| WATCH_TIMEOUT    | ✅ cooldown (trailing) |

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
│     ├─► CorrelationService + Beta             ▼                             │
│     ├─► ZScoreService (+ dynamic threshold)   EntryObserverService          │
│     └─► Filter Chain (entry candidates only): ├─► Cancel all watches        │
│         • Hurst (H < 0.45)                    └─► Clear state               │
│         • HalfLife (HL ≤ 48)                                                │
│         • ADF (p < 0.15)                                                    │
│                                                                             │
│  2. Check STRUCTURAL Exits (open positions):                                │
│     • CORRELATION_DROP (corr < 0.75)                                        │
│     • HURST_TRENDING (H ≥ 0.46, 2× confirmed)                              │
│     [NO TP/SL/TIMEOUT here - that's ExitObserver's job]                     │
│                                                                             │
│  3. Check Watches (EntryObserver):                                          │
│     • CORRELATION_DROP (corr < 0.77)                                        │
│     • HURST_TRENDING (H ≥ 0.46)                                             │
│                                                                             │
│  4. Check Entry Conditions + Funding Filter                                 │
│     → PendingEntrySignalEvent                                               │
│                                                                             │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
    ┌───────────────────────────────┐   ┌───────────────────────────────────┐
    │    EntryObserverService       │   │  (For existing watches)           │
    │    (WebSocket trailing entry) │   │  Frozen Anchor Validation:        │
    │                               │   │  • Frozen beta, spread_std, Z_thr │
    │  New watch:                   │   │  • Liveness check with new scan Z │
    │  • Subscribe to COIN + ETH    │   │  • IF Z < threshold - 0.45       │
    │  • Freeze anchor params       │   │  •   → Cancel (signal lost)      │
    │  • Track max_z via WebSocket  │   │  • ELSE → keep frozen anchor     │
    │  • Wait for pullback          │   │  • Update only halflife           │
    │                               │   │                                   │
    │  Conditions:                  │   └───────────────────────────────────┘
    │  • Pullback 0.07/0.6 → Entry │
    │  • Timeout 120min → Cancel    │
    │  • False alarm → Cancel       │
    │  • Z ≥ 6.0 → SL HIT          │
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
    │  • HL-based sizing (sqrt)     │
    │  • Open COIN + PRIMARY (10x)  │
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
    │  1. TIMEOUT: duration ≥ 48h (192 bars × 15m)                         │
    │  2. TAKE_PROFIT: |Z| ≤ TP × time_coef (1/3/5/8)                     │
    │  3. STOP_LOSS: |Z| ≥ trailing_SL (activation=1.4, offset=1.0)       │
    │  4. Update trailing SL: track min_z, tighten SL if recovered ≥1.4    │
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

| Параметр             | Описание                                           |
| -------------------- | -------------------------------------------------- |
| `use_trailing_entry` | Эмуляция EntryObserver на 1m свечах                |
| `use_live_exit`      | Эмуляция ExitObserver (TP/SL/TIMEOUT) на 1m свечах |

### Комбинации:

| trailing_entry | live_exit | Entry       | Exit             | Скорость |
| -------------- | --------- | ----------- | ---------------- | -------- |
| True           | True      | 1m trailing | 1m TP/SL/TIMEOUT | Медленно |
| True           | False     | 1m trailing | 15m TP/SL        | Средне   |
| False          | True      | Immediate   | 1m TP/SL/TIMEOUT | Средне   |
| False          | False     | Immediate   | 15m TP/SL        | Быстро   |

### Walk-Forward тесты:

`run_walk_forward_backtest.py` и `run_coin_walk_forward.py` используют быстрый режим (оба `false`) для ускорения.

---

## Сводка параметров (Production)

### Screener

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| TIMEFRAME                      | 15m       | Таймфрейм свечей                         |
| LOOKBACK_WINDOW_DAYS           | 3         | Окно для rolling расчётов                |
| MIN_CORRELATION                | 0.80      | Минимальная корреляция для входа         |
| CORRELATION_EXIT_THRESHOLD     | 0.75      | Порог выхода из позиции (гистерезис)     |
| CORRELATION_WATCH_THRESHOLD    | 0.77      | Порог удаления из watch (гистерезис)     |
| MIN_BETA / MAX_BETA            | 0.5 / 2.0 | Диапазон допустимых бет                  |

### Z-Score

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| Z_ENTRY_THRESHOLD              | 2.1       | Базовый порог входа (dynamic override)   |
| Z_TP_THRESHOLD                 | 0.25      | Базовый порог TP                         |
| Z_SL_THRESHOLD                 | 4.0       | Порог SL                                 |
| Z_SL_EXTREME_OFFSET            | 0.5       | SL offset для extreme входов (entry+0.5) |
| Z_EXTREME_LEVEL                | 6.0       | Макс. Z для watch (выше → отмена)        |
| ADAPTIVE_PERCENTILE            | 95        | Перцентиль для dynamic threshold         |
| DYNAMIC_THRESHOLD_WINDOW_BARS  | 440       | Окно для dynamic threshold (~4.6 дней)   |
| THRESHOLD_EMA_ALPHA_UP         | 0.01      | Медленный рост порога                    |
| THRESHOLD_EMA_ALPHA_DOWN       | 0.05      | Быстрое падение порога                   |

### Spread Quality Filters

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| HURST_THRESHOLD                | 0.45      | Порог Hurst (вход)                       |
| HURST_WATCH_THRESHOLD          | 0.46      | Tolerance для watches                    |
| HURST_TRENDING_FOR_EXIT        | 0.46      | Tolerance для позиций                    |
| HURST_TRENDING_CONFIRM_SCANS   | 2         | Скановых подтверждений для exit          |
| HURST_LOOKBACK_CANDLES         | 300       | Свечей для расчёта Hurst                 |
| HALFLIFE_MAX_BARS              | 48        | Макс. Half-Life (~12h)                   |
| HALFLIFE_LOOKBACK_CANDLES      | 300       | Свечей для расчёта Half-Life             |
| ADF_PVALUE_THRESHOLD           | 0.15      | Макс. p-value для стационарности         |
| ADF_LOOKBACK_CANDLES           | 300       | Свечей для расчёта ADF                   |

### Volatility

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| VOLATILITY_WINDOW              | 24 bars   | Окно волатильности (6h)                  |
| VOLATILITY_THRESHOLD           | 0.018     | Порог волатильности (1.8%)               |
| VOLATILITY_CRASH_WINDOW        | 16 bars   | Окно детекции краша (4h)                 |
| VOLATILITY_CRASH_THRESHOLD     | 0.05      | Порог краша (5%)                         |

### Trailing Entry

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| TRAILING_ENTRY_PULLBACK        | 0.07      | Откат Z для подтверждения (normal)       |
| TRAILING_ENTRY_PULLBACK_EXTREME| 0.6       | Откат Z для extreme сигналов             |
| TRAILING_ENTRY_TIMEOUT_MINUTES | 120       | Таймаут trailing entry (2 часа)          |
| FALSE_ALARM_HYSTERESIS         | 0.45      | Гистерезис для отмены watch              |
| MAX_FUNDING_COST_THRESHOLD     | -0.0010   | Макс. расход на фандинг (-0.10% за 8h)   |

### Position & Sizing

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| POSITION_SIZE_USDT             | 1000      | Размер позиции COIN leg                  |
| TARGET_HALFLIFE_BARS           | 16        | Эталон Half-Life для sizing (4h)         |
| MIN_SIZE_MULTIPLIER            | 0.5       | Мин. множитель размера позиции           |
| MAX_SIZE_MULTIPLIER            | 2.1       | Макс. множитель размера позиции          |
| MAX_OPEN_SPREADS               | 6         | Максимум одновременных спредов           |
| COOLDOWN_BARS                  | 16        | Cooldown после неудачного выхода (~4h)   |
| MAX_POSITION_BARS              | 192       | Макс. длительность позиции (~48h)        |
| EXCHANGE_DEFAULT_LEVERAGE      | 10        | Плечо                                    |

### Trailing SL

| Параметр                       | Значение  | Описание                                 |
| ------------------------------ | --------- | ---------------------------------------- |
| TRAILING_SL_OFFSET             | 1.0       | Offset от min_z для trailing SL          |
| TRAILING_SL_ACTIVATION         | 1.4       | Min Z recovery для активации trailing SL |

---

## Структура проекта

```
src/
├── config/
│   └── settings.py           # Все конфигурационные параметры
├── domain/
│   ├── data_loader/          # Загрузка OHLCV данных (async)
│   ├── entry_observer/       # Trailing entry логика (WebSocket)
│   ├── exit_observer/        # Real-time TP/SL/TIMEOUT мониторинг (WebSocket)
│   ├── orchestrator/         # Координация цикла сканирования
│   ├── planner/              # Cron планировщик
│   ├── position_state/       # Управление состоянием позиций (MongoDB)
│   ├── screener/             # Фильтры и расчёт Z-score
│   │   ├── correlation/      # Rolling correlation + beta (OLS)
│   │   ├── z_score/          # Z-Score + dynamic threshold (EMA)
│   │   ├── hurst_filter/     # Hurst exponent filter
│   │   ├── halflife_filter/  # Half-life filter (OU process)
│   │   ├── adf_filter/       # Augmented Dickey-Fuller test
│   │   ├── volatility_filter/# Market safety filter (ETH vol)
│   │   └── funding_filter/   # Funding rate cost filter
│   ├── trading/              # Исполнение сделок (atomic, limit+market)
│   └── trading_pairs/        # Репозиторий пар из MongoDB
├── infra/
│   ├── container.py          # Dependency Injection
│   ├── event_emitter/        # Pub/Sub события
│   ├── mongo.py              # MongoDB подключение
│   ├── redis.py              # Redis (Upstash)
│   └── scheduler.py          # APScheduler cron
└── integrations/
    ├── exchange/             # Binance API клиент (ccxt)
    └── telegram/             # Telegram бот (уведомления)

backtests/
├── run_backtest.py           # Основной бектест
├── run_walk_forward_backtest.py
└── run_coin_walk_forward.py
```
