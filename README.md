# Statistical Arbitrage Bot — Полное описание архитектуры

## Общая концепция

Бот реализует стратегию **статистического арбитража** на бессрочных фьючерсах Binance. Основная идея — торговля спредом между альткоином (COIN) и ETH как индексом экосистемы. Когда спред отклоняется от исторической нормы (измеряется Z-score), бот открывает дельта-нейтральную позицию в ожидании возврата к среднему.

**Торгуемые пары:** LINK, UNI, AAVE, RENDER, TURBO, ENS, FET, MORPHO, SPX — все против ETH/USDT:USDT как PRIMARY.

**Источник пар:** Пары загружаются из MongoDB (`TradingPairRepository`). Если MongoDB недоступна или пуста — fallback на `CONSISTENT_PAIRS` из конфигурации.

---

## Архитектура выходов (Exit Architecture)

### Разделение ответственности:

| Компонент               | Частота               | Проверяет         | Причины выхода                            |
| ----------------------- | --------------------- | ----------------- | ----------------------------------------- |
| **ExitObserverService** | Real-time (WebSocket) | TP/SL по Z-score  | TAKE_PROFIT, STOP_LOSS                    |
| **OrchestratorService** | Каждые 15 минут       | Структурные сломы | CORRELATION_DROP, HURST_TRENDING, TIMEOUT |

### Почему такое разделение?

**Проблема "Moving Goalposts":** Если пересчитывать beta/spread_mean/spread_std на каждом 15m сканировании, Z-score будет "плавать" — позиция может никогда не достичь TP, потому что параметры постоянно меняются.

**Решение:**

- При входе "замораживаем" параметры (beta, spread_mean, spread_std)
- ExitObserver использует **frozen параметры** для расчёта Z-score
- Orchestrator проверяет только **структурные условия** (корреляция, Hurst, таймаут)

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

#### Correlation Service

- Rolling Correlation с ETH
- Rolling Beta (β) через OLS регрессию

#### Z-Score Service

- **Spread** = log(COIN_price) - β × log(ETH_price)
- **Z-Score** = (Spread - Mean) / Std
- **Dynamic Entry Threshold** = EMA-сглаженный 95-й перцентиль |Z|

---

### 2.2 Structural Exit Conditions (Проверка структурных выходов)

Orchestrator проверяет **только структурные условия**:

| Условие              | Порог      | Описание               |
| -------------------- | ---------- | ---------------------- |
| **CORRELATION_DROP** | corr < 0.8 | Пара декоррелировалась |
| **HURST_TRENDING**   | H >= 0.45  | Спред стал трендовым   |
| **TIMEOUT**          | bars >= 96 | Позиция > 24 часов     |

**TP/SL НЕ проверяются здесь** — это делает ExitObserver в реальном времени.

---

### 2.3 Entry Conditions Check

**Условия входа:**

1. `|Z| >= dynamic_entry_threshold`
2. `|Z| < Z_SL_THRESHOLD (4.0)`
3. Нет открытой позиции по символу
4. Символ не в cooldown
5. `active_positions < MAX_OPEN_SPREADS`

**Фильтры качества спреда:**

- **Hurst Filter:** H < 0.45 (mean-reverting)
- **Half-Life Filter:** HL <= 48 bars
- **ADF Filter:** p-value < 0.08 (стационарность)
- **Funding Filter:** net cost > -0.05% за 8h

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
    IF |Z| <= max_z - 0.2 → ✅ REVERSAL CONFIRMED → EntrySignalEvent
```

### Re-Validate & Reset (каждые 15 минут)

При получении нового `PendingEntrySignalEvent` для уже отслеживаемого символа:

```
1. Обновить параметры расчёта (beta, spread_mean, spread_std)
2. Пересчитать Z с новыми параметрами
3. IF new_|Z| < entry_threshold → Cancel: PARAM_INVALIDATED (сигнал исчез)
4. ELSE → Reset max_z = new_|Z| (предотвращает "ложный откат")
```

**Зачем Reset max_z?** При изменении spread_std текущий Z-score меняется. Если std вырос — Z упал. Без сброса max_z бот увидит "откат" которого не было, и войдёт ложно. Reset гарантирует, что pullback измеряется от **нового** пика после пересчёта параметров.

---

## 4. Trading Service

### При входе сохраняет "frozen" параметры:

- `entry_beta` — beta на момент входа
- `spread_mean` — среднее спреда
- `spread_std` — стандартное отклонение спреда
- `z_tp_threshold`, `z_sl_threshold`

Эти параметры используются ExitObserver для расчёта Z-score.

### Dynamic Position Sizing (Half-Life Based):

Размер позиции рассчитывается динамически на основе Half-Life спреда:

```
Size = BaseSize × (TargetHalfLife / CurrentHalfLife)
```

| Half-Life (bars) | Multiplier | Описание |
| --- | --- | --- |
| 6 bars (1.5h) | 2.0x | Быстрая ревёрсия → большая позиция |
| 12 bars (3h) | 1.0x | Эталон (TARGET_HALFLIFE_BARS) |
| 24 bars (6h) | 0.5x | Медленная ревёрсия → маленькая позиция |

**Лимиты:** MIN_SIZE_MULTIPLIER (0.5x) – MAX_SIZE_MULTIPLIER (2.0x)

**Логика:** Чем быстрее спред возвращается к среднему, тем больше можно рисковать.

### Atomic Execution:

```
Parallel:
    Open COIN position
    Open PRIMARY position (hedge)

IF both succeeded → TradeOpenedEvent
ELSE → Rollback
```

---

## 5. Exit Observer (Real-Time TP/SL)

**Главный компонент для выходов по TP/SL.** Работает в реальном времени через WebSocket.

### Ключевая особенность: Frozen Parameters

ExitObserver использует **параметры, сохранённые при входе**:

```python
# Расчёт Z-score с frozen параметрами:
current_spread = log(coin_price) - frozen_beta × log(primary_price)
z_score = (current_spread - frozen_spread_mean) / frozen_spread_std
```

Это гарантирует, что TP/SL рассчитываются относительно **того же риск-профиля**, что был при входе.

### Dynamic TP (Time-Based):

Чем дольше позиция открыта, тем легче достичь TP:

| Время в позиции | Коэффициент | Пример (entry Z=3.0) |
| --------------- | ----------- | -------------------- |
| 0-4 часа        | 1×          | TP при Z ≤ 0.3       |
| 4-12 часов      | 3×          | TP при Z ≤ 0.9       |
| 12-24 часа      | 5×          | TP при Z ≤ 1.5       |
| 24+ часов       | 8×          | TP при Z ≤ 2.4       |

### Exit Conditions:

```
IF |Z| <= dynamic_TP → ExitSignalEvent(TAKE_PROFIT)
IF |Z| >= z_sl_threshold → ExitSignalEvent(STOP_LOSS)
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
│  2. Check STRUCTURAL Exits (open positions + watches):                      │
│     • CORRELATION_DROP (corr < 0.8)                                         │
│     • HURST_TRENDING (H >= 0.45)                                            │
│     • TIMEOUT (bars >= 96)                                                  │
│     [NO TP/SL here - that's ExitObserver's job]                             │
│                                                                             │
│  3. Check Entry Conditions ──► PendingEntrySignalEvent                      │
│                                                                             │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
    ┌───────────────────────────────┐   ┌───────────────────────────────────┐
    │    EntryObserverService       │   │  (For existing watches)           │
    │    (WebSocket trailing entry) │   │  Re-Validate & Reset:             │
    │                               │   │  • Update beta, spread_std        │
    │  New watch:                   │   │  • Recalc Z with new params       │
    │  • Subscribe to COIN + ETH    │   │  • IF |Z| < threshold → Cancel    │
    │  • Track max_z                │   │  • ELSE → Reset max_z             │
    │  • Wait for pullback          │   │                                   │
    │                               │   └───────────────────────────────────┘
    │  Exit conditions:             │
    │  • Timeout → Cancel           │
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
    │                   (WebSocket TP/SL monitoring)                        │
    │                                                                       │
    │  Uses FROZEN parameters from entry:                                   │
    │  • frozen_beta, frozen_spread_mean, frozen_spread_std                 │
    │                                                                       │
    │  Dynamic TP based on time in position:                                │
    │  • 0-4h: 1× coefficient                                               │
    │  • 4-12h: 3× coefficient                                              │
    │  • 12-24h: 5× coefficient                                             │
    │  • 24h+: 8× coefficient                                               │
    │                                                                       │
    │  Exit conditions:                                                     │
    │  • |Z| <= dynamic_TP → TAKE_PROFIT                                    │
    │  • |Z| >= z_sl_threshold → STOP_LOSS                                  │
    │                                                                       │
    └───────────────────────────────────┬───────────────────────────────────┘
                                        │
                                        ▼
                          ┌───────────────────────────────┐
                          │       ExitSignalEvent         │
                          │   (TP/SL or Structural)       │
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

## Сводка параметров

| Параметр                       | Значение  | Описание                                     |
| ------------------------------ | --------- | -------------------------------------------- |
| TIMEFRAME                      | 15m       | Таймфрейм свечей                             |
| LOOKBACK_WINDOW_DAYS           | 3         | Окно для rolling расчётов                    |
| MIN_CORRELATION                | 0.8       | Минимальная корреляция                       |
| MIN_BETA / MAX_BETA            | 0.5 / 2.0 | Диапазон допустимых бет                      |
| Z_ENTRY_THRESHOLD              | 2.0       | Базовый порог входа (dynamic override)       |
| Z_TP_THRESHOLD                 | 0.25      | Базовый порог TP                             |
| Z_SL_THRESHOLD                 | 4.0       | Порог SL                                     |
| ADAPTIVE_PERCENTILE            | 95        | Перцентиль для dynamic threshold             |
| DYNAMIC_THRESHOLD_WINDOW_BARS  | 440       | Окно для dynamic threshold                   |
| THRESHOLD_EMA_ALPHA            | 0.1       | Сглаживание dynamic threshold                |
| HURST_THRESHOLD                | 0.45      | Порог Hurst (вход)                           |
| HURST_WATCH_TOLERANCE          | 0         | Tolerance для watches/позиций                |
| HALFLIFE_MAX_BARS              | 48        | Макс. Half-Life (~12h)                       |
| ADF_PVALUE_THRESHOLD           | 0.08      | Макс. p-value для стационарности             |
| MAX_FUNDING_COST_THRESHOLD     | -0.0005   | Макс. расход на фандинг (-0.05% за 8h)       |
| TRAILING_ENTRY_PULLBACK        | 0.2       | Откат Z для подтверждения разворота          |
| TRAILING_ENTRY_TIMEOUT_MINUTES | 45        | Таймаут trailing entry                       |
| FALSE_ALARM_HYSTERESIS         | 0.2       | Гистерезис для отмены watch                  |
| POSITION_SIZE_USDT             | 100       | Размер позиции COIN leg                      |
| MAX_OPEN_SPREADS               | 5         | Максимум спредов                             |
| COOLDOWN_BARS                  | 16        | Cooldown после неудачного выхода (~4h)       |
| MAX_POSITION_BARS              | 96        | Макс. длительность позиции (~24h)            |
| EXCHANGE_DEFAULT_LEVERAGE      | 5         | Плечо                                        |
