# Sigma Core

Статистический арбитраж для Binance USDT-M Futures внутри ETH-экосистемы:
каждая монета торгуется как спред против `ETH/USDT:USDT`.

Проект остаётся исследовательской торговой системой, а не гарантией доходности.
По умолчанию реальная торговля выключена (`ALLOW_TRADING=false`). Включать её
имеет смысл только после актуального walk-forward, shadow-проверки и сверки
реальных fill/fee/funding с моделью.

> Все числовые значения ниже соответствуют рабочему `.env`. Код-дефолты в
> `src/config/settings.py` и шаблон `.env.example` местами отстают от рабочей
> конфигурации — источником правды считайте `.env` / `.env.prod`, а расхождения
> смотрите в разделе [Дрейф конфигурации](#дрейф-конфигурации).

## Торговая модель

Для пары COIN/ETH модель строится на логарифмах цен. Одна rolling OLS-регрессия
даёт сразу hedge ratio, центр спреда и его масштаб:

```text
log(COIN) = intercept + beta * log(ETH) + residual
spread    = log(COIN) - beta * log(ETH)
z         = (spread - intercept) / residual_std
```

Важно: центр z-score — это **intercept регрессии**, а знаменатель —
**стандартная ошибка остатков**, а не скользящие mean/std ряда спреда. Live
observers замораживают именно эти три величины (`beta`, `spread_mean`,
`spread_std`), поэтому бэктест обязан использовать то же представление.

`beta`, intercept и residual std оцениваются rolling OLS только по уже закрытым
свечам; окно — `LOOKBACK_WINDOW_DAYS = 3` дня (288 баров на 15m). Корреляция
считается по доходностям и остаётся отдельным regime-фильтром, а не заменой
коинтеграционной модели.

Новый вход требует одновременно:

- корреляцию доходностей не ниже `0.80`;
- beta в диапазоне `[0.5, 2.0]`;
- beta drift guard: медианы beta за `3` и `9` дней расходятся не более чем на
  `35%`;
- multi-window stability: минимум `2` из окон `3/6/9` дней, где каждое окно
  отдельно проверяет beta-диапазон, half-life и сырой ADF (внутри окон
  BH-коррекция не применяется);
- Hurst ниже `0.45`, half-life не более `48` баров;
- ADF после Benjamini–Hochberg FDR с базовым порогом `0.05` — коррекция
  считается по всему набору кандидатов текущего скана;
- безопасную волатильность ETH и доступный funding;
- `|z|` выше причинного adaptive threshold, но ниже `Z_EXTREME_LEVEL = 6.0`;
- отсутствие cooldown, открытой позиции и активного watch по этому символу;
- подтверждение разворота через trailing entry.

Adaptive threshold — это `max(Z_ENTRY_THRESHOLD, перцентиль 95 от |z| за 440
баров)`, причём текущий бар исключается из истории перцентиля. В спокойном
режиме работает пол `2.1`, в шумном порог поднимает сам себя, и входов
становится заметно меньше.

Сканер работает раз в 15 минут. Entry/Exit observers используют WebSocket
book ticker (mid-price) и frozen-параметры для внутрисвечного подтверждения
входа и TP/SL.

### Что двигается после входа

Замораживаются `beta`, `spread_mean` и `spread_std` — пересчёт модели не
сдвигает уже открытую позицию. Но сами уровни TP/SL не статичны:

| Механизм | Эффект |
| --- | --- |
| Trailing SL | SL подтягивается по мере улучшения `\|Z\|` |
| Extended SL | при входе с `\|entry_z\| > Z_SL_THRESHOLD` стоп раздвигается до `\|entry_z\| + Z_SL_EXTREME_OFFSET` |
| Dynamic TP при входе | `z_tp = max(z_tp_threshold, \|current_z\| * 0.1)` |
| Time-scaled TP | эффективный TP умножается на коэффициент `1/3/5/8` по возрасту позиции |

### Выходы

TP/SL и таймаут позиции обрабатывает ExitObserver в реальном времени.
Структурные выходы считаются на 15-минутном скане в orchestrator:

- `CORRELATION_DROP` при корреляции ниже `0.75`;
- `HURST_TRENDING` после `2` подтверждений подряд с порогом `0.47`;
- `ADF_NON_STATIONARY` и деградация half-life — тоже по `2` подтверждения.

Для watch-кандидатов пороги мягче, чем для входа: watch снимается при
корреляции ниже `0.77`. Отмена watch по ADF и half-life в orchestrator сейчас
намеренно отключена (логируется, но watch не снимается).

## Риск и исполнение

Актуальные значения рабочей конфигурации:

| Параметр | Значение | Переменная |
| --- | ---: | --- |
| Leverage | `10x`, cross | `EXCHANGE_DEFAULT_LEVERAGE` |
| Базовый notional COIN-ноги | `10000 USDT` | `POSITION_SIZE_USDT` |
| Максимум COIN-ноги от equity | `52.5%` | `MAX_COIN_NOTIONAL_PCT` |
| Максимальное использование margin | `50%` | `MAX_MARGIN_UTILIZATION` |
| Максимум одновременно открытых спредов | `3` | `MAX_OPEN_SPREADS` |
| Size multiplier по half-life | `0.5 … 2.1` | `MIN/MAX_SIZE_MULTIPLIER` |
| Максимальное время позиции | `192 × 15m ≈ 48h` | `MAX_POSITION_BARS` |
| Cooldown после неудачного выхода | `16 × 15m = 4h` | `COOLDOWN_BARS` |
| Trailing-entry timeout | `90 min` | `TRAILING_ENTRY_TIMEOUT_MINUTES` |
| Volatility threshold | `1.8%` | `VOLATILITY_THRESHOLD` |
| ETH flash-move threshold | `5%` за `4h` | `VOLATILITY_CRASH_THRESHOLD` |
| Порог токсичного funding | `-0.10% / 8h` | `MAX_FUNDING_COST_THRESHOLD` |

`MAX_COIN_NOTIONAL_PCT` — это финальный потолок COIN-ноги **после** half-life
множителя, а не базовый размер позиции.

Cooldown включается после SL, correlation drop, таймаута позиции, Hurst
trending и отмены watch.

### Fail-open и fail-closed

Важная асимметрия: часть защит при сбое пропускает торговлю, а не блокирует её.

- Volatility-фильтр при нехватке данных или ошибке загрузки считает рынок
  безопасным.
- Funding-фильтр при ошибке API возвращает «безопасно», а в batch-режиме
  подставляет ставку `0.0`.
- Наоборот, **в бэктесте** отсутствие историй funding на момент входа вход
  блокирует.
- Небезопасная волатильность отменяет все watch и прерывает скан целиком —
  в этом цикле структурные выходы не проверяются.

### Проверки перед торговлей

Перед live-запуском бот требует hedge mode (`dualSidePosition=true`), сверяет
биржевые позиции с сохранённым состоянием и блокирует новые входы при
расхождении экспозиции. Два уточнения:

- hedge mode только **проверяется**, но не включается автоматически;
- при `ALLOW_TRADING=false` и отсутствии открытых позиций сверка на старте
  пропускается и выполняется при первом `enable_trading()`.

`ALLOW_TRADING` останавливает только новые входы: закрытия, таймауты и
аварийное снижение риска остаются доступны. Неудачный откат ноги или исключение
при закрытии переводят сервис в fail-closed: `ALLOW_TRADING` и
`execution_ready` сбрасываются автоматически.

### Ордера

Входы отправляются IOC limit-ордерами с client order ID, до `5` попыток и
**без** fallback в market. Код восстанавливает результат после неоднозначного
сетевого ответа по `origClientOrderId`, агрегирует partial fills, сохраняет
фактические количества и среднюю цену. Если одна нога не набрана,
откатывается только реально исполненное количество второй ноги. Повторное
закрытие пропускает уже закрытую ногу.

Закрытия, в отличие от входов, идут **market**-ордерами.

### Известное расхождение: HurstFilterService

`HurstFilterService` в live-контейнере создаётся без аргументов
(`src/infra/container.py`), поэтому `HURST_THRESHOLD` и
`HURST_LOOKBACK_CANDLES` из окружения в него **не попадают** — используются
хардкод-дефолты сервиса `0.45` и `300`. Бэктест через
`backtests/backtest_shared.py` читает эти же настройки честно и передаёт их в
сервис.

Сейчас расхождения нет, потому что значения в `.env` совпадают с дефолтами. Но
как только Hurst начнут тюнить через окружение, бэктест и прод молча разойдутся.
До исправления wiring меняйте Hurst только правкой дефолтов в
`src/domain/screener/hurst_filter/hurst_filter.py`.

### Дрейф конфигурации

`.env.example` и код-дефолты отстают от рабочей конфигурации:

| Переменная | `.env` | `.env.prod` | `.env.example` |
| --- | ---: | ---: | ---: |
| `EXCHANGE_DEFAULT_LEVERAGE` | 10 | 10 | 5 |
| `POSITION_SIZE_USDT` | 10000 | 1000 | 1000 |
| `MAX_POSITION_BARS` | 192 | 192 | 96 |
| `VOLATILITY_THRESHOLD` | 0.018 | 0.018 | 0.012 |
| `MAX_SIZE_MULTIPLIER` | 2.1 | 2.1 | 1.25 |
| `Z_EXTREME_LEVEL` | 6 | 6 | 5 |
| `MAX_FUNDING_COST_THRESHOLD` | -0.0010 | -0.0010 | -0.0005 |
| `TRAILING_ENTRY_PULLBACK` | 0.25 | 0.07 | 0.2 |

`TRAILING_ENTRY_PULLBACK` расходится во всех трёх файлах, и это не косметика.
`0.07` — наследие старого масштабирования pullback на `sqrt(1m/15m)`, от
которого отказались: `compute_trailing_pullback_calibration` теперь возвращает
значение без изменений, потому что более частая выборка меняет момент
наблюдения разворота, а не требуемое расстояние от пика Z. Прогон бэктеста с
`.env` (0.25) и live с `.env.prod` (0.07) — это разные стратегии входа.

Неиспользуемая переменная: `CORRELATION_THRESHOLD` объявлена в настройках, но
нигде не читается. Живой гейт входа — `MIN_CORRELATION`.

## Реалистичность бэктеста

Актуальный движок — `backtests/run_backtest.py`.

Модель бэктеста:

- принимает решения только после закрытия 15m-свечи;
- для trailing entry и live exit использует синхронизированные закрытые
  1m-свечи, без доступа к будущему high/low;
- на каждом fill каждой ноги списывает taker commission;
- ухудшает цену на `half-spread + slippage`;
- начисляет реальные historical funding events один раз в их timestamp;
- считает PnL по фактическому notional, не умножая его повторно на leverage;
- не заполняет отсутствующие данные назад и не торгует до листинга;
- выравнивает COIN и ETH попарно, не требуя общей временной точки всего universe;
- считает Sharpe по дневным доходностям с annualization `sqrt(365)`.

Default execution assumptions для universe walk-forward: `2 bps` half-spread и
`1 bp` adverse slippage на fill сверх taker fee.

Математику фильтров бэктест не переписывает: `backtests/backtest_shared.py`
собирает те же самые сервисы из `src/domain/screener/`, что использует live.
Переписана только оркестрация — цикл скана, порядок обработки кандидатов и
распределение слотов между монетами.

Оставшиеся приближения:

- OHLCV не воспроизводит очередь в стакане, latency, market impact, outages,
  liquidation/ADL и точный bid/ask каждого исторического тика;
- fill всегда полный и мгновенный по модельной цене, partial fills и откаты
  ноги не моделируются;
- при отсутствии 1m-данных trailing entry падает обратно на 15m close;
- кандидаты сортируются по `|Z|` и занимают слоты сверху вниз, тогда как live
  обрабатывает события в порядке их появления;
- с `--no-funding-filter` funding PnL не начисляется вовсе, а не только
  отключается фильтр входа;
- фильтр по текущему 24h volume вносит survivorship bias в старые окна.

Поэтому положительный backtest — необходимое, но не достаточное условие для live.

## Диагностика воронки входа

Пустой результат бэктеста сам по себе не говорит, какой фильтр съел сигнал.
`backtests/entry_funnel.py` считает каждую точку отсева по всему конвейеру
входа, и оба раннера печатают отчёт автоматически.

`run_backtest.py` в конце прогона выводит две таблицы: сводную воронку и
разбивку по монетам.

```text
  stage                              count   % evaluated  % of signals
  → evaluated                       30,724       100.00%
    reject_correlation              16,691        54.33%
    reject_beta_drift                1,888         6.15%
    reject_stability                   837         2.72%
    reject_z_below_threshold        10,417        33.91%
  → signal                              21         0.07%
    reject_hurst                         8         0.03%         38.1%
    watch_started                       12         0.04%
  → entered                             12         0.04%
```

`run_universe_walk_forward.py` агрегирует те же счётчики по всем train-окнам
(там прогоняется весь universe) и отдельно по OOS-окнам, печатает строку на
монету и сохраняет всё в секцию `entry_funnel` результирующего JSON:

```json
{
  "entry_funnel": {
    "train": {
      "summary": { "evaluated": 0, "signal": 0, "entered": 0 },
      "stages": {},
      "by_coin": [{ "coin": "LINK", "signals": 0, "top_blockers": [] }]
    },
    "oos": {}
  }
}
```

Как читать:

- `evaluated` — сколько раз символ вообще оценивался на закрытой свече;
- `signal` — сколько раз он прошёл все бар-фильтры и попал в диапазон входа;
- `reject_z_below_threshold` по построению срабатывает почти всегда и в
  `top_blockers` исключён — смотрите на фильтры, которые селективны;
- если `watch_started` близко к `entered`, trailing entry не является узким
  местом, и ослаблять pullback бессмысленно.

Счётчики нужны именно для отбора монет: они показывают, монета не торгуется
из-за низкой корреляции с ETH, из-за нестабильной беты или потому что сигналы
есть, но их гасит Hurst/ADF.

## Каталог backtests

| Скрипт | Назначение | Статус |
| --- | --- | --- |
| `run_universe_walk_forward.py` | Train/OOS walk-forward с отбором topK, reliability shrinkage, kill-switch и `live_selection`. Главный инструмент выбора монет для прода. | Продовый путь |
| `run_backtest.py` | Основной симулятор: одно окно дат, портфель монет с общим балансом. | Актуальный движок |
| `backtest_shared.py`, `execution_model.py`, `entry_funnel.py` | Общая сборка конфига и сервисов, модель исполнения, счётчики воронки. | Ядро |
| `run_coin_walk_forward.py` | Параллельно гоняет каждую монету из списка помесячно и ранжирует за весь период. Отбора train/OOS нет, ранжирование по всей истории — риск переобучения. | Разведочный |
| `run_walk_forward_backtest.py` | Помесячные срезы одной монеты. Используется как subprocess-воркер для `run_coin_walk_forward.py`. | Служебный |

Предупреждение по `run_walk_forward_backtest.py`: без `--coin` он передаёт
пустой список пар и fallback на `CONSISTENT_PAIRS` не делает. Запуск отработает
без ошибки и покажет ноль сделок просто потому, что торговать было нечем.

Удалённый `run_backtest_legacy.py` умножал PnL каждой ноги на leverage,
применял `ffill`/`bfill` к пропускам (то есть торговал до листинга), брал
maker-fee вместо taker и считал Sharpe с `sqrt(365*96)`. Если встретите его
старые результаты — они завышены кратно, доверять им нельзя.

## Universe walk-forward

Главный тест выбора монет:

```bash
.venv/bin/python backtests/run_universe_walk_forward.py \
  --start 2026-01-21 \
  --end 2026-07-26 \
  --trainDays 60 \
  --tradeDays 14 \
  --topK 5 \
  --minTradesTrain 3 \
  --rankMetric netPnL \
  --workers 6 \
  --coinsFile backtests/eth_ecosystem_universe.json \
  --min-universe-volume-usdt 10000000 \
  --half-spread-bps 2 \
  --slippage-bps 1
```

Каждая монета намеренно тестируется как независимый счёт с одинаковым стартовым
балансом. Это правильно для ранжирования монет: результат одной монеты не зависит
от порядка сигналов другой. Поэтому `total_portfolio_pnl` нельзя интерпретировать
как equity curve общего live-счёта.

На каждом шаге используются `60` дней train и следующие `14` дней OOS. В отбор
попадают максимум пять монет, минимум с тремя train-сделками и положительным
результатом после reliability shrinkage. Sparse и negative fallback выключены.
Online kill-switch отключает отдельную монету после трёх убытков подряд или
убытка ниже `-1R`.

После исторических OOS-шагов отдельное trailing train-окно, заканчивающееся
ровно в `--end`, создаёт `live_selection`. OOS-история только квалифицирует эти
текущие кандидаты, но не ищет победителей задним числом.

Если `live_selection` пуст или OOS-сделок единицы — сначала смотрите секцию
`entry_funnel`, а не крутите параметры отбора: скорее всего проблема выше по
конвейеру, на фильтрах входа.

Подробности: [backtests/run_universe_walk_forward.md](backtests/run_universe_walk_forward.md).

## Постоянное обновление торговых пар

Еженедельный shadow-прогон из source checkout:

```bash
.venv/bin/python scripts/refresh_trading_pairs.py
```

С активацией прошедшей все policy gates версии:

```bash
.venv/bin/python scripts/refresh_trading_pairs.py --activate
```

Production image содержит тот же backtest path. Compose maintenance-профиль
запускает one-shot refresh с активацией:

```bash
docker compose --profile maintenance run --rm pair-refresh
```

Wrapper берёт trailing `186` дней, не допускает параллельные запуски и сохраняет
immutable version в MongoDB. Активация требует как минимум шесть OOS-шагов,
положительный aggregate OOS PnL, не менее 50% прибыльных шагов, достаточную
историю каждой текущей монеты, реалистичные execution costs и turnover не выше
60%. Пустой `live_selection` не пройдёт gate «минимум две монеты», и активация
корректно не состоится.

Рекомендуемый cron — раз в неделю, например в понедельник 03:20 UTC:

```cron
20 3 * * 1 cd /absolute/path/to/deployment && docker compose --profile maintenance run --rm pair-refresh >> logs/pair-refresh.log 2>&1
```

История и rollback active pointer:

```bash
.venv/bin/python scripts/update_trading_pairs_from_wf.py --list
.venv/bin/python scripts/update_trading_pairs_from_wf.py --rollback
```

Live screener перечитывает active version на каждом скане. Удалённая из новой
версии монета остаётся под наблюдением до закрытия уже существующей позиции, но
новый вход по ней запрещён.

## Запуск и проверка

Нужен Python 3.13, MongoDB и TimescaleDB:

```bash
python3.13 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
docker compose up -d
.venv/bin/python -m pytest -q tests
.venv/bin/python main.py
```

Скопируйте `.env.example` в `.env`, заполните подключения и ключи. Сначала
оставьте:

```dotenv
ALLOW_TRADING=false
EXCHANGE_TESTNET=true
```

Не коммитьте `.env`, `.env.prod` и API secrets.

Docker image запускается от непривилегированного пользователя. Healthcheck
становится healthy после первого успешного scanner cycle и становится stale,
если успешных циклов нет 30 минут. «Успешный» здесь означает, что job не бросил
исключение — цикл, прерванный volatility-фильтром, тоже считается успешным.
CI компилирует исходники, запускает полный набор тестов и только затем собирает
image. Production deployment использует точный image digest, ждёт healthy и при
ошибке возвращает предыдущий image.

## Основные каталоги

```text
main.py                         entry point
src/domain/screener/            causal pair model и фильтры
src/domain/entry_observer/      trailing entry
src/domain/exit_observer/       frozen-parameter TP/SL/timeout
src/domain/trading/             двухногая execution state machine
src/domain/trading_pairs/       immutable pair versions и active pointer
backtests/run_backtest.py       основной симулятор
backtests/entry_funnel.py       счётчики отсева по фильтрам входа
backtests/run_universe_walk_forward.py
scripts/refresh_trading_pairs.py
tests/                          regression/safety tests
```
