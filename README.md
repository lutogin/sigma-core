# Sigma Core

Статистический арбитраж для Binance USDT-M Futures внутри ETH-экосистемы:
каждая монета торгуется как спред против `ETH/USDT:USDT`.

Проект остаётся исследовательской торговой системой, а не гарантией доходности.
По умолчанию реальная торговля выключена (`ALLOW_TRADING=false`). Включать её
имеет смысл только после актуального walk-forward, shadow-проверки и сверки
реальных fill/fee/funding с моделью.

## Торговая модель

Для пары COIN/ETH модель строится на логарифмах цен:

```text
log(COIN) = alpha + beta * log(ETH) + residual
spread    = log(COIN) - beta * log(ETH)
z         = (spread - rolling_mean) / rolling_std
```

`beta`, среднее и стандартное отклонение оцениваются rolling OLS только по уже
закрытым свечам. Корреляция считается по доходностям, но не используется вместо
коинтеграционной модели. При входе параметры спреда замораживаются, поэтому TP и
SL не двигаются вслед за повторной оценкой модели.

Новый вход требует одновременно:

- корреляцию доходностей не ниже `0.80`;
- beta в диапазоне `[0.5, 2.0]`;
- устойчивость beta и прохождение минимум двух из окон `3/6/9` дней;
- Hurst ниже `0.45`, half-life не более `48` баров;
- ADF после Benjamini–Hochberg FDR с базовым порогом `0.05`;
- безопасную волатильность ETH и доступный funding;
- `|z|` выше причинного adaptive threshold, но не выше `5.0`;
- подтверждение разворота через trailing entry.

Сканер работает раз в 15 минут. Entry/Exit observers используют WebSocket и
frozen-параметры для внутрисвечного подтверждения входа и TP/SL.

## Риск и исполнение

Сбалансированные fail-closed defaults:

| Параметр | Значение |
| --- | ---: |
| Leverage | `5x`, cross |
| Базовый notional COIN-ноги | `1000 USDT` |
| Максимум COIN-ноги от equity | `10%` |
| Максимальное использование margin | `50%` |
| Максимум одновременно открытых спредов | `3` |
| Максимальный size multiplier | `1.25` |
| Максимальное время позиции | `96 × 15m ≈ 24h` |
| Trailing-entry timeout | `90 min` |
| Volatility threshold | `1.2%` |

Перед live-запуском бот сверяет hedge mode, биржевые позиции и сохранённое
состояние. Новые входы блокируются при расхождении экспозиции. `ALLOW_TRADING`
останавливает только новые входы: закрытия и аварийное снижение риска остаются
доступны.

Входы отправляются IOC limit-ордерами с client order ID. Код восстанавливает
результат после неоднозначного сетевого ответа, агрегирует partial fills,
сохраняет фактические количества и среднюю цену. Если одна нога не набрана,
откатывается только реально исполненное количество второй ноги. Повторное
закрытие пропускает уже закрытую ногу.

## Реалистичность бэктеста

Актуальный движок — `backtests/run_backtest.py`; `run_backtest_legacy.py`
оставлен только для истории.

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

Это всё ещё приближение. OHLCV не воспроизводит очередь в стакане, latency,
market impact, outages, liquidation/ADL и точный bid/ask каждого исторического
тика. Фильтр по текущему 24h volume также вносит survivorship bias в старые
окна. Поэтому положительный backtest — необходимое, но не достаточное условие
для live.

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
60%.

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
становится healthy только после успешного scanner cycle и становится stale,
если успешных циклов нет 30 минут. CI компилирует исходники, запускает полный
набор тестов и только затем собирает image. Production deployment использует
точный image digest, ждёт healthy и при ошибке возвращает предыдущий image.

## Основные каталоги

```text
main.py                         entry point
src/domain/screener/            causal pair model и фильтры
src/domain/entry_observer/      trailing entry
src/domain/exit_observer/       frozen-parameter TP/SL/timeout
src/domain/trading/             двухногая execution state machine
src/domain/trading_pairs/       immutable pair versions и active pointer
backtests/run_backtest.py       основной симулятор
backtests/run_universe_walk_forward.py
scripts/refresh_trading_pairs.py
tests/                          regression/safety tests
```
