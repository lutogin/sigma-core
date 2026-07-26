# Universe walk-forward и обновление live-пар

`run_universe_walk_forward.py` отвечает на два разных вопроса:

1. Какие монеты стабильно проходили train-отбор и зарабатывали в следующих,
   ещё не виденных OOS-окнах?
2. Какие монеты прошли тот же train-отбор на окне, заканчивающемся сейчас, и
   поэтому являются кандидатами на следующий live-период?

Каждая монета намеренно тестируется на отдельном счёте с одинаковым стартовым
балансом. Это исключает зависимость результата одной монеты от порядка сигналов
других монет и позволяет честно ранжировать инструменты. Суммарный PnL в отчёте
нельзя трактовать как equity curve одного общего live-счёта.

## Каузальная схема

Для каждого шага:

1. Все монеты тестируются на train-окне.
2. Отбор использует только результаты этого train-окна.
3. Выбранные монеты тестируются на следующем OOS trade-окне.
4. У каждой монеты применяется собственный online kill-switch.

После всех OOS-шагов выполняется дополнительный train-only прогон на последних
`trainDays`, заканчивающихся ровно в `end`. Он записывается в `live_selection`.
Именно этот список задаёт порядок текущих кандидатов. Исторический OOS только
подтверждает или отклоняет эти кандидатуры — он не используется для повторного
поиска победителей.

Бэктест загружает реальные historical funding events для ETH и всех монет,
использует taker fee, half-spread и adverse slippage на каждой ноге и каждом
fill. Если funding-фильтр включён, но funding history для момента входа
отсутствует, вход блокируется.

## Рекомендуемый production-прогон

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

`refresh_trading_pairs.py` использует эту production-конфигурацию, включая
минимальный текущий 24h volume `10,000,000 USDT`. У прямого CLI
`run_universe_walk_forward.py` volume-фильтр по умолчанию выключен (`0`), чтобы
исследователь случайно не принял фильтрацию по сегодняшней ликвидности за
исторически доступную информацию. Отрицательный и sparse fallback по умолчанию
выключены в обоих путях.

Основные параметры CLI:

- `--trainDays 60` — длина train-окна.
- `--tradeDays 14` — длина следующего OOS-окна и шаг.
- `--topK 5` — максимум монет в одном отборе.
- `--minTradesTrain 3` — минимум сделок для строгого train-отбора.
- `--rankMetric netPnL|netSharpe` — метрика ранжирования с reliability shrinkage.
- `--killLossStreak 3` — остановить независимый счёт после серии убытков.
- `--killNegativeR 1.0` — остановить счёт при PnL ниже `-1R`.
- `--allow-sparse-train-selection true` — явный opt-in для кандидатов с одной
  прибыльной сделкой; для production не рекомендуется.
- `--allow-negative-train-selection true` — явный opt-in для убыточных
  кандидатов; для production не рекомендуется.

Текущий volume-фильтр полезен для следующего live-периода, но создаёт
survivorship bias в старых OOS-окнах: историческая ликвидность и delisted
инструменты им не восстанавливаются. Это ограничение нужно учитывать при
интерпретации результата.

Результат сохраняется в
`backtests/results/universe_wf_<start>_<end>.json`.

## Безопасная публикация

Сначала можно создать immutable candidate без переключения live:

```bash
.venv/bin/python scripts/update_trading_pairs_from_wf.py \
  backtests/results/universe_wf_2026-01-21_2026-07-26.json
```

Активация выполняется только при прохождении всех fail-closed gates:

- результат не старше 21 дня;
- минимум 6 завершённых OOS-окон;
- не менее 50% OOS-окон прибыльны;
- суммарный независимый OOS PnL положительный;
- монета присутствует в текущем `live_selection`;
- у монеты положительный OOS PnL, минимум 2 OOS-сделки, selection rate не ниже
  20% и ни одного срабатывания kill-switch;
- прошло минимум 2 и максимум 5 монет;
- Jaccard turnover относительно предыдущей version не выше 60%.
- execution model включает funding, trailing/live exits, taker commission и не
  менее 1 bps half-spread + 0.5 bps slippage.

```bash
.venv/bin/python scripts/update_trading_pairs_from_wf.py \
  backtests/results/universe_wf_2026-01-21_2026-07-26.json \
  --activate
```

Посмотреть историю и откатить active pointer:

```bash
.venv/bin/python scripts/update_trading_pairs_from_wf.py --list
.venv/bin/python scripts/update_trading_pairs_from_wf.py --rollback
```

Live screener перечитывает active version из MongoDB на каждом 15-минутном
скане. Пара, удалённая новой version, остаётся в monitoring universe до
закрытия уже открытой позиции, но не создаёт повторный вход.

## Периодический запуск

Wrapper сам рассчитывает trailing-период, не допускает параллельные запуски и
после успешного WF применяет policy:

```bash
.venv/bin/python scripts/refresh_trading_pairs.py --activate
```

Рекомендуемая частота — раз в неделю. Ежедневный запуск создаёт почти одинаковые
выборки, повышает turnover и не добавляет достаточно новой OOS-информации.
Пример cron по понедельникам в 03:20 UTC:

```cron
20 3 * * 1 cd /absolute/path/to/sigma-core && .venv/bin/python scripts/refresh_trading_pairs.py --activate >> logs/pair-refresh.log 2>&1
```

Без `--activate` job работает в shadow-режиме: сохраняет прошедшую candidate
version, но не переключает live.
