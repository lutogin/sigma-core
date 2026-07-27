import pytest

from backtests.entry_funnel import (
    STAGES,
    EntryFunnel,
    format_funnel_report,
    format_symbol_funnel_table,
    summarize_totals,
    top_blockers,
)


def _funnel_with_two_coins() -> EntryFunnel:
    funnel = EntryFunnel()
    for _ in range(100):
        funnel.count_bar()
        funnel.record("AAVE/USDT:USDT", "evaluated")
        funnel.record("LINK/USDT:USDT", "evaluated")

    funnel.record("AAVE/USDT:USDT", "reject_stability", 90)
    funnel.record("AAVE/USDT:USDT", "reject_z_below_threshold", 10)

    funnel.record("LINK/USDT:USDT", "reject_z_below_threshold", 95)
    funnel.record("LINK/USDT:USDT", "signal", 5)
    funnel.record("LINK/USDT:USDT", "reject_hurst", 3)
    funnel.record("LINK/USDT:USDT", "watch_started", 2)
    funnel.record("LINK/USDT:USDT", "entered", 1)
    return funnel


def test_unknown_stage_fails_loudly() -> None:
    funnel = EntryFunnel()
    with pytest.raises(ValueError):
        funnel.record("AAVE/USDT:USDT", "reject_typo")


def test_watch_reasons_map_to_stages() -> None:
    funnel = EntryFunnel()
    funnel.record_watch_reason("LINK/USDT:USDT", None)
    funnel.record_watch_reason("LINK/USDT:USDT", "TIMEOUT")
    funnel.record_watch_reason("LINK/USDT:USDT", "NO_MARGIN")
    # Unknown reasons must not silently invent a stage.
    funnel.record_watch_reason("LINK/USDT:USDT", "SOMETHING_ELSE")

    counts = funnel.for_symbol("LINK/USDT:USDT")
    assert counts["watch_entered"] == 1
    assert counts["watch_timeout"] == 1
    assert counts["watch_no_margin"] == 1
    assert sum(counts.values()) == 3


def test_totals_cover_every_declared_stage() -> None:
    totals = _funnel_with_two_coins().totals()
    assert set(totals) == set(STAGES)
    assert totals["evaluated"] == 200
    assert totals["reject_stability"] == 90
    assert totals["signal"] == 5
    assert totals["entered"] == 1


def test_summary_splits_pre_and_post_signal_rejections() -> None:
    summary = summarize_totals(_funnel_with_two_coins().totals())
    assert summary["evaluated"] == 200
    # 90 stability + 10 + 95 below-threshold
    assert summary["pre_signal_rejected"] == 195
    assert summary["signal"] == 5
    assert summary["post_signal_rejected"] == 3
    assert summary["entered"] == 1


def test_top_blockers_ignores_z_below_threshold_by_default() -> None:
    counts = _funnel_with_two_coins().for_symbol("AAVE/USDT:USDT")
    assert top_blockers(counts) == [("reject_stability", 90)]
    assert ("reject_z_below_threshold", 10) in top_blockers(counts, exclude=())


def test_roundtrip_and_merge_accumulate() -> None:
    original = _funnel_with_two_coins()
    restored = EntryFunnel.from_dict(original.to_dict())
    assert restored.totals() == original.totals()
    assert restored.bars_processed == original.bars_processed

    restored.merge(original)
    assert restored.totals()["evaluated"] == 400
    assert restored.bars_processed == 200

    # Merging a serialized payload must behave like merging the object.
    restored.merge(original.to_dict())
    assert restored.totals()["evaluated"] == 600


def test_merge_ignores_missing_payload() -> None:
    funnel = _funnel_with_two_coins()
    before = funnel.totals()
    funnel.merge(None)
    assert funnel.totals() == before


def test_from_dict_drops_unknown_stages() -> None:
    funnel = EntryFunnel.from_dict(
        {
            "bars_processed": 5,
            "per_symbol": {"LINK/USDT:USDT": {"evaluated": 3, "bogus_stage": 7}},
        }
    )
    counts = funnel.for_symbol("LINK/USDT:USDT")
    assert counts == {"evaluated": 3}


def test_reports_render_without_crashing() -> None:
    funnel = _funnel_with_two_coins()

    report = "\n".join(format_funnel_report(funnel))
    assert "evaluated" in report
    assert "reject_stability" in report

    table = "\n".join(format_symbol_funnel_table(funnel))
    # Coins that never entered are listed first so blockers are obvious.
    assert table.index("AAVE") < table.index("LINK")
    assert "stability=90" in table


def test_empty_funnel_reports_are_safe() -> None:
    funnel = EntryFunnel()
    assert "no symbol observations recorded" in "\n".join(
        format_funnel_report(funnel)
    )
    assert format_symbol_funnel_table(funnel) == []
