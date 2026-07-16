# tests/test_ai_services.py
"""
Unit-тесты для чистых расчётных функций ai_services.py.
Запуск: venv\\Scripts\\pytest tests\ -v
"""
import sys
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Добавляем src/ в путь чтобы импорт работал без установки пакета
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Патчим load_config ДО импорта ai_services — иначе он упадёт если нет config.json
with patch("builtins.open", side_effect=lambda p, *a, **kw: open(p, *a, **kw) if "config.json" not in str(p) else __import__("io").StringIO('{"ai":{"lead_time_days":14,"safety_stock_multiplier":1.65,"temperature":0.3,"model_forecast":"test","forecast_batch_size":5},"crawler":{"retry_count":3}}')):
    pass

# Более чистый способ — мокаем load_config и CONFIG напрямую
import unittest.mock as _mock
_config_mock = {
    "ai": {
        "lead_time_days": 14,
        "safety_stock_multiplier": 1.65,
        "temperature": 0.3,
        "model_forecast": "test/model",
        "forecast_batch_size": 5,
        "history_days": 30,
        "items_limit": 50,
    },
    "crawler": {"retry_count": 3},
}

with _mock.patch.dict("sys.modules", {}):
    with _mock.patch("json.load", return_value=_config_mock):
        with _mock.patch("builtins.open", _mock.mock_open(read_data="{}")):
            with _mock.patch("pathlib.Path.exists", return_value=True):
                import importlib
                import ai_services as ai


# ══════════════════════════════════════════════════════════════════
# T1–T3: calc_avg_sales
# ══════════════════════════════════════════════════════════════════

class TestCalcAvgSales:

    def test_normal_decreasing_stock(self):
        """T1: Нормальный случай — остатки падают равномерно."""
        # [100, 90, 85, 80] → дельты: 10, 5, 5 за 3 дня
        avg, daily = ai.calc_avg_sales([100, 90, 85, 80])
        assert avg == pytest.approx(20 / 3, rel=1e-2)
        assert daily == [10, 5, 5]

    def test_delivery_in_middle_is_ignored(self):
        """T2: Поставка посередине не искажает средний расход."""
        # [50, 40, 80, 70] → дельты: -10 (продажа), +40 (поставка, игнор), -10 (продажа)
        avg, daily = ai.calc_avg_sales([50, 40, 80, 70])
        assert daily == [10, 10]           # поставка не попала
        assert avg == pytest.approx(20 / 3, rel=1e-2)  # делим на 3 дня

    def test_single_data_point_returns_zero(self):
        """T3: Только одна точка — невозможно считать дельты."""
        avg, daily = ai.calc_avg_sales([100])
        assert avg == 0.0
        assert daily == []

    def test_empty_list_returns_zero(self):
        """Граничный случай: пустой список."""
        avg, daily = ai.calc_avg_sales([])
        assert avg == 0.0
        assert daily == []

    def test_all_deliveries_no_sales(self):
        """Остатки только растут — нет продаж."""
        avg, daily = ai.calc_avg_sales([100, 150, 200])
        assert avg == 0.0
        assert daily == []


# ══════════════════════════════════════════════════════════════════
# T4–T6: calc_recommended_qty
# ══════════════════════════════════════════════════════════════════

class TestCalcRecommendedQty:

    def test_basic_rop_calculation(self):
        """T4: Базовый расчёт без товара в пути."""
        # avg=10, lead=14, safety=15, stock=50, transit=0
        # ROP = 10×14 + 15 = 155; order = 155 - 50 = 105
        qty, rop = ai.calc_recommended_qty(
            avg_sales=10, lead_time=14, safety_stock=15,
            current_qty=50, in_transit=0
        )
        assert rop == 155
        assert qty == 105

    def test_in_transit_reduces_order(self):
        """T5: Товар в пути уменьшает заказ."""
        # ROP = 155; order = max(0, 155 - 50 - 60) = 45
        qty, rop = ai.calc_recommended_qty(
            avg_sales=10, lead_time=14, safety_stock=15,
            current_qty=50, in_transit=60
        )
        assert qty == 45

    def test_sufficient_stock_no_order(self):
        """T6: Достаточный запас — заказ не нужен, результат 0."""
        # avg=5, lead=14, safety=10, ROP=80; stock=200 >> ROP
        qty, _ = ai.calc_recommended_qty(
            avg_sales=5, lead_time=14, safety_stock=10,
            current_qty=200, in_transit=0
        )
        assert qty == 0

    def test_recommended_never_negative(self):
        """Никогда не возвращает отрицательное значение."""
        qty, _ = ai.calc_recommended_qty(
            avg_sales=1, lead_time=14, safety_stock=5,
            current_qty=9999, in_transit=0
        )
        assert qty == 0


# ══════════════════════════════════════════════════════════════════
# T7–T9 + T15: build_diagnosis
# ══════════════════════════════════════════════════════════════════

def _item(days_to_zero, lead_time=14, avg_sales=10.0, safety_stock=15,
          recommended_qty=50, stock=50, in_transit=0):
    return {
        "days_to_zero":   days_to_zero,
        "lead_time":      lead_time,
        "avg_sales":      avg_sales,
        "safety_stock":   safety_stock,
        "recommended_qty": recommended_qty,
        "stock":          stock,
        "in_transit":     in_transit,
    }


class TestBuildDiagnosis:

    def test_critical_urgency(self):
        """T7: days_to_zero < lead_time → 🔴 Критично."""
        result = ai.build_diagnosis(_item(days_to_zero=5, lead_time=14))
        assert result.startswith("🔴 Критично")

    def test_warning_urgency(self):
        """T8: lead_time < days_to_zero < lead_time*1.5 → 🟡 Внимание."""
        result = ai.build_diagnosis(_item(days_to_zero=18, lead_time=14))
        assert result.startswith("🟡 Внимание")

    def test_normal_urgency(self):
        """T9: days_to_zero >= lead_time*1.5 → 🟢 Норма."""
        result = ai.build_diagnosis(_item(days_to_zero=30, lead_time=14))
        assert result.startswith("🟢 Норма")

    def test_note_appended_when_provided(self):
        """note добавляется с эмодзи-иконкой."""
        result = ai.build_diagnosis(_item(days_to_zero=5), note="Сезонный рост")
        assert "💡 Сезонный рост" in result

    def test_no_note_when_empty(self):
        """Без note — иконка не появляется."""
        result = ai.build_diagnosis(_item(days_to_zero=30), note="")
        assert "💡" not in result

    def test_days_to_zero_zero_avg_sales(self):
        """T15: avg_sales=0 → days_to_zero=999 → не делим на ноль."""
        # days_to_zero уже должен быть вычислен снаружи как 999
        result = ai.build_diagnosis(_item(days_to_zero=999, avg_sales=0, lead_time=14))
        assert result.startswith("🟢 Норма")

    def test_boundary_exactly_lead_time(self):
        """Граничный случай: days_to_zero == lead_time → 🔴 (строго меньше не срабатывает)."""
        result = ai.build_diagnosis(_item(days_to_zero=14, lead_time=14))
        # dtoz < lt → False (равно), dtoz < lt*1.5=21 → True → Внимание
        assert result.startswith("🟡 Внимание")


# ══════════════════════════════════════════════════════════════════
# T10–T12: compute_abc
# ══════════════════════════════════════════════════════════════════

class TestComputeAbc:

    def test_one_dominant_item_gets_A(self):
        """T10: Один товар даёт >80% оборота → категория A."""
        items = [
            {"item_name": "Товар-А", "revenue": 800},
            {"item_name": "Товар-Б", "revenue": 100},
            {"item_name": "Товар-В", "revenue": 100},
        ]
        result = ai.compute_abc(items)
        assert result["Товар-А"] == "A"
        assert result["Товар-Б"] == "B"
        assert result["Товар-В"] == "C"

    def test_equal_items_distribution(self):
        """T11: 10 равных товаров — первые ~8 в A, остальные в B/C."""
        items = [{"item_name": f"T{i}", "revenue": 100} for i in range(10)]
        result = ai.compute_abc(items)
        a_count = sum(1 for v in result.values() if v == "A")
        # 8 товаров дают 80% при равном распределении
        assert a_count == 8

    def test_no_prices_fallback_to_volume(self):
        """T12: revenue=0 у всех → фоллбек на 'volume', не падает."""
        items = [
            {"item_name": "T1", "revenue": 0, "volume": 500},
            {"item_name": "T2", "revenue": 0, "volume": 300},
            {"item_name": "T3", "revenue": 0, "volume": 200},
        ]
        result = ai.compute_abc(items)
        assert "T1" in result
        assert result["T1"] == "A"

    def test_empty_input_returns_empty(self):
        """Пустой список → пустой словарь, не падает."""
        assert ai.compute_abc([]) == {}

    def test_all_zero_revenue_and_volume(self):
        """Все нули → все товары получают 'C'."""
        items = [{"item_name": f"T{i}", "revenue": 0} for i in range(3)]
        result = ai.compute_abc(items)
        assert all(v == "C" for v in result.values())


# ══════════════════════════════════════════════════════════════════
# T13–T14: call_openrouter retry-логика
# ══════════════════════════════════════════════════════════════════

class TestCallOpenrouterRetry:

    def test_succeeds_on_second_attempt(self, tmp_path, mocker):
        """T13: Первый вызов HTTP 429, второй -> 200 с валидным ответом."""
        mocker.patch.object(ai, "get_api_key", return_value="test-key")
        mocker.patch.object(ai, "_log_llm_error")

        fail_resp = MagicMock()
        fail_resp.status_code = 429
        fail_resp.text = "Too Many Requests"

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {
            "choices": [{"message": {"content": "hello"}}]
        }

        # Патчим requests.post внутри модуля ai_services (не глобально)
        mocker.patch.object(ai.requests, "post", side_effect=[fail_resp, ok_resp])
        mocker.patch.object(ai.time, "sleep")

        result = ai.call_openrouter({"model": "test"}, max_attempts=3)
        assert result == "hello"

    def test_all_attempts_fail_writes_log(self, tmp_path, mocker):
        """T14: Все 3 попытки провалились -> исключение + запись в лог."""
        mocker.patch.object(ai, "get_api_key", return_value="test-key")
        log_written = []
        mocker.patch.object(ai, "_log_llm_error", side_effect=lambda m: log_written.append(m))

        fail_resp = MagicMock()
        fail_resp.status_code = 500
        fail_resp.text = "Internal Server Error"

        # Патчим requests.post внутри модуля ai_services
        mocker.patch.object(ai.requests, "post", return_value=fail_resp)
        mocker.patch.object(ai.time, "sleep")

        with pytest.raises(Exception):
            ai.call_openrouter({"model": "test"}, max_attempts=3)

        assert len(log_written) == 1
        assert "500" in log_written[0]
