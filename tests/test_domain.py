from pathlib import Path
import sys
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from domain import expected_gear, is_alert, is_silver_family


class DomainRulesTest(unittest.TestCase):
    def test_expected_gear_uses_half_up_rounding(self) -> None:
        self.assertEqual(expected_gear(0), 0)
        self.assertEqual(expected_gear(14), 0)
        self.assertEqual(expected_gear(15), 1)
        self.assertEqual(expected_gear(200), 7)

    def test_any_condition_creates_an_alert(self) -> None:
        self.assertTrue(is_alert(speed=121, rpm=1000, gear=4))
        self.assertTrue(is_alert(speed=60, rpm=6001, gear=2))
        self.assertTrue(is_alert(speed=60, rpm=1000, gear=1))
        self.assertFalse(is_alert(speed=60, rpm=1000, gear=2))

    def test_gray_and_silver_are_one_family(self) -> None:
        for value in ("Gray", "grey", " SILVER "):
            self.assertTrue(is_silver_family(value))
        self.assertFalse(is_silver_family("Blue"))


if __name__ == "__main__":
    unittest.main()

