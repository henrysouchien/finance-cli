"""Federal tax bracket and FICA constants for supported tax years."""

from __future__ import annotations

from decimal import Decimal
from typing import Any


SUPPORTED_TAX_YEARS: frozenset[int] = frozenset({2025, 2026})


FEDERAL_BRACKETS_2025: dict[str, list[tuple[int, Decimal]]] = {
    "single": [  # IRS Rev. Proc. 2024-40, IRB 2024-45, § 2.01 Table 3.
        (0, Decimal("10")),
        (11_925_00, Decimal("12")),
        (48_475_00, Decimal("22")),
        (103_350_00, Decimal("24")),
        (197_300_00, Decimal("32")),
        (250_525_00, Decimal("35")),
        (626_350_00, Decimal("37")),
    ],
    "married_filing_jointly": [  # IRS Rev. Proc. 2024-40, IRB 2024-45, § 2.01 Table 1.
        (0, Decimal("10")),
        (23_850_00, Decimal("12")),
        (96_950_00, Decimal("22")),
        (206_700_00, Decimal("24")),
        (394_600_00, Decimal("32")),
        (501_050_00, Decimal("35")),
        (751_600_00, Decimal("37")),
    ],
    "married_filing_separately": [  # IRS Rev. Proc. 2024-40, IRB 2024-45, § 2.01 Table 4.
        (0, Decimal("10")),
        (11_925_00, Decimal("12")),
        (48_475_00, Decimal("22")),
        (103_350_00, Decimal("24")),
        (197_300_00, Decimal("32")),
        (250_525_00, Decimal("35")),
        (375_800_00, Decimal("37")),
    ],
    "head_of_household": [  # IRS Rev. Proc. 2024-40, IRB 2024-45, § 2.01 Table 2.
        (0, Decimal("10")),
        (17_000_00, Decimal("12")),
        (64_850_00, Decimal("22")),
        (103_350_00, Decimal("24")),
        (197_300_00, Decimal("32")),
        (250_500_00, Decimal("35")),
        (626_350_00, Decimal("37")),
    ],
}


STANDARD_DEDUCTION_2025: dict[str, int] = {
    "single": 15_750_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 3.01.
    "married_filing_jointly": 31_500_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 3.01.
    "married_filing_separately": 15_750_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 3.01.
    "head_of_household": 23_625_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 3.01.
}


FEDERAL_BRACKETS_2026: dict[str, list[tuple[int, Decimal]]] = {
    "single": [  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.01 Table 3.
        (0, Decimal("10")),
        (12_400_00, Decimal("12")),
        (50_400_00, Decimal("22")),
        (105_700_00, Decimal("24")),
        (201_775_00, Decimal("32")),
        (256_225_00, Decimal("35")),
        (640_600_00, Decimal("37")),
    ],
    "married_filing_jointly": [  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.01 Table 1.
        (0, Decimal("10")),
        (24_800_00, Decimal("12")),
        (100_800_00, Decimal("22")),
        (211_400_00, Decimal("24")),
        (403_550_00, Decimal("32")),
        (512_450_00, Decimal("35")),
        (768_700_00, Decimal("37")),
    ],
    "married_filing_separately": [  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.01 Table 4.
        (0, Decimal("10")),
        (12_400_00, Decimal("12")),
        (50_400_00, Decimal("22")),
        (105_700_00, Decimal("24")),
        (201_775_00, Decimal("32")),
        (256_225_00, Decimal("35")),
        (384_350_00, Decimal("37")),
    ],
    "head_of_household": [  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.01 Table 2.
        (0, Decimal("10")),
        (17_700_00, Decimal("12")),
        (67_450_00, Decimal("22")),
        (105_700_00, Decimal("24")),
        (201_750_00, Decimal("32")),
        (256_200_00, Decimal("35")),
        (640_600_00, Decimal("37")),
    ],
}


STANDARD_DEDUCTION_2026: dict[str, int] = {
    "single": 16_100_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.14(1).
    "married_filing_jointly": 32_200_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.14(1).
    "married_filing_separately": 16_100_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.14(1).
    "head_of_household": 24_150_00,  # IRS Rev. Proc. 2025-32, IRB 2025-45, § 4.14(1).
}


FICA_CONSTANTS_2025: dict[str, Any] = {
    "ss_rate": Decimal("0.062"),  # SSA 2025 COLA fact sheet.
    "ss_wage_base_cents": 17_610_000,  # SSA 2025 COLA fact sheet.
    "medicare_rate": Decimal("0.0145"),  # SSA 2025 COLA fact sheet.
    "additional_medicare_rate": Decimal("0.009"),  # IRS Form 8959 instructions.
    "additional_medicare_thresholds_cents": {  # IRS Form 8959 instructions.
        "single": 20_000_000,
        "married_filing_jointly": 25_000_000,
        "married_filing_separately": 12_500_000,
        "head_of_household": 20_000_000,
    },
    "se_earnings_multiplier": Decimal("0.9235"),  # IRS Schedule SE instructions.
    "se_ss_rate": Decimal("0.124"),  # IRS Schedule SE instructions.
    "se_medicare_rate": Decimal("0.029"),  # IRS Schedule SE instructions.
}


FICA_CONSTANTS_2026: dict[str, Any] = {
    "ss_rate": Decimal("0.062"),  # SSA 2026 COLA fact sheet.
    "ss_wage_base_cents": 18_450_000,  # SSA 2026 COLA fact sheet.
    "medicare_rate": Decimal("0.0145"),  # SSA 2026 COLA fact sheet.
    "additional_medicare_rate": Decimal("0.009"),  # IRS Form 8959 instructions.
    "additional_medicare_thresholds_cents": {  # IRS Form 8959 instructions.
        "single": 20_000_000,
        "married_filing_jointly": 25_000_000,
        "married_filing_separately": 12_500_000,
        "head_of_household": 20_000_000,
    },
    "se_earnings_multiplier": Decimal("0.9235"),  # IRS Schedule SE instructions.
    "se_ss_rate": Decimal("0.124"),  # IRS Schedule SE instructions.
    "se_medicare_rate": Decimal("0.029"),  # IRS Schedule SE instructions.
}


FEDERAL_BRACKETS: dict[int, dict[str, list[tuple[int, Decimal]]]] = {
    2025: FEDERAL_BRACKETS_2025,
    2026: FEDERAL_BRACKETS_2026,
}

STANDARD_DEDUCTION: dict[int, dict[str, int]] = {
    2025: STANDARD_DEDUCTION_2025,
    2026: STANDARD_DEDUCTION_2026,
}

FICA_CONSTANTS: dict[int, dict[str, Any]] = {
    2025: FICA_CONSTANTS_2025,
    2026: FICA_CONSTANTS_2026,
}
