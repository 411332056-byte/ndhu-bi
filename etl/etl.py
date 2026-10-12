"""
台電發購電量 ETL
================
來源：台灣電力股份有限公司「台灣電力公司發購電量(能源別)及公用售電業售電量(用途別)」
      政府資料開放平臺 https://data.gov.tw/dataset/35392
      001.csv 發購電量（能源別）  https://service.taipower.com.tw/data/opendata/apply/file/d003005/001.csv
      002.csv 售電量（用途別）    https://service.taipower.com.tw/data/opendata/apply/file/d003005/002.csv

執行：python etl/etl.py        （只用 Python 標準函式庫，不需安裝套件）
輸入：raw/*.csv（原檔，001 為 Big5 編碼、002 為 UTF-8 with BOM，不做任何修改）
輸出：data/*.csv、data/etl_report.json
核對失敗時程式以非 0 結束，不會寫出不一致的資料。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw"
OUT = ROOT / "data"

RAW_GEN = RAW / "taipower_d003005_001_generation_by_source.csv"
RAW_SALES = RAW / "taipower_d003005_002_sales_by_use.csv"

KWH_PER_YIDU = 1e8  # 1 億度 = 1 億 kWh

# 能源別 → 分析用類別。is_renewable 依台電再生能源統計口徑。
# 「垃圾沼氣」「廢棄物」計入再生能源後，2025 年占比 12.66%，與台電公布的 12.7% 一致（見 etl_report.json）。
CATEGORY_MAP = {
    # 原始能源別: (類別, 是否再生能源, 說明)
    "燃煤":     ("燃煤",     False, "台電自發＋民營電廠購電"),
    "燃氣":     ("燃氣",     False, "台電自發＋民營電廠購電"),
    "燃油":     ("燃油",     False, "台電自發"),
    "汽電共生": ("汽電共生", False, "購電；以化石燃料為主"),
    "核能":     ("核能",     False, "台電自發；核三 2 號機 2025-05-17 停機"),
    "抽蓄水力": ("抽蓄水力", False, "儲能型水力，不計入再生能源"),
    "電池":     ("儲能電池", False, "儲能放電量，不計入再生能源"),
    "太陽能":   ("太陽光電", True,  "台電自建＋購電"),
    "風力":     ("風力",     True,  "台電自建＋購電"),
    "慣常水力": ("水力",     True,  "台電自發"),
    "承攬水力": ("水力",     True,  "購電"),
    "水力":     ("水力",     True,  "購電（民營小水力）"),
    "地熱":     ("其他再生", True,  "台電自建＋購電"),
    "生質能":   ("其他再生", True,  "購電"),
    "廢棄物":   ("其他再生", True,  "購電"),
    "垃圾沼氣": ("其他再生", True,  "購電；欄位名稱照原檔"),
}

GROUP_ORDER = ["燃煤", "燃氣", "燃油", "汽電共生", "核能", "抽蓄水力", "儲能電池",
               "太陽光電", "風力", "水力", "其他再生"]
RENEWABLE_GROUPS = ["太陽光電", "風力", "水力", "其他再生"]

# 外部核對值：台電公布之 2025 年系統發購電結構（科技新報 2026-03-20 引述台電資料）
PUBLISHED_2025 = {"total_yidu": 2524.4, "renewable_pct": 12.7, "nuclear_pct": 1.2}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_text(path: Path) -> tuple[str, str]:
    """依序嘗試 UTF-8(BOM)、Big5/CP950。回傳 (文字, 編碼)。"""
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "cp950"):
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"無法判斷編碼：{path}")


def parse_number(text: str) -> tuple[float | None, str]:
    """回傳 (數值, 旗標)。空白→None；科學記號→標記精度不足。"""
    s = text.strip().replace(",", "")
    if s == "":
        return None, "missing"
    value = float(s)
    if "e" in s.lower():
        return value, "scientific_notation"
    return value, ""


class CheckFailed(Exception):
    pass


def check(cond: bool, name: str, detail: str, results: list) -> None:
    results.append({"check": name, "passed": bool(cond), "detail": detail})
    if not cond:
        raise CheckFailed(f"{name}：{detail}")


def main() -> int:
    OUT.mkdir(exist_ok=True)
    checks: list[dict] = []
    warnings: list[str] = []

    # ---------- Extract ----------
    gen_text, gen_enc = read_text(RAW_GEN)
    sales_text, sales_enc = read_text(RAW_SALES)
    gen_rows = list(csv.DictReader(io.StringIO(gen_text)))
    sales_rows = list(csv.DictReader(io.StringIO(sales_text)))

    check(list(gen_rows[0].keys()) == ["年度", "發購電", "能源別", "發購電量"],
          "001 欄位", f"欄位為 {list(gen_rows[0].keys())}", checks)
    check(list(sales_rows[0].keys()) == ["年度", "用途別", "售電量"],
          "002 欄位", f"欄位為 {list(sales_rows[0].keys())}", checks)

    # ---------- Transform：發購電量 ----------
    long_rows = []
    seen = set()
    raw_total_by_year: dict[int, float] = defaultdict(float)
    for r in gen_rows:
        year = int(r["年度"])
        supplier = r["發購電"].strip()
        energy = r["能源別"].strip()
        key = (year, supplier, energy)
        if key in seen:
            raise CheckFailed(f"{key} 重複出現")
        seen.add(key)
        if energy not in CATEGORY_MAP:
            raise CheckFailed(f"未知能源別「{energy}」，請先補進 CATEGORY_MAP")
        value, flag = parse_number(r["發購電量"])
        if value is not None and value < 0:
            raise CheckFailed(f"{key} 為負值 {value}")
        if flag == "scientific_notation":
            warnings.append(f"{year} {supplier}{energy} 原檔為科學記號「{r['發購電量'].strip()}」，"
                            f"有效位數只到 6 位，誤差約 ±0.05 億度")
        group, renewable, _ = CATEGORY_MAP[energy]
        kwh = value or 0.0
        raw_total_by_year[year] += kwh
        long_rows.append({
            "year": year, "supplier": supplier, "energy_raw": energy, "group": group,
            "is_renewable": int(renewable), "kwh": kwh,
            "yidu": round(kwh / KWH_PER_YIDU, 4), "flag": flag,
        })

    years = sorted(raw_total_by_year)
    check(years == list(range(years[0], years[-1] + 1)), "年度連續",
          f"{years[0]}–{years[-1]} 共 {len(years)} 年", checks)
    check(len(seen) == len(gen_rows), "無重複列", f"{len(gen_rows)} 列皆唯一（年度×發購電×能源別）", checks)

    # 依類別彙總
    by_group: dict[int, dict[str, float]] = {y: {g: 0.0 for g in GROUP_ORDER} for y in years}
    for row in long_rows:
        by_group[row["year"]][row["group"]] += row["kwh"]

    # 核對 1：類別加總 = 原始逐列加總（每一年）
    max_diff = max(abs(sum(by_group[y].values()) - raw_total_by_year[y]) for y in years)
    check(max_diff < 1, "類別加總＝總發購電量",
          f"{len(years)} 個年度，最大差異 {max_diff:.6f} 度", checks)

    group_rows, summary_rows = [], []
    prev_re = None
    for y in years:
        total = raw_total_by_year[y]
        shares = {g: by_group[y][g] / total * 100 for g in GROUP_ORDER}
        # 核對 2：占比合計 100%
        check(abs(sum(shares.values()) - 100) < 1e-9, f"{y} 占比合計",
              f"{sum(shares.values()):.10f}%", checks)
        for g in GROUP_ORDER:
            group_rows.append({
                "year": y, "group": g, "is_renewable": int(g in RENEWABLE_GROUPS),
                "yidu": round(by_group[y][g] / KWH_PER_YIDU, 3),
                "share_pct": round(shares[g], 3),
            })
        re_kwh = sum(by_group[y][g] for g in RENEWABLE_GROUPS)
        fossil_kwh = sum(by_group[y][g] for g in ["燃煤", "燃氣", "燃油", "汽電共生"])
        re_yoy = None if prev_re in (None, 0) else (re_kwh / prev_re - 1) * 100
        summary_rows.append({
            "year": y,
            "total_yidu": round(total / KWH_PER_YIDU, 3),
            "renewable_yidu": round(re_kwh / KWH_PER_YIDU, 3),
            "renewable_share_pct": round(re_kwh / total * 100, 3),
            "renewable_yoy_pct": "" if re_yoy is None else round(re_yoy, 2),
            "fossil_share_pct": round(fossil_kwh / total * 100, 3),
            "nuclear_share_pct": round(shares["核能"], 3),
            "coal_share_pct": round(shares["燃煤"], 3),
            "gas_share_pct": round(shares["燃氣"], 3),
        })
        prev_re = re_kwh

    # ---------- Transform：售電量 ----------
    sales_long, sales_total = [], defaultdict(float)
    for r in sales_rows:
        y = int(r["年度"])
        value, flag = parse_number(r["售電量"])
        if value is None:
            raise CheckFailed(f"{y} {r['用途別']} 售電量缺值")
        use = r["用途別"].strip().replace("售電量", "")
        sales_total[y] += value
        sales_long.append({"year": y, "use": use, "yidu": round(value / KWH_PER_YIDU, 3)})
    check(sorted(sales_total) == years, "售電量年度與發購電量一致",
          f"{min(sales_total)}–{max(sales_total)}", checks)
    for s in summary_rows:
        st = sales_total[s["year"]] / KWH_PER_YIDU
        s["sales_yidu"] = round(st, 3)
        # 售電量 / 發購電量：差額包含線損、廠內用電與抽蓄耗電
        s["sales_to_supply_pct"] = round(st / s["total_yidu"] * 100, 2)
    check(all(0 < s["sales_to_supply_pct"] < 100 for s in summary_rows), "售電量小於發購電量",
          f"比值介於 {min(s['sales_to_supply_pct'] for s in summary_rows)}% 與 "
          f"{max(s['sales_to_supply_pct'] for s in summary_rows)}%", checks)

    # ---------- 外部核對：與台電公布 2025 年數字比較 ----------
    s25 = next(s for s in summary_rows if s["year"] == 2025)
    check(abs(s25["total_yidu"] - PUBLISHED_2025["total_yidu"]) <= 0.1, "2025 總量 vs 台電公布",
          f"ETL {s25['total_yidu']:.1f} 億度；公布 {PUBLISHED_2025['total_yidu']} 億度", checks)
    check(abs(s25["renewable_share_pct"] - PUBLISHED_2025["renewable_pct"]) <= 0.05,
          "2025 再生能源占比 vs 台電公布",
          f"ETL {s25['renewable_share_pct']:.2f}%；公布 {PUBLISHED_2025['renewable_pct']}%", checks)
    check(abs(s25["nuclear_share_pct"] - PUBLISHED_2025["nuclear_pct"]) <= 0.05,
          "2025 核能占比 vs 台電公布",
          f"ETL {s25['nuclear_share_pct']:.2f}%；公布 {PUBLISHED_2025['nuclear_pct']}%", checks)

    # ---------- Load ----------
    def write(name: str, rows: list[dict]) -> None:
        with open(OUT / name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    write("generation_long.csv", long_rows)
    write("generation_by_group.csv", group_rows)
    write("annual_summary.csv", summary_rows)
    write("sales_by_use.csv", sales_long)
    write("category_map.csv", [
        {"energy_raw": k, "group": v[0], "is_renewable": int(v[1]), "note": v[2]}
        for k, v in CATEGORY_MAP.items()
    ])

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": {
            "publisher": "台灣電力股份有限公司",
            "dataset": "台灣電力公司發購電量(能源別)及公用售電業售電量(用途別)",
            "dataset_url": "https://data.gov.tw/dataset/35392",
            "license": "政府資料開放授權條款-第1版",
        },
        "raw_files": [
            {"file": RAW_GEN.name, "encoding": gen_enc, "rows": len(gen_rows), "sha256": sha256(RAW_GEN)},
            {"file": RAW_SALES.name, "encoding": sales_enc, "rows": len(sales_rows), "sha256": sha256(RAW_SALES)},
        ],
        "period": f"{years[0]}–{years[-1]}（年資料）",
        "unit": "億度（1 億度 = 1 億 kWh）",
        "checks": checks,
        "warnings": warnings,
    }
    (OUT / "etl_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    passed = sum(c["passed"] for c in checks)
    print(f"ETL 完成：{len(years)} 年、{len(long_rows)} 筆原始列 → data/")
    print(f"核對 {passed}/{len(checks)} 通過")
    for w in warnings:
        print("注意：", w)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except CheckFailed as e:
        print("核對失敗，未寫出資料：", e, file=sys.stderr)
        sys.exit(1)
