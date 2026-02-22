# load_branch_metrics.py

import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os


# =======================
# ===== НАСТРОЙКИ =======
# =======================

INPUT_DIR = Path("data/output")
TABLE_NAME = "branch_metrics"

BRANCH_CODES = ["lv", "rb", "lc", "hm", "if", "zt", "mk", "dp", "ck"]

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", 3306),
    "database": os.getenv("DB_NAME"),
}


# =======================
def get_engine():
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        "?charset=utf8mb4"
    )
    return create_engine(url)


# =======================
def normalize_barcode(series):
    return (
        series.astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )


# =======================
def process_file(file_path: Path, branch_code: str, engine):

    print(f"Загружаем: {file_path.name} | branch={branch_code}")

    df = pd.read_excel(file_path)

    # barcode
    if "barcode" not in df.columns:
        print("Нет колонки barcode, пропускаю файл")
        return

    df["barcode"] = normalize_barcode(df["barcode"])
    df = df.dropna(subset=["barcode"])

    # числа -> int, пустое -> 0
    numeric_cols = [
        "sales_prev_month",
        "sales_current_month",
        "sales_period",
        "sales_from_warehouses",
        "stock_balance",
    ]

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # дата (если пусто — 1900-01-01)
    if "last_movement_date" not in df.columns:
        df["last_movement_date"] = "1900-01-01"
    else:
        df["last_movement_date"] = pd.to_datetime(
            df["last_movement_date"],
            dayfirst=True,
            errors="coerce"
        ).fillna(pd.Timestamp("1900-01-01")).dt.date

    insert_sql = text(f"""
        INSERT INTO {TABLE_NAME}
        (
            branch_code, barcode,
            sales_prev_month, sales_current_month,
            sales_period, sales_from_warehouses,
            stock_balance, last_movement_date
        )
        VALUES
        (
            :branch_code, :barcode,
            :sales_prev_month, :sales_current_month,
            :sales_period, :sales_from_warehouses,
            :stock_balance, :last_movement_date
        )
        ON DUPLICATE KEY UPDATE
            sales_prev_month = VALUES(sales_prev_month),
            sales_current_month = VALUES(sales_current_month),
            sales_period = VALUES(sales_period),
            sales_from_warehouses = VALUES(sales_from_warehouses),
            stock_balance = VALUES(stock_balance),
            last_movement_date = VALUES(last_movement_date)
    """)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(insert_sql, {
                "branch_code": branch_code,
                "barcode": row["barcode"],
                "sales_prev_month": row["sales_prev_month"],
                "sales_current_month": row["sales_current_month"],
                "sales_period": row["sales_period"],
                "sales_from_warehouses": row["sales_from_warehouses"],
                "stock_balance": row["stock_balance"],
                "last_movement_date": row["last_movement_date"],
            })

    print(f"Готово: {file_path.name}")


# =======================
def main():

    engine = get_engine()

    files = []
    for code in BRANCH_CODES:
        p = INPUT_DIR / f"{code}_clean.xlsx"
        if p.exists():
            files.append((p, code))
        else:
            print(f"Нет файла: {p.name}")

    if not files:
        print("Нет файлов филиалов для загрузки")
        return

    for file_path, branch_code in files:
        process_file(file_path, branch_code, engine)


# =======================
if __name__ == "__main__":
    main()