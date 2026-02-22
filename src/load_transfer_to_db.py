# load_transfer_to_db.py

import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os


# =======================
# ===== НАСТРОЙКИ =======
# =======================

INPUT_FILE = Path("data/output/products_transfer_clean.xlsx")
TABLE_NAME = "products_transfer"

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
def main():

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Файл не найден: {INPUT_FILE}")

    print("Читаем очищенный файл...")

    df = pd.read_excel(INPUT_FILE)

    df = df.rename(columns={
        "Товар": "name",
        "Штрих-код": "barcode",
        "Сальдо (кон.)": "stock_total",
        "остаток в ЦО": "stock_central",
        "дата последнего прихода": "last_purchase_date",
    })

    # =======================
    # приведение типов
    # =======================

    # дата (если нет — 1900-01-01)
    df["last_purchase_date"] = pd.to_datetime(
        df["last_purchase_date"],
        dayfirst=True,
        errors="coerce"
    ).fillna(pd.Timestamp("1900-01-01")).dt.date

    # остатки
    df["stock_total"] = (
        pd.to_numeric(df["stock_total"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    df["stock_central"] = (
        pd.to_numeric(df["stock_central"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    engine = get_engine()

    print("Загружаем данные в БД...")

    insert_sql = text(f"""
        INSERT INTO {TABLE_NAME}
        (barcode, name, stock_total, stock_central, last_purchase_date)
        VALUES
        (:barcode, :name, :stock_total, :stock_central, :last_purchase_date)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            stock_total = VALUES(stock_total),
            stock_central = VALUES(stock_central),
            last_purchase_date = VALUES(last_purchase_date)
    """)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(insert_sql, {
                "barcode": str(row["barcode"]),
                "name": row["name"],
                "stock_total": row["stock_total"],
                "stock_central": row["stock_central"],
                "last_purchase_date": row["last_purchase_date"],
            })

    print("Готово. Данные обновлены.")


# =======================
if __name__ == "__main__":
    main()