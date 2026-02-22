# prepare_branch_metrics.py

import pandas as pd
from pathlib import Path
import re


# =======================
# ===== НАСТРОЙКИ =======
# =======================

INPUT_DIR = Path("data/input")
OUTPUT_DIR = Path("data/output")

BRANCH_FILES = [
    "lv_input.xlsx",
    "rb_input.xlsx",
    "lc_input.xlsx",
    "hm_input.xlsx",
    "if_input.xlsx",
    "zt_input.xlsx",
    "mk_input.xlsx",
    "dp_input.xlsx",
    "ck_input.xlsx",
]


# =======================
def clean_columns(columns):
    return (
        columns
        .str.replace("\n", " ", regex=False)
        .str.replace("\r", " ", regex=False)
        .str.strip()
        .str.replace("  ", " ", regex=False)
    )


# =======================
def find_header_row(df):
    for i in range(len(df)):
        row = df.iloc[i].astype(str)
        if row.str.contains("Штрих", case=False, na=False).any():
            return i
    return None


# =======================
def extract_month(col_name):
    match = re.search(r"\d{2}\.\d{4}", col_name)
    if match:
        return pd.to_datetime(match.group(), format="%m.%Y")
    return None


# =======================
def process_file(file_name):

    input_path = INPUT_DIR / file_name
    output_path = OUTPUT_DIR / file_name.replace("_input", "_clean")

    if not input_path.exists():
        print(f"Файл не найден: {file_name}")
        return

    print(f"Обрабатываем: {file_name}")

    df_raw = pd.read_excel(input_path, header=None)

    header_row = find_header_row(df_raw)

    if header_row is None:
        print("Не удалось найти строку заголовков")
        return

    df = pd.read_excel(input_path, header=header_row)
    df.columns = clean_columns(df.columns)

    # =======================
    # поиск колонок продаж
    # =======================

    sales_cols = [
        col for col in df.columns
        if "Продажа" in col
        and "склад" in col.lower()
        and "получатель" in col.lower()
        and "за" in col.lower()
    ]

    if not sales_cols:
        print("Не найдены колонки продаж")
        return

    # сортируем по дате
    sales_cols_sorted = sorted(
        sales_cols,
        key=lambda x: extract_month(x) if extract_month(x) is not None else pd.Timestamp.min
    )

    # берём максимум 2 последних
    sales_cols_sorted = sales_cols_sorted[-2:]

    # =======================
    # формируем итог
    # =======================

    needed_columns = {
        "Штрих-код": "barcode",
        "Продажа (склад получатель) за период": "sales_period",
        "Сальдо (кон.)": "stock_balance",
        "Дата последнего перемещения": "last_movement_date",
        "Продажи склады отправки": "sales_from_warehouses",
    }

    # добавляем месяцы динамически
    if len(sales_cols_sorted) == 2:
        needed_columns[sales_cols_sorted[0]] = "sales_prev_month"
        needed_columns[sales_cols_sorted[1]] = "sales_current_month"
    elif len(sales_cols_sorted) == 1:
        needed_columns[sales_cols_sorted[0]] = "sales_current_month"

    missing = [col for col in needed_columns if col not in df.columns]

    if missing:
        print(f"Отсутствуют колонки: {missing}")
        return

    df = df[list(needed_columns.keys())]
    df = df.rename(columns=needed_columns)

    # удалить строки без штрих-кода
    df = df.dropna(subset=["barcode"])
    df["barcode"] = (
    df["barcode"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.strip()
)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    

    print(f"Готово: {file_name}")


# =======================
def main():
    for file_name in BRANCH_FILES:
        process_file(file_name)


# =======================
if __name__ == "__main__":
    main()