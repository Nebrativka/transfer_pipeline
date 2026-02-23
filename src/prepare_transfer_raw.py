# prepare_transfer_raw.py

import pandas as pd
from pathlib import Path


# =======================
# ===== НАСТРОЙКИ =======
# =======================

INPUT_FILE = Path("data/input/products_transfer_raw.xlsx")
OUTPUT_FILE = Path("data/output/products_transfer_clean.xlsx")

REQUIRED_COLUMNS = [
    "Товар",
    "Штрих-код",
    "Сальдо (кон.)",
    "остаток в ЦО",
    "дата последнего прихода",
]


# =======================
def find_header_row(df):
    for i in range(len(df)):
        row = df.iloc[i].astype(str)

        if row.str.contains("Штрих", case=False, na=False).any():
            return i

    return None


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
def main():

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Файл не найден: {INPUT_FILE}")

    print("Читаем исходный файл...")

    df_raw = pd.read_excel(INPUT_FILE, header=None)

    header_row = find_header_row(df_raw)

    if header_row is None:
        raise ValueError("Не удалось найти строку заголовков")

    print(f"Заголовки найдены в строке: {header_row + 1}")

    df = pd.read_excel(INPUT_FILE, header=header_row)

    df.columns = clean_columns(df.columns)

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Отсутствуют нужные колонки: {missing_columns}")

    df = df[REQUIRED_COLUMNS]
    
    # Удаляем товары с "УЦІНКА"
    df = df[~df["Товар"].str.contains("УЦІНКА", case=False, na=False)]

    # удаляем строки без штрих-кода
    df = df.dropna(subset=["Штрих-код"])

    # штрих-код всегда строка
    df["Штрих-код"] = (
    pd.to_numeric(df["Штрих-код"], errors="coerce")
      .astype("Int64")
      .astype(str)
)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)

    print("Готово. Файл сохранен.")


# =======================
if __name__ == "__main__":
    main()