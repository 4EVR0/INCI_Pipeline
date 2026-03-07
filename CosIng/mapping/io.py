import pandas as pd


def load_kcia_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    required_cols = ["ingredient_code", "std_name_ko", "std_name_en"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"KCIA CSV에 필요한 컬럼이 없습니다: {missing}")

    return df.copy()


def load_cosing_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # CosIng 컬럼명이 inci_name 또는 std_name_en 등 다를 수 있으니 유연하게 처리
    candidate_cols = ["inci_name", "std_name_en", "ingredient_name"]
    found = next((c for c in candidate_cols if c in df.columns), None)

    if found is None:
        raise ValueError(
            f"CosIng CSV에 이름 컬럼이 없습니다. 후보 컬럼 중 하나 필요: {candidate_cols}"
        )

    df = df.copy()
    if found != "inci_name":
        df["inci_name"] = df[found]

    return df