import pandas as pd
import numpy as np
import re

# Citește fișierul CSV original cu date brute
df = pd.read_csv("veridion_entity_resolution_challenge.csv")

# Selectează doar coloanele relevante pentru deduplicare
selected_cols = [
    "company_name", "company_legal_names", "company_commercial_names",
    "short_description", "long_description",
    "primary_phone", "phone_numbers",
    "primary_email", "emails",
    "website_url", "website_domain",
    "facebook_url", "twitter_url", "instagram_url", "linkedin_url",
    "main_city", "main_postcode", "main_country_code",
    "main_latitude", "main_longitude",
    "domains", "all_domains"
]

# Creează un subset din DataFrame doar cu aceste coloane
df_selected = df[selected_cols].copy()

# Funcție pentru normalizarea textului:
# - transformă în lowercase
# - elimină spații suplimentare
# - păstrează caractere utile (e-mailuri, URL-uri etc.)
def normalize_text(value):
    if pd.isna(value):
        return ""
    value = str(value).lower().strip()
    value = re.sub(r"[^\w\s@.:/-]", "", value)  # păstrează doar caractere utile
    value = re.sub(r"\s+", " ", value)  # înlocuiește spațiile multiple cu unul singur
    return value

# Aplică funcția de normalizare pe toate coloanele de tip text
for col in df_selected.select_dtypes(include="object").columns:
    df_selected[col] = df_selected[col].apply(normalize_text)

# Curăță câmpul de telefon principal — elimină .0 din valorile convertite eronat din float
df_selected["primary_phone"] = df_selected["primary_phone"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)

# Salvează datele normalizate într-un nou fișier CSV
df_selected.to_csv("normalized_companies.csv", index=False)

# Confirmare în consolă
print("Normalizarea a fost aplicată și salvată în 'normalized_companies.csv'")
