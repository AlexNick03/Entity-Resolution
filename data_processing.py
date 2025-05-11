import pandas as pd
import numpy as np
import re

df = pd.read_csv("veridion_entity_resolution_challenge.csv")

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

df_selected = df[selected_cols].copy()

def normalize_text(value):
    if pd.isna(value):
        return ""
    value = str(value).lower().strip()
    value = re.sub(r"[^\w\s@.:/-]", "", value)  
    value = re.sub(r"\s+", " ", value)  
    return value

for col in df_selected.select_dtypes(include="object").columns:
    df_selected[col] = df_selected[col].apply(normalize_text)


df_selected["primary_phone"] = df_selected["primary_phone"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)

df_selected.to_csv("normalized_companies.csv", index=False)

print("Normalizarea a fost aplicată și salvată în 'normalized_companies.csv'")