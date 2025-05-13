import pandas as pd
import re

# 1. Load raw dataset
df = pd.read_csv("veridion_entity_resolution_challenge.csv")

# 2. Define relevant columns
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

# 3. Normalize text
def normalize_text(value):
    if pd.isna(value):
        return ""
    value = str(value).lower().strip()
    value = re.sub(r"[^\w\s@.:/-]", "", value)  # păstrează caractere utile
    value = re.sub(r"\s+", " ", value)
    return value

# 4. Apply normalization to all text fields
for col in df_selected.select_dtypes(include="object").columns:
    df_selected[col] = df_selected[col].apply(normalize_text)

# 5. Clean primary phone format
df_selected["primary_phone"] = df_selected["primary_phone"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)

# 6. Drop rows with too few non-empty relevant fields
# We'll require at least 3 non-empty values among key fields
key_fields = [
    "company_name", "company_legal_names", "company_commercial_names",
    "short_description", "website_domain"
]
df_selected = df_selected[df_selected[key_fields].apply(lambda row: sum([val != "" for val in row]), axis=1) >= 3]

# 7. Save result

df_selected.to_csv("normalized_companies.csv", index=False)
print("Normalized data saved to 'normalized_companies.csv'")
