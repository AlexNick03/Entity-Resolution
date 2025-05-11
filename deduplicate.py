import pandas as pd
import numpy as np
from rapidfuzz.fuzz import token_sort_ratio
import networkx as nx
from itertools import combinations
from tqdm import tqdm

# === 1. Încarcă datele normalizate ===
df = pd.read_csv("normalized_companies.csv").fillna("")
df = df.reset_index(drop=True)

# === 2. Blocking: grupăm doar în interiorul aceleiași țări sau oraș ===
# (reduce masiv numărul de comparații brute)
blocking_key = df["main_country_code"].fillna("") + "|" + df["main_city"].fillna("")
df["block_key"] = blocking_key

# === 3. Funcție de scor de similaritate între două rânduri ===
def compute_similarity_score(row1, row2):
    name_score = token_sort_ratio(row1["company_name"], row2["company_name"])
    domain_score = token_sort_ratio(str(row1["website_domain"]), str(row2["website_domain"]))
    phone_score = token_sort_ratio(str(row1["primary_phone"]), str(row2["primary_phone"]))
    city_score = token_sort_ratio(str(row1["main_city"]), str(row2["main_city"]))
    return 0.4 * name_score + 0.3 * domain_score + 0.2 * phone_score + 0.1 * city_score

# === 4. Construim graful de similaritate ===
G = nx.Graph()
G.add_nodes_from(df.index)

threshold = 85
print("Începem gruparea după block-uri")

for block_value, block_df in tqdm(df.groupby("block_key")):
    indices = block_df.index.tolist()
    for i, j in combinations(indices, 2):
        score = compute_similarity_score(df.loc[i], df.loc[j])
        if score >= threshold:
            G.add_edge(i, j)

# === 5. Grupare în group_id ===
groups = list(nx.connected_components(G))
group_map = {idx: gid for gid, group in enumerate(groups) for idx in group}
df["group_id"] = df.index.map(group_map).fillna(-1).astype(int)

# === 6. Alegem cea mai completă companie din fiecare grup ===
df["completeness_score"] = df.apply(lambda row: sum([1 for val in row if val != ""]), axis=1)
representatives = df.sort_values("completeness_score", ascending=False).drop_duplicates(subset=["group_id"])
representatives = representatives.drop(columns=["completeness_score", "block_key"])

# === 7. Salvare ===
df.to_csv("all_companies_with_group_id.csv", index=False)
representatives.to_csv("unique_companies_after_dedup.csv", index=False)

print("Grupare completă. Rezultatul este în:")
print(" - all_companies_with_group_id.csv (toate rândurile cu group_id)")
print(" - unique_companies_after_dedup.csv (doar companiile unice)")
