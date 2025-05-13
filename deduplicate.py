import pandas as pd
import networkx as nx
from itertools import combinations
from rapidfuzz.fuzz import token_sort_ratio
from tqdm import tqdm

# 1. Load normalized data
df = pd.read_csv("normalized_companies.csv").fillna("").reset_index(drop=True)

# 2. Define improved blocking key: first 4 letters of name + domain
df["block_key"] = df["company_name"].str[:4] + "|" + df["website_domain"].str[:4]

# 3. Additional group for exact name matching
df["name_group"] = df["company_name"]

# 4. Define similarity scoring function
def compute_similarity_score(row1, row2):
    name_score = token_sort_ratio(row1["company_name"], row2["company_name"])
    comm_score = token_sort_ratio(row1["company_commercial_names"], row2["company_commercial_names"])
    comm_score_2 = token_sort_ratio(row1["company_legal_names"], row2["company_legal_names"])
    domain_score = token_sort_ratio(str(row1["website_domain"]), str(row2["website_domain"]))
    phone_score = token_sort_ratio(str(row1["primary_phone"]), str(row2["primary_phone"]))
    desc_score = token_sort_ratio(row1["short_description"], row2["short_description"])

    # Force 100 if very high similarity in key fields
    if (
        name_score >= 98 or
        comm_score >= 98 or
        comm_score_2 >= 98 or
        domain_score >= 98 
      
    ):
        return 100

    # Overlap domain tokens
    domains_1 = set(str(row1["domains"]).split())
    domains_2 = set(str(row2["domains"]).split())
    domain_overlap_bonus = 5 if len(domains_1.intersection(domains_2)) > 0 else 0

    # Overlap website URLs
    urls_1 = set(str(row1["website_url"]).split())
    urls_2 = set(str(row2["website_url"]).split())
    url_overlap_bonus = 5 if len(urls_1.intersection(urls_2)) > 0 else 0

    # Social media overlap bonus + forced match if extremely similar
    social_fields = ["facebook_url", "linkedin_url", "twitter_url", "instagram_url"]
    social_1 = set([str(row1[field]) for field in social_fields])
    social_2 = set([str(row2[field]) for field in social_fields])
    social_overlap_bonus = 5 if len(social_1.intersection(social_2)) > 0 else 0

    for s1 in social_1:
        for s2 in social_2:
            if token_sort_ratio(s1, s2) >= 98 and s1 != "" and s2 != "":
                return 100

    return (
        0.35 * name_score +
        0.25 * comm_score +
        0.15 * domain_score +
        0.05 * phone_score +
        0.15 * desc_score +
        domain_overlap_bonus +
        url_overlap_bonus +
        social_overlap_bonus
    )

# 5. Build similarity graph
G = nx.Graph()
G.add_nodes_from(df.index)

print(" Starting fuzzy matching with improved scoring and blocking...")

# 6. Match within relaxed blocking key
for block_value, block_df in tqdm(df.groupby("block_key")):
    indices = block_df.index.tolist()
    for i, j in combinations(indices, 2):
        score = compute_similarity_score(df.loc[i], df.loc[j])
        if score >= 85:
            G.add_edge(i, j)

# 7. Additional matching by identical company_name
for name_value, name_df in tqdm(df.groupby("name_group")):
    indices = name_df.index.tolist()
    if len(indices) > 1:
        for i, j in combinations(indices, 2):
            score = compute_similarity_score(df.loc[i], df.loc[j])
            if score >= 85:
                G.add_edge(i, j)

# 8. Assign group_id to connected components
groups = list(nx.connected_components(G))
group_map = {idx: gid for gid, group in enumerate(groups) for idx in group}
df["group_id"] = df.index.map(group_map).fillna(-1).astype(int)

# 9. Select most complete record per group
df["completeness_score"] = df.apply(lambda row: sum([1 for val in row if val != ""]), axis=1)
representatives = df.sort_values("completeness_score", ascending=False).drop_duplicates(subset=["group_id"])
representatives = representatives.drop(columns=["completeness_score", "block_key", "name_group"])

# 10. Save output
df.to_csv("Results/all_companies_with_group_id.csv", index=False)
representatives.to_csv("Results/unique_companies.csv", index=False)

print(" Deduplication complete.")

