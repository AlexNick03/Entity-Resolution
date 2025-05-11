import pandas as pd
input_file = "veridion_entity_resolution_challenge.snappy.parquet"
df = pd.read_parquet(input_file)

output_file = "veridion_entity_resolution_challenge.csv"
df.to_csv(output_file, index=False)

print(f"Fișierul a fost salvat ca: {output_file}")