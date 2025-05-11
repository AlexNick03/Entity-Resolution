# Entity-Resolution
Veridion Deeptech Engineer Internship Challenge

This project addresses the problem of **entity resolution** — detecting and grouping duplicate records that refer to the same real-world company, even if their data varies slightly. The dataset consists of ~35k companies from different sources, which leads to inconsistencies in names, contact details, and web presence.

The goal was to build a system that assigns a group to each record, ensuring that all entries referring to the same company are grouped together and that the unique companies would be identified.

---
## 📌 Project overview

- Load and normalize company data from `.parquet` using **pandas**
- Select relevant features (name, domain, phone, email, location)
- Preprocess text fields (**lowercase, strip, remove noise**)
- Compute pairwise similarity using **RapidFuzz (token_sort_ratio)**
- Block comparisons by **country + city** to improve performance
- Build graph of similar entries and group using **NetworkX components**
- Pick the most complete company per group as canonical record
---
## ⚙️ Setup & Dependencies

### 📦 Install required libraries

To run this fuzzy deduplication pipeline, you need the following Python libraries:

#### 🔧 Core Data Handling
```bash
pip install pandas tqdm
```
### 🧠 Fuzzy Matching
```bash
pip install rapidfuzz
```
### 🔗 Graph-Based Grouping
```bash
pip install networkx
```
### 📚 reading Parquet files
```bash
pip install pyarrow fastparquet
```
## 📂 Project Structure

This project is organized into three main modules, each with a specific role in the fuzzy deduplication pipeline:

### 🔹 `parquet_reader.py`
- Loads the original `.parquet` file
- Converts it to `.csv` format using `pandas`
- Handles missing values and prepares raw input

### 🔹 `data_processing.py`
- Selects the most relevant columns for deduplication
- Applies normalization (lowercase, strip, cleanup) to key fields like name, domain, phone, email, city
- Outputs a clean `normalized_companies.csv` file

### 🔹 `deduplicate.py`
- Applies a blocking strategy to limit comparisons (by `country + city`)
- Computes fuzzy similarity scores using `RapidFuzz`
- Builds a similarity graph with `NetworkX`
- Groups duplicate records and assigns a `group_id`
- Selects the most complete record per group and saves final outputs
## 🧠 Working Process

This section describes each module of the project, the logic behind it, and exactly what was implemented.

---

### 🟦 `parquet_reader.py`

The first thing I encountered was that the dataset was provided in `.parquet` format. Although Parquet is efficient for storage, it's not very convenient when you want to quickly explore or manipulate the data during development.

To work more comfortably, I decided to convert the file to `.csv`. I used `pandas.read_parquet()` and made sure it works with both `pyarrow` and `fastparquet`, so that the script is compatible across environments.

To inspect the `.csv` more easily, I used the **Edit CSV** extension in **Visual Studio Code**, which helped me visually understand the structure and spot inconsistencies.

Additionally, before converting, I used an **online parquet viewer** to double-check that the dataset was loading properly and to preview the schema directly in-browser — just to be sure everything was readable.

> ✔ What I did:
- Loaded the `.parquet` file with `pandas.read_parquet()`
- Supported both `pyarrow` and `fastparquet` engines
- Inspected the resulting `.csv` using the Edit CSV extension in VS Code
- Used an online viewer to preview the `.parquet` structure
- Exported the dataset as `veridion_entity_resolution_challenge.csv` for further processing



---
