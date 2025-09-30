# DBT Project Overview

This document outlines the structure, purpose, and best practices for our dbt project.
It is intended for LLM-assisted documentation, query generation, and analytics support.

---

## 1. Staging Layer
- **Purpose:** Add brand new raw data.
- **Organization:** Subdirectories per data source (e.g., Google Search Console, LinkedIn Ads, Salesforce, Reddit Ads, etc.).
- **Processing:**
  - Minimal transformations.
  - Primarily **renaming fields**.
  - No additional logic applied at this stage.
- **Best Practice:** Before creating a new file, **check if one already exists** to avoid duplication.

---

## 2. Models Layer
- **Purpose:** Add context and reusable logic.
- **Characteristics:** Generally a **one-to-one relationship** with the staging layer.
- **Processing:**
  - Add **case statements** and other reusable logic.
  - Apply **filters** to exclude unnecessary data.
- **Output:** Feeds into the **core layer**.
- **Best Practice:** Before creating a new file, **check if one already exists** to avoid duplication.

---

## 3. Core Layer
- **Purpose:** Create **dimension and fact tables**.
- **Processing:**
  - Consolidates and standardizes data.
  - Enables reusability for downstream layers.
- **Output:** Feeds into the **reporting layer**.

---

## 4. Data Warehouse (DWH) Reporting Layer
- **Purpose:** Provide **final curated data** for reporting and analytics.
- **Access:**
  - These are the **metrics exposed to end users**.
  - Other layers are **not directly exposed**.
- **Maintenance:**
  - Anytime files are updated here, the **associated YAML file must also be updated**.
  - Column definitions in YAML are **pushed into both the LLM and BI tools**, ensuring consistent definitions across systems.
- **Best Practice:** Prefer adding **new columns to existing tables** instead of creating new tables, unless the **grain or topic** justifies a new one.

---

## General Practices
- When joining, prefer to go only **one layer down**.
- Update **YAML files** whenever reporting layer models are changed.
- Column definitions must stay in sync with LLM + BI tools.

---

## Restricted Areas (Do Not Add New Files)
- **Intermediate Layer**
- **Mart Directory**
- **Maps Directory**

These areas contain **historic legacy code** that will be consolidated in the future.
No new files should be added here.

---

## LLM Usage Notes
- Use this document as the **source of truth** for dbt structure.
- When generating queries, respect **layer boundaries**.
- Only expose metrics from the **reporting layer**.
- Ensure **column definitions** align with YAML specifications.
- Do **not** create or suggest new files in the restricted areas.
- Avoid **duplicate files** in staging and models.
- Prefer **adding columns** to existing reporting tables when possible.

---

## Example Flow
**Raw Source → Staging → Models → Core → Reporting**

---