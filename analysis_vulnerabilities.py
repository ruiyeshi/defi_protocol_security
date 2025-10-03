import pandas as pd

# vulnerability–tool coverage matrix template
data = [
    # Vulnerability, Layer, Frequency (% of contracts), Slither, Mythril, Manticore, Smartian, Securify, OtherTool
    ["Reentrancy", "SC", 13.2, 1, 1, 1, 1, 0, 0],
    ["Access Control / Auth Error", "SC", 9.4, 1, 0, 1, 1, 1, 0],
    ["Price Oracle Manipulation", "PRO", 7.8, 0, 1, 1, 1, 0, 0],
    ["Unchecked Return Values", "SC", 5.2, 1, 1, 1, 0, 0, 0],
    ["Integer Overflow / Underflow", "SC", 3.1, 1, 1, 1, 1, 1, 0],
    ["Logic / Sanity Error", "SC", 2.4, 0, 0, 0, 0, 0, 0],
    ["Unprotected Self-Destruct", "SC", 1.9, 1, 1, 0, 1, 0, 0],
    ["Flash-loan Manipulation", "PRO", 1.5, 0, 1, 1, 0, 0, 0],
    ["Oracle Price Deviation", "PRO", 1.3, 0, 1, 1, 1, 0, 0],
    ["Unhandled Exception", "SC", 0.9, 1, 0, 0, 0, 1, 0],
]

cols = [
    "Vulnerability", "Layer", "Frequency(%)",
    "Slither", "Mythril", "Manticore", "Smartian", "Securify", "OtherTool"
]

df_vuln = pd.DataFrame(data, columns=cols)
df_vuln

# Step 2: Aggregate tool coverage and frequency statistics
df_vuln["ToolCoverage"] = df_vuln[["Slither","Mythril","Manticore","Smartian","Securify","OtherTool"]].sum(axis=1)
print("\n=== Tool coverage per vulnerability ===")
print(df_vuln[["Vulnerability","ToolCoverage"]])

print("\n=== Average frequency by layer ===")
print(df_vuln.groupby("Layer")["Frequency(%)"].mean())

import matplotlib.pyplot as plt
import seaborn as sns

# Step 3: Visualization
plt.figure(figsize=(10,6))
sns.barplot(data=df_vuln, x="Vulnerability", y="Frequency(%)", hue="Layer")
plt.xticks(rotation=70, ha="right")
plt.title("Vulnerability Frequency by Layer")
plt.tight_layout()
plt.show()