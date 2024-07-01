# %%
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams.update({"font.size": 18, "font.family": "Times New Roman"})

# Data
columns = ["Value of time", "Operating cost", "Toll cost"]
base_scenario = [131681349, 32100025, 572774]
flooded_scenario = [162024580, 38400932, 725656]

# Differences
differences = np.array(flooded_scenario) - np.array(base_scenario)

# Plot
x = np.arange(len(columns))
width = 0.35

fig, ax1 = plt.subplots(figsize=(10, 8))

# Bars for the base and flooded scenarios
bars1 = ax1.bar(x - width / 2, base_scenario, width, label="Base Scenario")
bars2 = ax1.bar(x + width / 2, flooded_scenario, width, label="Flooded Scenario")

# Line plot for the differences
ax2 = ax1.twinx()
line = ax2.plot(
    x,
    differences,
    color="black",
    marker="o",
    linestyle="-",
    linewidth=2,
    label="Flood-induced Cost Escalation",
)

# Adding labels, title, and legend
ax1.set_xlabel("Cost Components")
ax1.set_ylabel("Cost Values")
ax2.set_ylabel("Increased Cost Values")
ax1.set_title(
    "Cost Comparison: Base vs Flooded Scenario", fontsize=20, fontweight="bold", pad=10
)
ax1.set_xticks(x)
ax1.set_xticklabels(columns)

# Annotate the difference
for i, diff in enumerate(differences):
    ax2.annotate(
        f"{diff}",
        xy=(i, diff),
        xytext=(5, 5),
        textcoords="offset points",
        color="black",
    )

# Add legends
lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines + lines2, labels + labels2, loc="upper right")

# Enhance plot readability
ax1.tick_params(axis="y")
ax2.tick_params(axis="y")

# Display plot
plt.show()
