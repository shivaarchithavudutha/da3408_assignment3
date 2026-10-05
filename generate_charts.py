import matplotlib.pyplot as plt

frameworks = ['Apache Spark (3.5.1)', 'Ray Data (2.59.0)']
wall_clock_times = [492.91, 276.94]
peak_memory_gb = [2.2, 1.4]

plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.8))

# 1. Total Wall-Clock Execution Time
bars1 = ax1.bar(frameworks, wall_clock_times, color=['#d9534f', '#428bca'], width=0.5, edgecolor='black', linewidth=1)
ax1.set_title('Total Pipeline Execution Time (Lower is Better)', fontsize=11, fontweight='bold', pad=12)
ax1.set_ylabel('Wall-Clock Seconds', fontsize=10)
ax1.set_ylim(0, 560)
for bar in bars1:
    yval = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2.0, yval + 10, f"{yval:.2f}s\n({yval/60:.1f} min)", ha='center', va='bottom', fontweight='bold', fontsize=9.5)

# 2. Peak Memory Footprint
bars2 = ax2.bar(frameworks, peak_memory_gb, color=['#c9302c', '#3071a9'], width=0.5, edgecolor='black', linewidth=1)
ax2.set_title('Peak Memory Footprint (Lower is Better)', fontsize=11, fontweight='bold', pad=12)
ax2.set_ylabel('Gigabytes (GB)', fontsize=10)
ax2.set_ylim(0, 3.0)
for bar in bars2:
    yval = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2.0, yval + 0.08, f"{yval:.1f} GB", ha='center', va='bottom', fontweight='bold', fontsize=9.5)

plt.tight_layout()
plt.savefig('/home/vboxuser/da3408_a3_scale/benchmark_charts.png', dpi=300)
print("Charts successfully saved to /home/vboxuser/da3408_a3_scale/benchmark_charts.png")
