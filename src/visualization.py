import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import os

# Professional finance style
sns.set_theme(style="whitegrid")

plt.rcParams.update({
    'font.size': 10,
    'axes.labelsize': 12,
    'axes.titlesize': 14,
    'legend.fontsize': 10,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10
})

os.makedirs("visual_outputs", exist_ok=True)

def save_and_show(fig, name):
    filename = f"visual_outputs/{name}.png"
    fig.savefig(filename, bbox_inches='tight', dpi=300)
    print(f"Saved: {filename}")
    plt.show()


def format_numbers(values):
    return [f"{int(v):,}" for v in values]


def simple_plot(aggregated_data):
    fig, ax = plt.subplots(figsize=(14, 7))

    avenues = aggregated_data['Investment_Avenues']
    mutual = aggregated_data['Total_Mutual_Funds']
    equity = aggregated_data['Total_Equity_Market']
    deb = aggregated_data['Total_Debentures']

    x = np.arange(len(avenues))
    width = 0.25

    bars1 = ax.bar(x - width, mutual, width, label='Mutual Funds', color='#2A9D8F')
    bars2 = ax.bar(x, equity, width, label='Equity Market', color='#E9C46A')
    bars3 = ax.bar(x + width, deb, width, label='Debentures', color='#F4A261')

    ax.set_title("Investment Allocation by Avenue", pad=12)
    ax.set_ylabel("Total Score")
    ax.set_xticks(x)
    ax.set_xticklabels(avenues)
    ax.legend()

    for bars in [bars1, bars2, bars3]:
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f"{height:,.0f}", 
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 5),
                        textcoords="offset points",
                        ha='center', va='bottom')

    fig.tight_layout()
    save_and_show(fig, "investment_simple_chart")


def comprehensive_dashboard_visualization(aggregated_data, full_dataset_path="Finance_data_augmented.csv"):
    df = pd.read_csv(full_dataset_path)

    fig = plt.figure(figsize=(18, 12))
    fig.suptitle("Investment Insight Dashboard", fontsize=16, y=1.03)

    # Subplot 1: Bar Comparison (Corrected instead of simple_plot)
    ax1 = fig.add_subplot(2, 2, 1)
    x = np.arange(len(aggregated_data))
    width = 0.25

    m = aggregated_data['Total_Mutual_Funds']
    e = aggregated_data['Total_Equity_Market']
    d = aggregated_data['Total_Debentures']

    ax1.bar(x - width, m, width, label='Mutual Funds', color='#2A9D8F')
    ax1.bar(x, e, width, label='Equity Market', color='#E9C46A')
    ax1.bar(x + width, d, width, label='Debentures', color='#F4A261')

    ax1.set_title('Investment by Avenue')
    ax1.set_xticks(x)
    ax1.set_xticklabels(aggregated_data['Investment_Avenues'])
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)

    for bar in ax1.containers:
        ax1.bar_label(bar, padding=3)

    # Subplot 2: Age Distribution
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.hist(df['age'], bins=15, edgecolor='black', alpha=0.7, color='#457B9D')
    ax2.set_title("Age Distribution")
    ax2.grid(axis='y', alpha=0.3)

    # Subplot 3: Gender Preference Heatmap
    ax3 = fig.add_subplot(2, 2, 3)
    heat_data = df.groupby("gender")[['Mutual_Funds', 'Equity_Market', 'Debentures']].mean()
    sns.heatmap(heat_data.T, annot=True, cmap="Greens", fmt=".1f", ax=ax3)
    ax3.set_title("Gender Preference Patterns")

    # Subplot 4: Decision Factor Pie
    ax4 = fig.add_subplot(2, 2, 4)
    factor_counts = df['Factor'].value_counts()
    ax4.pie(factor_counts.values,
            autopct='%1.1f%%',
            labels=factor_counts.index,
            pctdistance=0.85,
            colors=['#2A9D8F','#E76F51','#8AB17D'])
    ax4.set_title("Investment Decision Drivers")

    fig.tight_layout()
    save_and_show(fig, "dashboard_comprehensive")



def create_single_clean_dashboard(aggregated_data, full_dataset_path="Finance_data_augmented.csv"):
    df = pd.read_csv(full_dataset_path)

    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle("Financial Insights Dashboard", fontsize=16, y=0.98)

    x = np.arange(len(aggregated_data))

    # Chart 1: Investment breakdown
    axes[0][0].bar(x, aggregated_data['Count'], color='#4CAF50')
    axes[0][0].set_title("Number of Investors by Avenue")
    axes[0][0].set_xticks(x)
    axes[0][0].set_xticklabels(aggregated_data['Investment_Avenues'])

    # Chart 2: Age analysis
    axes[0][1].hist(df['age'], bins=15, edgecolor='black', alpha=0.8, color='#003049')
    axes[0][1].set_title("Age Spread")

    # Chart 3: Gender split
    g_counts = df['gender'].value_counts()
    axes[1][0].pie(g_counts.values, labels=g_counts.index, autopct='%1.1f%%',
                   colors=['#1D3557', '#E63946'], pctdistance=0.85)
    axes[1][0].set_title("Gender Distribution")

    # Chart 4: Expected returns perception
    e_counts = df['Expect'].value_counts()
    axes[1][1].bar(e_counts.index, e_counts.values, color='#6A4C93')
    axes[1][1].set_title("Return Expectations")
    axes[1][1].tick_params(axis='x', rotation=15)

    fig.tight_layout()
    save_and_show(fig, "dashboard_single_clean")
