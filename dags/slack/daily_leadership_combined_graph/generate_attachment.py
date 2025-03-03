import io

import matplotlib.pyplot as plt


def generate_attachment(context, df):
    """
    Generates a bar and line plot for revenue comparison.

    :param df: Pandas DataFrame containing SQL results.
    :return: In-memory file containing the plot.
    """

    print("df", df)
    fig, ax1 = plt.subplots(figsize=(10, 6))

    categories = ["Last Year", "Last Month", "Current"]

    # Ensure revenue values are not None
    revenue = [
        df["last_year_revenue"][0] if "last_year_revenue" in df and not df["last_year_revenue"].isnull()[0] else 0,
        df["last_month_revenue"][0] if "last_month_revenue" in df and not df["last_month_revenue"].isnull()[0] else 0,
        df["current_revenue"][0] if "current_revenue" in df and not df["current_revenue"].isnull()[0] else 0,
    ]

    # Ensure growth values are handled properly to avoid NoneType issues
    mom_growth = df["mom_growth"][0] if "mom_growth" in df and not df["mom_growth"].isnull()[0] else 0
    yoy_growth = df["yoy_growth"][0] if "yoy_growth" in df and not df["yoy_growth"].isnull()[0] else 0

    growth_mom = [None, mom_growth, None]
    growth_yoy = [None, None, yoy_growth]

    x = range(len(categories))
    ax1.bar(x, revenue, color="royalblue", alpha=0.7, label="Revenue")

    ax2 = ax1.twinx()
    ax2.plot(
        x[1:],
        growth_mom[1:],
        color="green",
        marker="o",
        linestyle="-",
        label="MoM Growth",
    )
    ax2.plot(
        x[2:],
        growth_yoy[2:],
        color="red",
        marker="o",
        linestyle="-",
        label="YoY Growth",
    )

    ax1.set_xlabel("Time Period")
    ax1.set_ylabel("Revenue ($)", color="royalblue")
    ax2.set_ylabel("Growth (%)", color="crimson")
    ax1.set_xticks(x)
    ax1.set_xticklabels(categories)
    ax1.set_title("Monthly Revenue Comparison with Growth Rates")

    ax1.grid(axis="y", linestyle="--", alpha=0.6)
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png", bbox_inches="tight")
    img_bytes.seek(0)
    plt.close()

    return img_bytes
