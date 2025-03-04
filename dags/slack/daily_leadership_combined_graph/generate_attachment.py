import io

import matplotlib.pyplot as plt


def generate_attachment(context, df):
    """
    Generates bar and line plots for revenue and total orders comparison over the last 18 months.

    :param df: Pandas DataFrame containing SQL results.
    :return: In-memory file containing the plot.
    """
    fig, (ax1, ax3) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    ax2 = ax1.twinx()
    ax4 = ax3.twinx()

    # Ensure data is sorted by date
    df = df.sort_values(by="yearmonth")

    # Extract last 18 months of data
    df = df.tail(18)

    # Convert yearmonth to string for better x-axis labels
    df["yearmonth"] = df["yearmonth"].astype(str)

    # Ensure revenue and orders values are not None
    revenue = df["current_revenue"].fillna(0).tolist()
    revenue_to_date = df["current_revenue_to_date"].fillna(0).tolist()
    last_month_revenue_to_date = df["last_month_revenue_to_date"].fillna(0).tolist()
    orders = df["current_orders"].fillna(0).tolist()
    orders_to_date = df["current_orders_to_date"].fillna(0).tolist()
    last_month_orders_to_date = df["last_month_orders_to_date"].fillna(0).tolist()

    # Ensure growth values are handled properly
    mom_growth = df["mom_growth"].fillna(0).tolist()
    yoy_growth = df["yoy_growth"].fillna(0).tolist()
    mom_orders_growth = df["mom_orders_growth"].fillna(0).tolist()
    yoy_orders_growth = df["yoy_orders_growth"].fillna(0).tolist()

    x = range(len(df))

    # Plot revenue as bars
    ax1.bar(x, revenue, color="royalblue", alpha=0.7, label="Total Revenue")
    ax1.bar(x, revenue_to_date, color="deepskyblue", alpha=0.7, label="Revenue To Date")
    ax1.bar(x, last_month_revenue_to_date, color="lightblue", alpha=0.7, label="Last Month To Date")

    # Secondary axis for growth percentage on revenue
    ax2.plot(x, mom_growth, color="green", marker="o", linestyle="-", label="MoM Revenue Growth")
    ax2.plot(x, yoy_growth, color="red", marker="o", linestyle="-", label="YoY Revenue Growth")

    # Plot orders as bars
    ax3.bar(x, orders, color="darkorange", alpha=0.7, label="Total Orders")
    ax3.bar(x, orders_to_date, color="gold", alpha=0.7, label="Orders To Date")
    ax3.bar(x, last_month_orders_to_date, color="khaki", alpha=0.7, label="Last Month To Date")

    # Secondary axis for growth percentage on orders
    ax4.plot(x, mom_orders_growth, color="green", marker="o", linestyle="-", label="MoM Orders Growth")
    ax4.plot(x, yoy_orders_growth, color="red", marker="o", linestyle="-", label="YoY Orders Growth")

    ax1.set_ylabel("Revenue ($)", color="royalblue")
    ax2.set_ylabel("Growth (%)", color="crimson")
    ax3.set_ylabel("Total Orders", color="darkorange")
    ax4.set_ylabel("Growth (%)", color="crimson")

    ax3.set_xlabel("Year-Month")
    ax1.set_xticks(x)
    ax3.set_xticks(x)
    ax3.set_xticklabels(df["yearmonth"], rotation=45, ha="right")

    ax1.set_title("Revenue and Growth Trend (Last 18 Months)")
    ax3.set_title("Total Orders and Growth Trend (Last 18 Months)")

    ax1.grid(axis="y", linestyle="--", alpha=0.6)
    ax3.grid(axis="y", linestyle="--", alpha=0.6)

    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    ax3.legend(loc="upper left")
    ax4.legend(loc="upper right")

    fig.tight_layout()

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png", bbox_inches="tight")
    img_bytes.seek(0)
    plt.close()

    return img_bytes
