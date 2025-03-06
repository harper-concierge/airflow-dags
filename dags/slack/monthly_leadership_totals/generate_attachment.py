import io

import pandas as pd
import matplotlib.pyplot as plt

# Define color lookup table
COLOR_PALETTE = {
    "total_revenue": "black",
    "concierge_revenue": "tab:blue",
    "try_revenue": "tab:purple",
    "concierge_to_date": "tab:cyan",
    "try_to_date": "tab:pink",
    "total_orders": "black",
    "concierge_orders": "darkorange",
    "try_orders": "forestgreen",
    "concierge_orders_to_date": "moccasin",
    "try_orders_to_date": "lightgreen",
    "growth_percentage": "red",
}


def generate_attachment(context, df):
    print("Available columns:", df.columns.tolist())  # Debugging

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12), sharex=True)
    # plt.subplots_adjust(bottom=0.2)  # Add bottom padding to prevent label overlap
    plt.subplots_adjust(hspace=0.5, bottom=0.2)

    df = df.sort_values(by="yearmonth").tail(24)  # Ensure last 24 months are used
    df["month_name"] = pd.to_datetime(df["yearmonth"]).dt.strftime("%b\n%Y")

    # Revenue & Orders Data (Converted to Pounds)
    concierge_revenue = (df["current_concierge_revenue"].fillna(0) / 100000).tolist()
    try_revenue = (df["current_try_revenue"].fillna(0) / 100000).tolist()
    total_revenue = [c + t for c, t in zip(concierge_revenue, try_revenue)]

    concierge_orders = df["current_concierge_orders"].fillna(0).tolist()
    try_orders = df["current_try_orders"].fillna(0).tolist()
    total_orders = [c + t for c, t in zip(concierge_orders, try_orders)]

    # Revenue & Orders To Date (Current & Previous)
    concierge_revenue_to_date = (df["current_concierge_revenue_to_date"].fillna(0) / 100000).tolist()
    try_revenue_to_date = (df["current_try_revenue_to_date"].fillna(0) / 100000).tolist()
    previous_concierge_revenue_to_date = (df["previous_concierge_revenue_to_date"].fillna(0) / 100000).tolist()
    previous_try_revenue_to_date = (df["previous_try_revenue_to_date"].fillna(0) / 100000).tolist()

    concierge_orders_to_date = df["current_concierge_orders_to_date"].fillna(0).tolist()
    try_orders_to_date = df["current_try_orders_to_date"].fillna(0).tolist()
    previous_concierge_orders_to_date = df["previous_concierge_orders_to_date"].fillna(0).tolist()
    previous_try_orders_to_date = df["previous_try_orders_to_date"].fillna(0).tolist()

    x = range(len(df))
    width = 0.4
    offset = width / 3

    # Restore: YoY Growth Calculation (Using Correct Previous Year Revenue)
    growth_percentage = [0] * 12  # First 12 months have no YoY comparison
    for i in range(12, len(df) - 1):  # Start at Month 13, EXCLUDE the last month (current month)
        prev_idx = i - 12  # Compare to the same month last year
        if total_revenue[prev_idx] > 0.3:  # Only compare months with revenue over £200
            growth = (total_revenue[i] - total_revenue[prev_idx]) / total_revenue[prev_idx] * 100
        else:
            growth = 0
        growth_percentage.append(growth)
        print(f"{i} {growth} {total_revenue[i]} {total_revenue[prev_idx]}")

    # Revenue Per Order Calculation
    revenue_per_order_concierge = [
        (rev / orders) * 1000 if orders > 0 else None for rev, orders in zip(concierge_revenue, concierge_orders)
    ]
    revenue_per_order_try = [
        (rev / orders) * 1000 if orders > 0 else None for rev, orders in zip(try_revenue, try_orders)
    ]

    # Revenue Plot
    ax1.bar(x, total_revenue, width, color=COLOR_PALETTE["total_revenue"], alpha=0.3, label="Total Revenue")
    ax1.bar(
        [i - offset for i in x],
        concierge_revenue,
        width / 2,
        color=COLOR_PALETTE["concierge_revenue"],
        label="Concierge Revenue",
    )
    ax1.bar([i + offset for i in x], try_revenue, width / 2, color=COLOR_PALETTE["try_revenue"], label="Try Revenue")

    # Revenue To Date
    ax1.bar(
        [i - offset for i in x],
        concierge_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_to_date"],
        label="Concierge To Date",
    )
    ax1.bar(
        [i + offset for i in x],
        try_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["try_to_date"],
        label="Try To Date",
    )
    ax1.bar(
        [i - offset for i in x],
        previous_concierge_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_to_date"],
        label="Previous Concierge To Date",
    )
    ax1.bar(
        [i + offset for i in x],
        previous_try_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["try_to_date"],
        label="Previous Try To Date",
    )

    ax1.set_ylabel("Revenue (£k)", color="royalblue")
    ax1.set_xlabel("Year-Month")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["month_name"], ha="center")
    ax1.legend(loc="upper left", fontsize="small")
    ax1.set_title("Monthly Revenue & YoY Growth", fontsize=14, fontweight="bold")
    ax1.tick_params(labelbottom=True)

    # Orders Plot
    ax2.bar(x, total_orders, width, color=COLOR_PALETTE["total_orders"], alpha=0.3, label="Total Orders")
    ax2.bar(
        [i - offset for i in x],
        concierge_orders,
        width / 2,
        color=COLOR_PALETTE["concierge_orders"],
        label="Concierge Orders",
    )
    ax2.bar([i + offset for i in x], try_orders, width / 2, color=COLOR_PALETTE["try_orders"], label="Try Orders")

    # Orders To Date
    ax2.bar(
        [i - offset for i in x],
        concierge_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_orders_to_date"],
        label="Concierge Orders To Date",
    )
    ax2.bar(
        [i + offset for i in x],
        try_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["try_orders_to_date"],
        label="Try Orders To Date",
    )
    ax2.bar(
        [i - offset for i in x],
        previous_concierge_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_orders_to_date"],
        label="Previous Concierge Orders To Date",
    )
    ax2.bar(
        [i + offset for i in x],
        previous_try_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["try_orders_to_date"],
        label="Previous Try Orders To Date",
    )

    ax2.set_ylabel("Orders", color="darkorange")
    ax2.set_xlabel("Year-Month")
    ax2.set_xticklabels(df["month_name"], ha="center")
    ax2.legend(loc="upper left", fontsize="small")
    ax2.set_title("Monthly Orders & Revenue Per Order", fontsize=14, fontweight="bold")

    # Revenue Per Order Line Plot
    ax5 = ax2.twinx()
    ax5.plot(
        x, revenue_per_order_concierge, color="blue", marker="s", linestyle="-", label="Revenue Per Concierge Order"
    )
    ax5.plot(x, revenue_per_order_try, color="purple", marker="o", linestyle="-", label="Revenue Per Try Order")
    ax5.set_ylabel("Revenue Per Order (£)", color="black")
    ax5.legend(loc="upper center", fontsize="small")

    # YoY Growth Line Plot
    ax3 = ax1.twinx()
    ax3.plot(
        x[:-1],  # Exclude current month
        growth_percentage,
        color=COLOR_PALETTE["growth_percentage"],
        marker="o",
        linestyle="--",
        label="YoY Growth %",
    )
    ax3.set_ylabel("YoY Growth %", color=COLOR_PALETTE["growth_percentage"])
    ax3.legend(loc="upper center", fontsize="small")

    fig.tight_layout()

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png", bbox_inches="tight")
    img_bytes.seek(0)
    plt.close()

    return img_bytes
