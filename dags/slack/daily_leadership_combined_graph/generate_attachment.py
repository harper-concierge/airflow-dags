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
    plt.subplots_adjust(bottom=0.2)  # Add bottom padding to prevent label overlap

    df = df.sort_values(by="yearmonth").tail(24)  # Ensure last 24 months are used
    df["month_name"] = pd.to_datetime(df["yearmonth"]).dt.strftime("%b\n%Y")

    # Revenue & Orders Data (Converted to Pounds)
    concierge_revenue = (df["current_concierge_revenue"].fillna(0) / 1000).tolist()
    try_revenue = (df["current_try_revenue"].fillna(0) / 1000).tolist()
    total_revenue = [c + t for c, t in zip(concierge_revenue, try_revenue)]

    concierge_orders = df["current_concierge_orders"].fillna(0).tolist()
    try_orders = df["current_try_orders"].fillna(0).tolist()
    total_orders = [c + t for c, t in zip(concierge_orders, try_orders)]

    # Revenue & Orders To Date (Current & Previous)
    concierge_revenue_to_date = (df["current_concierge_revenue_to_date"].fillna(0) / 1000).tolist()
    try_revenue_to_date = (df["current_try_revenue_to_date"].fillna(0) / 1000).tolist()
    previous_concierge_revenue_to_date = (df["previous_concierge_revenue_to_date"].fillna(0) / 1000).tolist()
    previous_try_revenue_to_date = (df["previous_try_revenue_to_date"].fillna(0) / 1000).tolist()

    concierge_orders_to_date = df["current_concierge_orders_to_date"].fillna(0).tolist()
    try_orders_to_date = df["current_try_orders_to_date"].fillna(0).tolist()
    previous_concierge_orders_to_date = df["previous_concierge_orders_to_date"].fillna(0).tolist()
    previous_try_orders_to_date = df["previous_try_orders_to_date"].fillna(0).tolist()

    # Previous Year Revenue for Growth Calculation
    previous_total_revenue = [
        (p_c + p_t) / 1000
        for p_c, p_t in zip(
            df["previous_concierge_revenue_to_date"].fillna(0), df["previous_try_revenue_to_date"].fillna(0)
        )
    ]

    # Calculate Growth Percentage (Avoid Division by Zero)
    growth_percentage = [(c - p) / p * 100 if p > 0 else None for c, p in zip(total_revenue, previous_total_revenue)]

    # Calculate Revenue Per Order (Separate for Concierge and Try)
    revenue_per_order_concierge = [
        rev / orders if orders > 0 else None for rev, orders in zip(concierge_revenue, concierge_orders)
    ]
    revenue_per_order_try = [rev / orders if orders > 0 else None for rev, orders in zip(try_revenue, try_orders)]

    x = range(len(df))
    width = 0.4
    offset = width / 3

    # Revenue Plot
    ax1.bar(x, total_revenue, width, color=COLOR_PALETTE["total_revenue"], label="Total Revenue (£k)")
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

    # Orders Plot
    ax2.bar(x, total_orders, width, color=COLOR_PALETTE["total_orders"], label="Total Orders")
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
    ax2.legend(loc="upper left", fontsize="small")

    # Revenue Per Order Line Plot (Plotted on Orders graph)
    ax5 = ax2.twinx()
    ax5.plot(
        x, revenue_per_order_concierge, color="blue", marker="s", linestyle="-", label="Revenue Per Concierge Order"
    )
    ax5.plot(x, revenue_per_order_try, color="purple", marker="o", linestyle="-", label="Revenue Per Try Order")
    ax5.set_ylabel("Revenue Per Order (£)", color="black")
    ax5.legend(loc="lower right", fontsize="small")

    # Growth Percentage Line Plot
    ax3 = ax1.twinx()
    ax3.plot(
        x,
        growth_percentage,
        color=COLOR_PALETTE["growth_percentage"],
        marker="o",
        linestyle="--",
        label="YoY Growth %",
    )
    ax3.set_ylabel("YoY Growth %", color=COLOR_PALETTE["growth_percentage"])
    ax3.legend(loc="upper right", fontsize="small")

    fig.tight_layout()

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png", bbox_inches="tight")
    img_bytes.seek(0)
    plt.close()

    return img_bytes
