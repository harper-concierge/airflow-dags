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
}


def generate_attachment(context, df):
    print("Available columns:", df.columns.tolist())  # Debugging

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    df = df.sort_values(by="yearmonth").tail(18)  # Ensure last 18 months are used
    df["month_name"] = pd.to_datetime(df["yearmonth"]).dt.strftime("%b %Y")

    # Revenue & Orders Data
    concierge_revenue = df["current_concierge_revenue"].fillna(0).tolist()
    try_revenue = df["current_try_revenue"].fillna(0).tolist()
    total_revenue = [c + t for c, t in zip(concierge_revenue, try_revenue)]

    concierge_orders = df["current_concierge_orders"].fillna(0).tolist()
    try_orders = df["current_try_orders"].fillna(0).tolist()
    total_orders = [c + t for c, t in zip(concierge_orders, try_orders)]

    # Current month and previous months' progress
    concierge_revenue_to_date = df["current_concierge_revenue_to_date"].fillna(0).tolist()
    try_revenue_to_date = df["current_try_revenue_to_date"].fillna(0).tolist()
    previous_concierge_revenue_to_date = df["previous_concierge_revenue_to_date"].fillna(0).tolist()
    previous_try_revenue_to_date = df["previous_try_revenue_to_date"].fillna(0).tolist()

    concierge_orders_to_date = df["current_concierge_orders_to_date"].fillna(0).tolist()
    try_orders_to_date = df["current_try_orders_to_date"].fillna(0).tolist()
    previous_concierge_orders_to_date = df["previous_concierge_orders_to_date"].fillna(0).tolist()
    previous_try_orders_to_date = df["previous_try_orders_to_date"].fillna(0).tolist()

    x = range(len(df))
    width = 0.4
    offset = width / 3

    # Revenue Plot
    ax1.bar(x, total_revenue, width, color=COLOR_PALETTE["total_revenue"], alpha=0.3, label="Total Revenue")
    ax1.bar(
        [i - offset for i in x],
        concierge_revenue,
        width / 2,
        color=COLOR_PALETTE["concierge_revenue"],
        alpha=1,
        label="Concierge Revenue",
    )
    ax1.bar(
        [i + offset for i in x],
        try_revenue,
        width / 2,
        color=COLOR_PALETTE["try_revenue"],
        alpha=1,
        label="Try Revenue",
    )

    # Current & Previous Month Progress
    ax1.bar(
        [i - offset for i in x],
        concierge_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_to_date"],
        alpha=1,
        label="Concierge To Date",
    )
    ax1.bar(
        [i + offset for i in x],
        try_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["try_to_date"],
        alpha=1,
        label="Try To Date",
    )

    ax1.bar(
        [i - offset for i in x],
        previous_concierge_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_to_date"],
        alpha=1,
        label="Previous Concierge To Date",
    )
    ax1.bar(
        [i + offset for i in x],
        previous_try_revenue_to_date,
        width / 2,
        color=COLOR_PALETTE["try_to_date"],
        alpha=1,
        label="Previous Try To Date",
    )

    ax1.set_ylabel("Revenue ($)", color="royalblue")
    ax1.set_xlabel("Year-Month")
    ax1.set_xticks(x)
    pd.set_option("display.max_colwidth", 3)

    ax1.set_xticklabels(df["month_name"], rotation=90, ha="center")

    ax1.legend(loc="upper left", fontsize="small")

    # Orders Plot
    ax2.bar(x, total_orders, width, color=COLOR_PALETTE["total_orders"], alpha=0.3, label="Total Orders")
    ax2.bar(
        [i - offset for i in x],
        concierge_orders,
        width / 2,
        color=COLOR_PALETTE["concierge_orders"],
        alpha=1,
        label="Concierge Orders",
    )
    ax2.bar(
        [i + offset for i in x],
        try_orders,
        width / 2,
        color=COLOR_PALETTE["try_orders"],
        alpha=1,
        label="Try Orders",
    )

    ax2.bar(
        [i - offset for i in x],
        concierge_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_orders_to_date"],
        alpha=1,
        label="Concierge Orders To Date",
    )
    ax2.bar(
        [i + offset for i in x],
        try_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["try_orders_to_date"],
        alpha=1,
        label="Try Orders To Date",
    )

    ax2.bar(
        [i - offset for i in x],
        previous_concierge_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["concierge_orders_to_date"],
        alpha=1,
        label="Previous Concierge Orders To Date",
    )
    ax2.bar(
        [i + offset for i in x],
        previous_try_orders_to_date,
        width / 2,
        color=COLOR_PALETTE["try_orders_to_date"],
        alpha=1,
        label="Previous Try Orders To Date",
    )

    ax2.set_ylabel("Orders", color="darkorange")
    ax2.set_xlabel("Year-Month")
    ax2.legend(loc="upper left", fontsize="small")

    fig.tight_layout()

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png", bbox_inches="tight")
    img_bytes.seek(0)
    plt.close()

    return img_bytes
