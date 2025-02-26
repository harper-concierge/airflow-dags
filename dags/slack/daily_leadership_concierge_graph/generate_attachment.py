import io

import matplotlib.pyplot as plt


def generate_attachment(context, df):
    """
    Generates a line plot for the given DataFrame.

    :param df: Pandas DataFrame containing SQL results.
    :return: In-memory file containing the plot.
    """
    plt.figure(figsize=(12, 6))

    if "current_year_date" in df and "num_orders_paid_current" in df:
        plt.plot(df["current_year_date"], df["num_orders_paid_current"], label="Current Year Orders Paid", marker="o")
    if "last_year_date" in df and "num_orders_paid_last_year" in df:
        plt.plot(df["last_year_date"], df["num_orders_paid_last_year"], label="Last Year Orders Paid", marker="o")

    plt.xlabel("Date")
    plt.ylabel("Orders Paid")
    plt.title("Orders Paid Comparison: Last 2 Months vs Last Year")
    plt.legend()
    plt.xticks(rotation=45)

    img_bytes = io.BytesIO()
    plt.savefig(img_bytes, format="png", bbox_inches="tight")
    img_bytes.seek(0)
    plt.close()

    return img_bytes
