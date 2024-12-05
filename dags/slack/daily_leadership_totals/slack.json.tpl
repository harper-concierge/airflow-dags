{
  "channel": "#harperhq",
  "blocks": [
    {
      "type": "header",
      "text": {
        "type": "plain_text",
        "text": "{{ dag_run.data_interval_start.strftime('%A') }}'s Orders - {{ dag_run.data_interval_start.strftime('%Y-%m-%d') }} ( vs {{ last_week_date.strftime('%Y-%m-%d') }})",
        "emoji": true
      }
    },
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "{{ ':chart_with_upwards_trend:' if results_df.iloc[0].appointments_diff > 0 else ':chart_with_downwards_trend:' }} Total Concierge Appointments Booked: *{{ results_df.iloc[0].num_appointments_booked }}* ({{ results_df.iloc[0].appointments_diff }} | {{ results_df.iloc[0].appointments_percent_change }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].try_orders_diff > 0 else ':chart_with_downwards_trend:' }} Total Try Orders Placed: *{{ results_df.iloc[0].num_try_orders_created }}* ({{ results_df.iloc[0].try_orders_diff }} | {{ results_df.iloc[0].try_orders_percent_change }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].orders_paid_diff > 0 else ':chart_with_downwards_trend:' }} Total Orders Paid Today: *{{ results_df.iloc[0].num_orders_paid }}* ({{ results_df.iloc[0].orders_paid_diff }} | {{ results_df.iloc[0].orders_paid_percent_change }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].items_purchased_diff > 0 else ':chart_with_downwards_trend:' }} Total Items Purchased: *{{ results_df.iloc[0].total_items_purchased }}* ({{ results_df.iloc[0].items_purchased_diff }} | {{ results_df.iloc[0].items_purchased_percent_change }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].value_ordered_diff_gbp > 0 else ':chart_with_downwards_trend:' }} Total Amount Ordered: *£{{ "{:,.2f}".format(results_df.iloc[0].total_value_ordered_gbp) }}* ({{ "{:,.2f}".format(results_df.iloc[0].value_ordered_diff_gbp) }} | {{ results_df.iloc[0].value_ordered_percent_change }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].amount_purchased_diff_gbp > 0 else ':chart_with_downwards_trend:' }} Total Amount Purchased: *£{{ "{:,.2f}".format(results_df.iloc[0].total_amount_purchased_gbp) }}* ({{ "{:,.2f}".format(results_df.iloc[0].amount_purchased_diff_gbp) }} | {{ results_df.iloc[0].amount_purchased_percent_change }}%)"
      }
    }
  ]
}
