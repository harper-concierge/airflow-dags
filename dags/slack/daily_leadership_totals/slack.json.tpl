{
  "channel": "#harperhq",
  "blocks": [
    {
      "type": "header",
      "text": {
        "type": "plain_text",
        "text": "{{ dag_run.data_interval_start.strftime('%A') }}'s Revenue & Orders - {{ dag_run.data_interval_start.strftime('%Y-%m-%d') }} (vs {{ last_week_date.strftime('%Y-%m-%d') }})",
        "emoji": true
      }
    },
    {
      "type": "section",
      "text": {
        "type": "mrkdwn",
        "text": "{{ ':chart_with_upwards_trend:' if results_df.iloc[0].concierge_revenue_diff > 0 else ':chart_with_downwards_trend:' }} Concierge Revenue: *£{{ '{:,.2f}'.format(results_df.iloc[0].concierge_revenue) }}* ({{ '{:,.2f}'.format(results_df.iloc[0].concierge_revenue_diff) }} | {{ results_df.iloc[0].concierge_revenue_change_percent }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].try_revenue_diff > 0 else ':chart_with_downwards_trend:' }} Try Revenue: *£{{ '{:,.2f}'.format(results_df.iloc[0].try_revenue) }}* ({{ '{:,.2f}'.format(results_df.iloc[0].try_revenue_diff) }} | {{ results_df.iloc[0].try_revenue_change_percent }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].total_revenue_diff > 0 else ':chart_with_downwards_trend:' }} Total Revenue: *£{{ '{:,.2f}'.format(results_df.iloc[0].total_revenue) }}* ({{ '{:,.2f}'.format(results_df.iloc[0].total_revenue_diff) }} | {{ results_df.iloc[0].total_revenue_change_percent }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].concierge_orders_diff > 0 else ':chart_with_downwards_trend:' }} Concierge Orders: *{{ results_df.iloc[0].concierge_orders }}* ({{ results_df.iloc[0].concierge_orders_diff }} | {{ results_df.iloc[0].concierge_orders_change_percent }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].try_orders_diff > 0 else ':chart_with_downwards_trend:' }} Try Orders: *{{ results_df.iloc[0].try_orders }}* ({{ results_df.iloc[0].try_orders_diff }} | {{ results_df.iloc[0].try_orders_change_percent }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].total_orders_diff > 0 else ':chart_with_downwards_trend:' }} Total Orders: *{{ results_df.iloc[0].total_orders }}* ({{ results_df.iloc[0].total_orders_diff }} | {{ results_df.iloc[0].total_orders_change_percent }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].concierge_revenue_per_order_diff > 0 else ':chart_with_downwards_trend:' }} Concierge Revenue per Order: *£{{ '{:,.2f}'.format(results_df.iloc[0].concierge_revenue_per_order) }}* ({{ '{:,.2f}'.format(results_df.iloc[0].concierge_revenue_per_order_diff) }} | {{ results_df.iloc[0].concierge_revenue_per_order_change_percent }}%)\n\n{{ ':chart_with_upwards_trend:' if results_df.iloc[0].try_revenue_per_order_diff > 0 else ':chart_with_downwards_trend:' }} Try Revenue per Order: *£{{ '{:,.2f}'.format(results_df.iloc[0].try_revenue_per_order) }}* ({{ '{:,.2f}'.format(results_df.iloc[0].try_revenue_per_order_diff) }} | {{ results_df.iloc[0].try_revenue_per_order_change_percent }}%)"
      }
    }
  ]
}
