{
  "channel": "#alerts-dev-test",
  "blocks": [
    {
      "type": "header",
      "text": {
        "type": "plain_text",
        "text": "{{ dag_run.data_interval_start.strftime('%A') }}'s Trends - {{ dag_run.data_interval_start.strftime('%Y-%m-%d') }} ( vs {{ last_week_date.strftime('%Y-%m-%d') }})",
        "emoji": true
      }
    }
  ]
}
