from __future__ import annotations
import json
from typing import TYPE_CHECKING, Any, Mapping, Iterable, Sequence

from tabulate import tabulate
from airflow.exceptions import AirflowException
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.slack.transfers.base_sql_to_slack import BaseSqlToSlackOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

ICON_URL: str = "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static/pin_100.png"


class SqlToSlackWebhookOperator(BaseSqlToSlackOperator):
    """
    Executes an SQL query and sends the results to Slack Incoming Webhook.

    This operator renders a Slack message or blocks using a Jinja2 templated Slack JSON configuration
    (provided in the `slack_config` parameter). The Slack message can be constructed using the query results
    as a Pandas dataframe (`results_df`) and formatted with the Tabulate library for better display.

    .. note::
        This operator works specifically with Slack Incoming Webhook connections, not the Slack API connection.

    :param sql: SQL query to be executed (templated).
    :param sql_conn_id: Reference to a specific database connection.
    :param slack_conn_id: Slack Incoming Webhook connection ID.
    :param sql_hook_params: Extra parameters to pass to the SQL hook.
    :param slack_base_url: Base URL for the Slack API, if different from the default.
    :param slack_config: Jinja2-templated JSON configuration for Slack message or blocks (templated).
    :param results_df_name: Name of the Pandas DataFrame variable for the query results, default is 'results_df'.
    :param parameters: Parameters to pass to the SQL query.
    :param slack_timeout: Timeout for the Slack API request, default is 30 seconds.
    :param slack_proxy: Proxy to use for Slack API requests, if any.
    :param slack_retry_handlers: List of retry handlers for Slack API requests.
    """

    # Fields and extensions for templating
    template_fields: Sequence[str] = ("sql", "slack_config")
    template_ext: Sequence[str] = (".sql", ".jinja", ".j2", ".tpl")
    template_fields_renderers = {"sql": "sql", "slack_config": "jinja"}
    times_rendered = 0

    def __init__(
        self,
        *,
        sql: str,
        sql_conn_id: str,
        slack_conn_id: str = SlackHook.default_conn_name,
        sql_hook_params: dict | None = None,
        slack_base_url: str | None = None,
        slack_config: str,
        results_df_name: str = "results_df",
        parameters: list | tuple | Mapping[str, Any] | None = None,
        slack_timeout: int = 30,
        slack_proxy: str | None = None,
        slack_retry_handlers: list | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            sql=sql, sql_conn_id=sql_conn_id, sql_hook_params=sql_hook_params, parameters=parameters, **kwargs
        )

        self.slack_conn_id = slack_conn_id
        self.slack_base_url = slack_base_url
        self.slack_config = slack_config
        self.results_df_name = results_df_name
        self.slack_timeout = slack_timeout
        self.slack_proxy = slack_proxy
        self.slack_retry_handlers = slack_retry_handlers
        self.kwargs = kwargs

    def _render_and_send_slack_message(self, context, df) -> None:
        """
        Render the Slack configuration and send the message to Slack.

        :param context: Airflow's execution context.
        :param df: Pandas DataFrame containing the SQL query results.
        """
        # Add the DataFrame to the context for Jinja2 rendering
        context[self.results_df_name] = df

        # Ensure slack_config is rendered with context
        self.render_template_fields(context)
        print(self.slack_config)

        # Parse the already rendered `slack_config` as JSON
        try:
            slack_json = json.loads(self.slack_config)
        except json.JSONDecodeError as e:
            raise AirflowException(f"Error parsing Slack config JSON: {e}")

        # Ensure at least one of 'blocks' or 'text' is present
        blocks = slack_json.get("blocks")
        text = slack_json.get("text")
        if not blocks and not text:
            raise AirflowException("Slack config must include either 'blocks' or 'text'.")

        # Extract channel if present
        channel = slack_json.get("channel")

        # Send the Slack message
        self._send_slack_message(channel, blocks=blocks, text=text)

    def _send_slack_message(self, channel, blocks=None, text=None) -> None:
        """
        Send a message to a Slack channel.

        :param channel: Slack channel to send the message to.
        :param blocks: JSON array of blocks to send.
        :param text: Plain-text message to send.
        """

        if blocks is None:
            blocks = []

        # Log the message for debugging purposes
        self.log.info("Sending Slack message: %s", {"text": text, "blocks": blocks})

        # Get SlackHook instance
        slack_hook = self._get_slack_hook()

        # Construct API call parameters
        api_call_params = {
            "channel": channel,
            "username": "HarperStats",
            "icon_url": ICON_URL,
        }
        if text:
            api_call_params["text"] = text
        if blocks:
            api_call_params["blocks"] = blocks

        # Make the API call
        slack_hook.call("chat.postMessage", json=api_call_params)

    def _get_slack_hook(self) -> SlackHook:
        """
        Retrieve the SlackHook instance.

        :return: SlackHook instance configured with connection details.
        """
        return SlackHook(
            slack_conn_id=self.slack_conn_id,
            base_url=self.slack_base_url,
            timeout=self.slack_timeout,
            proxy=self.slack_proxy,
            retry_handlers=self.slack_retry_handlers,
        )

    def render_template_fields(self, context, jinja_env=None) -> None:
        """
        Render template fields, ensuring proper Jinja2 rendering of Slack configuration and SQL query.

        :param context: Airflow's execution context.
        :param jinja_env: Custom Jinja2 environment (optional).
        """
        if self.times_rendered == 0:
            # On the first render, exclude slack_message since query results are not yet retrieved
            fields_to_render: Iterable[str] = (x for x in self.template_fields if x != "slack_config")
        else:
            fields_to_render = self.template_fields

        # Use the default Jinja environment if none is provided
        if not jinja_env:
            jinja_env = self.get_template_env()

        # Add Tabulate library to the Jinja environment for table rendering
        jinja_env.filters["tabulate"] = tabulate

        # Render the template fields
        self._do_render_template_fields(self, fields_to_render, context, jinja_env, set())
        self.times_rendered += 1

    def execute(self, context: Context) -> None:
        """
        Execute the operator by retrieving SQL query results and sending them to Slack.

        :param context: Airflow's execution context.
        """
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise AirflowException("Expected 'sql' parameter is missing or invalid.")
        if not self.slack_config.strip():
            raise AirflowException("Expected 'slack_config' parameter is missing.")

        # Get the query results as a Pandas DataFrame
        df = self._get_query_results()

        # Render and send the Slack message
        print(df)
        self._render_and_send_slack_message(context, df)

        # Log the completion of the task
        self.log.debug("Finished sending SQL data to Slack")
