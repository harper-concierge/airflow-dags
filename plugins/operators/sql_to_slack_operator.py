from __future__ import annotations
import io
import json
import traceback
import importlib.util
from typing import TYPE_CHECKING, Any, Mapping, Iterable, Sequence
from datetime import timedelta

import pandas as pd
from tabulate import tabulate
from airflow.models import Variable
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

    Enhancements:
    - Supports attaching a graph generated from an external `generate_plot.py` script.
    - Dynamically imports and runs the `generate_plot(df)` function from the script.
    - Sends a file attachment to Slack if a plot is generated.

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
    :param generate_attachment_script: Path to an external Python script that defines `generate_plot(df)`.
    """

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
        generate_attachment_script: str | None = None,  # Path to external script for custom plot
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
        self.generate_attachment_script = generate_attachment_script  # External script for graph generation
        self.kwargs = kwargs  # Preserve any additional arguments

    def _render_and_send_slack_message(self, context, df) -> None:
        """
        Render the Slack configuration and send the message to Slack.

        :param context: Airflow's execution context.
        :param df: Pandas DataFrame containing the SQL query results.
        """
        # Add the DataFrame to the context for Jinja2 rendering
        context[self.results_df_name] = df
        context["last_week_date"] = context["dag_run"].data_interval_start - timedelta(days=7)

        print("Results", df)

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
        channel = "#alerts-dev-airflow"
        print("channel", channel)
        channel = slack_json.get("channel", channel)
        print("slack config channel", channel)
        channel = Variable.get("SLACK_OVERRIDE_CHANNEL", channel)
        print("After slack override channel", channel)

        # Generate graph if script is provided
        img_bytes = None
        if self.generate_attachment_script:
            img_bytes = self._execute_generate_attachement(context, df)

        self._send_slack_message(channel, blocks=blocks, text=text, img_bytes=img_bytes)

    def _send_slack_message(self, channel, blocks=None, text=None, img_bytes=None) -> None:
        """
        Send a message to a Slack channel with optional file attachment.

        :param channel: Slack channel to send the message to.
        :param blocks: JSON array of blocks to send.
        :param text: Plain-text message to send.
        :param img_bytes: In-memory file object for image upload.
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

        # https://stackoverflow.com/q/79226005
        # Send one or the other
        print("img_bytes", img_bytes)
        print("channel", channel)
        if img_bytes:
            # Ensure the pointer is reset to the start of the file
            img_bytes.seek(0)  # Reset file pointer
            print(f"img_bytes size: {img_bytes.getbuffer().nbytes if img_bytes else 'None'}")

            # Ensure image is not empty
            if img_bytes.getbuffer().nbytes == 0:
                raise ValueError("Generated image is empty.")
            img_response = slack_hook.call(
                "files.upload",
                data={
                    "channels": channel,
                    "filename": f"{self.task_id}.png",
                    "title": f"{self.task_id}",
                    "content": img_bytes.read(),
                },
            )
            print("img_response", img_response["file"]["shares"])
            slack_hook.call("chat.update", ts=img_response["ts"], json=api_call_params)
        else:
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

        print("context", context)
        if self.times_rendered == 0:
            # On the first render, exclude slack_message since query results are not yet retrieved
            fields_to_render: Iterable[str] = (x for x in self.template_fields if x != "slack_config")
        else:
            fields_to_render = self.template_fields

        print("fields_to_render", fields_to_render)

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

        print(df)
        self._render_and_send_slack_message(context, df)

        # Log the completion of the task
        self.log.debug("Finished sending SQL data to Slack")

    def _execute_generate_attachement(self, context, df: pd.DataFrame) -> io.BytesIO | None:
        """
        Loads and executes the `generate_plot(df)` function from an external Python script.

        :param df: DataFrame containing SQL query results.
        :return: In-memory image file if successful, otherwise None.
        """
        try:
            spec = importlib.util.spec_from_file_location("generate_attachment", self.generate_attachment_script)
            generate_attachment_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(generate_attachment_module)

            if not hasattr(generate_attachment_module, "generate_attachment"):
                raise ImportError("The script must define a `generate_attachment(df)` function.")

            return generate_attachment_module.generate_attachment(context, df)

        except Exception as e:
            print(f"Failed to execute generate_plot.py: {e}")
            traceback.print_exc()
            return None
