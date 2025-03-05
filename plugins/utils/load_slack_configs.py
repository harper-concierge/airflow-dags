import os

from airflow.exceptions import AirflowException


def load_slack_configs(slack_dir):
    """
    Load and validate subdirectory configurations for Slack.
    Each subdirectory must contain exactly two files: 'query.sql' and 'slack.json.tpl'.

    :param slack_dir: The absolute path to the directory containing subdirectories.
    :return: A list of dictionaries with subdirectory names and relative file paths in the format 'subdir/query.sql'.
    """

    if not os.path.isdir(slack_dir):
        raise AirflowException(f"The directory {slack_dir} does not exist.")

    configs = []
    for sub_dir in os.listdir(slack_dir):
        sub_dir_path = os.path.join(slack_dir, sub_dir)

        # Skip if not a directory
        if not os.path.isdir(sub_dir_path):
            continue

        # Validate presence of 'query.sql' and 'slack.json.tpl'
        required_files = {"query.sql", "slack.json.tpl"}
        actual_files = set(os.listdir(sub_dir_path))

        if not required_files.issubset(actual_files):
            raise AirflowException(
                f"Subdirectory {sub_dir} must contain 'query.sql' and 'slack.json.tpl'. "
                f"Missing: {required_files - actual_files}"
            )

        # Add the directory name and file paths (subdir/query.sql and subdir/slack.json.tpl) to the configs list
        config = {
            "id": sub_dir,
            "query_file": f"{sub_dir}/query.sql",
            "slack_file": f"{sub_dir}/slack.json.tpl",
        }
        if "generate_attachment.py" in actual_files:
            config["generate_attachment_script"] = f"{slack_dir}/{sub_dir}/generate_attachment.py"
        else:
            config["generate_attachment_script"] = None

        configs.append(config)

    return configs
