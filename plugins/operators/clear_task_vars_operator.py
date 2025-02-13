import re

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook

from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin


class ClearTaskVarsOperator(DagRunTaskCommsMixin, BaseOperator):
    """
    Operator for clearing task variables from the task communications table.
    """

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_datalake_conn_id",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_postgres_sqlalchemy_engine(self, hook, engine_kwargs=None):
        """
        Get an sqlalchemy_engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: the created engine.
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        conn_uri = re.sub(r"\?.*$", "", conn_uri)
        return create_engine(conn_uri, **engine_kwargs)

    def execute(self, context):
        """
        Clear all task variables for the current task instance.
        """
        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            # Ensure task communications table exists
            self.ensure_task_comms_table_exists(conn)

            # Clear all task variables using the mixin method
            self.clear_task_vars(conn, context)

            self.log.info(f"Successfully cleared task variables for task {self.task_id}")
