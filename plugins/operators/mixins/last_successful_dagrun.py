from datetime import datetime

from airflow.models import XCom
from airflow.utils.session import provide_session


class LastSuccessfulDagrunMixin:
    """
    Mixin class to fetch column names for a given table in a specified schema using an SQLAlchemy connection.
    """

    def set_last_successful_dagrun_ts(self, context):
        context["ti"].xcom_push(
            key=self.last_successful_dagrun_xcom_key, value=context["data_interval_end"].to_iso8601_string()
        )

    @provide_session
    def get_last_successful_dagrun_ts(self, run_id, session=None):
        if not self.rebuild:
            query = XCom.get_many(
                include_prior_dates=True,
                dag_ids=self.dag_id,
                run_id=run_id,
                task_ids=self.task_id,
                key=self.last_successful_dagrun_xcom_key,
                session=session,
                limit=1,
            )

            xcom = query.first()
            if xcom:
                return datetime.fromisoformat(xcom.value)

        # return the earliest start date for the Dag
        return self.dag.start_date
