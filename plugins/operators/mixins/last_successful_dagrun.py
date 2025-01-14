import pendulum
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
        rebuild = False
        try:
            if self.rebuild:
                rebuild = True
        except AttributeError:
            rebuild = False

        if not rebuild:
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
                self.log.info(
                    f"[LastSuccessfulDagrunMixin] Parsing {self.last_successful_dagrun_xcom_key} xcom value {xcom.value}"  # noqa
                )

                try:
                    # Attempt to parse the XCom value as a date
                    return pendulum.parse(xcom.value)
                except TypeError:
                    # Handle old XCom value formats as a one-off
                    if isinstance(xcom.value, dict) and "timestamp" in xcom.value:
                        timestamp = xcom.value["timestamp"]
                        return pendulum.parse(timestamp)
                    elif isinstance(xcom.value, int):
                        # Convert an integer timestamp to a Pendulum datetime
                        return pendulum.from_timestamp(xcom.value)
                    else:
                        raise TypeError(f"Cannot parse xcom value {xcom.value}")

        # return the earliest start date for the Dag
        return self.dag.start_date
