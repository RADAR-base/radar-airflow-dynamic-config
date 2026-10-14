"""Exceptions custom tasks can raise whether or not Airflow is installed.

Inside Airflow this is Airflow's own ``AirflowSkipException``. Inside a
pipeline's separate env, where Airflow is absent, it is a stand-in class with
the same name. The env runner reports it back, and the task operator raises the
real Airflow exception, so the task is marked skipped in both cases::

    from dagloader.taskprocessor.exceptions import AirflowSkipException
"""
try:
    from airflow.exceptions import AirflowSkipException
except ImportError:
    class AirflowSkipException(Exception):
        """Stand-in for Airflow's skip exception in a separate env."""

__all__ = ['AirflowSkipException']
