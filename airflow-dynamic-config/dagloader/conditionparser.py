import rule_engine as re
from airflow.providers.standard.operators.branch import BaseBranchOperator


class ConditionParser:
    def __init__(self, condition_str: str, condition_name: str = "default_condition"):
        self.condition_str = condition_str
        self.condition_name = condition_name
        self.engine = re.Rule(condition_str)

    def evaluate(self, context: dict) -> bool:
        return self.engine.matches(context)

    def get_conditional_task(self, task, true_task, context: dict):
        @task.branch(task_id=self.condition_name + "_branch_task")
        @staticmethod
        def _airflow_task_condition(task) -> bool:
            """Example method to evaluate conditions based on Airflow task context."""
            # This is a placeholder for actual condition logic based on Airflow context
            return self.evaluate(context)

        if self.evaluate(context):
            return _airflow_task_condition(task)
        else:
            return None


class ConditionOperator(BaseBranchOperator):
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#branching
    def __init__(self, condition_parser: ConditionParser, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.condition_parser = condition_parser

    def choose_branch(self, context):
        if self.condition_parser.evaluate(context):
            return self.condition_parser.condition_name + "_true_task"
        else:
            return self.condition_parser.condition_name + "_false_task"