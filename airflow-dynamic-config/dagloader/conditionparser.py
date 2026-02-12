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
    def __init__(self, intermediate_storage, action_config: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.action_name = action_config.get('name', None)
        condition_str = action_config.get('condition', '')
        self.condition_parser = ConditionParser(condition_str=condition_str,
                                             condition_name=self.action_name)
        self.intermediate_storage = intermediate_storage
        self.action_config = action_config

    def choose_branch(self, context):
        task_ids = self.action_config.get('depends_on', [])
        data = {}
        for data_key in task_ids:
            data[data_key] = self.intermediate_storage.load(data_key)
        if self.condition_parser.evaluate(data):
            return self.action_name
        else:
            return None