import rule_engine as re
from airflow.providers.standard.operators.branch import BaseBranchOperator
import copy
import logging

logger = logging.getLogger(__name__)


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
    """Branch operator gating a conditional action.

    Same pattern as the other operators:
      - `_initial_action_config` holds the pristine YAML action entry.
      - `action_config` is the active version mutated by `pre_execute`.
      - `_resolve_config()` (re)builds the `ConditionParser` from the
        active `action_config['condition']`.
    ParamMaker emits the action's config under key `<name>` and the
    condition string under key `<name>_condition`; the branch operator's
    own `task_id` is `<name>_branch`, so both keys are looked up
    explicitly off the action name rather than via `self.task_id`.
    """

    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#branching
    def __init__(self, intermediate_storage, action_config: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intermediate_storage = intermediate_storage
        self._initial_action_config = copy.deepcopy(action_config) or {}
        self.action_config = copy.deepcopy(self._initial_action_config)
        self.action_name = self.action_config.get('name')
        self._resolve_config()

    def _resolve_config(self):
        self.condition_parser = ConditionParser(
            condition_str=self.action_config.get('condition', '') or '',
            condition_name=self.action_name,
        )

    def pre_execute(self, context):
        super().pre_execute(context)
        params = (context or {}).get('params') or {}
        config_override = params.get(self.action_name)
        condition_override = params.get(f"{self.action_name}_condition")
        new_action_config = copy.deepcopy(self._initial_action_config)
        if isinstance(config_override, dict) and config_override:
            base_config = new_action_config.get('config') or {}
            new_action_config['config'] = {**base_config, **config_override}
        if isinstance(condition_override, str) and condition_override:
            new_action_config['condition'] = condition_override
        if new_action_config == self.action_config:
            return
        logger.info(
            f"Condition '{self.task_id}' applying runtime override "
            f"(config={bool(config_override)}, condition={bool(condition_override)})"
        )
        self.action_config = new_action_config
        self._resolve_config()

    def choose_branch(self, context):
        task_ids = self.action_config.get('depends_on', [])
        data = {}
        for data_key in task_ids:
            data[data_key] = self.intermediate_storage.load(data_key)
            logger.info(f"Loaded data for key '{data_key}': {data[data_key]}")
        filter_list_key = self.action_config.get('filter_list_key')
        if filter_list_key and filter_list_key in data:
            elements = data[filter_list_key]
            if not isinstance(elements, list):
                elements = [elements]
            matched_elements = []
            for el in elements:
                # Merge the context so other dependencies are accessible.
                # The element replaces the list temporarily for evaluation.
                eval_context = data.copy()
                eval_context[filter_list_key] = el
                if self.condition_parser.evaluate(eval_context):
                    matched_elements.append(el)
            if matched_elements:
                storage_key = self.action_name + "_condition"
                self.intermediate_storage.save(storage_key, matched_elements)
                logger.info(f"Filtered {len(matched_elements)} elements and saved to '{storage_key}'")
                return self.action_name
            else:
                return None
        else:
            if self.condition_parser.evaluate(data):
                return self.action_name
            else:
                return None