class ActionParser:
    def __init__(self, actions: dict):
        self.actions = actions

    def parse_actions(self):
        parsed_actions = {}
        for action_name, action_details in self.actions.items():
            parsed_actions[action_name] = self.parse_single_action(action_details)
        return parsed_actions

    def parse_single_action(self, action_details: dict):
        # Implement parsing logic here
        return action_details