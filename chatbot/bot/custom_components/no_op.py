from rasa.engine.recipes.default_recipe import DefaultV1Recipe
from rasa.engine.graph import GraphComponent, ExecutionContext
from rasa.shared.nlu.training_data.message import Message

@DefaultV1Recipe.register(component_types=["tokenizer"], is_trainable=False)
class NoOpTokenizer(GraphComponent):
    @classmethod
    def create(cls, config, model_storage, resource, execution_context: ExecutionContext):
        return cls(config)

    def __init__(self, config):
        pass

    def process_training_data(self, training_data, **kwargs):
        return training_data

    def process(self, messages: list[Message], **kwargs):
        for message in messages:
            # Keep text as single "token"
            message.set("tokens", [{"text": message.get("text")}])