from rasa.engine.recipes.default_recipe import DefaultV1Recipe
from rasa.engine.graph import GraphComponent, ExecutionContext
from rasa.shared.nlu.training_data.message import Message
import re
from typing import Any, Dict, List

@DefaultV1Recipe.register(component_types=["tokenizer"], is_trainable=False)
class EOSPunctuationRemoval(GraphComponent):
    """
    * Remove end of sentence punctuation.
    """
    
    @staticmethod
    def requires() -> List[str]:
        return ["text"]
    
    @staticmethod
    def is_trainable() -> bool:
        # Prevent pruning
        return True
    
    @classmethod
    def create(cls, config: Dict[str, Any], model_storage=None, resource=None, execution_context=None):
        print("✅ UserIntentClassifier loaded!")
        return cls(config)

    def __init__(self, config):
        self.eos_punct_patt = re.compile(r"[.!?]+$")

    def process_training_data(self, training_data, **kwargs):
        return training_data

    def process(self, messages: list[Message], **kwargs):
        for message in messages:
            text = self.eos_punct_patt.sub("", message["text"])
            message.set("tokens", [{"text": text}])