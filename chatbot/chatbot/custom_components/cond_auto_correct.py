from typing import Any, Dict, Optional, List
from rasa.engine.graph import GraphComponent, ExecutionContext
from rasa.engine.recipes.default_recipe import DefaultV1Recipe
from rasa.shared.nlu.training_data.message import Message
from rasa.shared.nlu.training_data.training_data import TrainingData
from autocorrect import Speller

@DefaultV1Recipe.register(
    DefaultV1Recipe.ComponentType.MESSAGE_TOKENIZER,
    is_trainable=False,
)
class ConditionalAutoCorrectComponent(GraphComponent):
    """Custom Rasa component to autocorrect user messages."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)
        self.spell = Speller(lang='en')

    @classmethod
    def create(cls,
               config:Dict[str, Any],
               execution_context:ExecutionContext) -> GraphComponent:
        return cls(config)

    @classmethod
    def create(
            cls,
            config: Dict[str, Any],
            execution_context: ExecutionContext,
        ) -> GraphComponent:
            return cls(config)

    def process(self, messages: List[Message]) -> List[Message]:
        for message in messages:
            if self.should_autocorrect(message):     
                original_text = message.get("text")
                corrected_text = self.spell(original_text)
                message.set("text", corrected_text)
        return messages

    def should_autocorrect(self, message) -> bool:
        """
        * 
        """
        tracker = message.get("tracker")
        if not tracker:
            return False
        for slot_name in self.target_slots:
            if tracker.get_slots(slot_name):
                return True
        return False
    
    @property
    def target_slots(self):
        return []
    
    def train(
        self,
        training_data: TrainingData,
        ) -> None:
        """No training needed for autocorrect."""
        pass