from functions.users import get_user_info_from_token
import logging
from rasa.engine.graph import GraphComponent, ExecutionContext
from rasa.engine.recipes.default_recipe import DefaultV1Recipe
from rasa.shared.nlu.training_data.training_data import TrainingData
from rasa.shared.nlu.training_data.message import Message
from rasa.engine.storage.resource import Resource
from rasa.engine.storage.storage import ModelStorage
from rasa.engine.storage.resource import Resource 
from transformers import pipeline
from typing import Any, Dict, List, Tuple

#tmp:
import ipdb

logger = logging.getLogger(__name__)

@DefaultV1Recipe.register(
    component_types=["intent_classifier"],
    is_trainable=True
)
class UserIntentClassifier(GraphComponent):
    """
    * Route intents based on initial user input.
    """
    required_inputs = ["text"]

    @classmethod
    def create(cls, config: Dict[str, Any], model_storage=None, resource=None, execution_context=None):
        print("✅ UserIntentClassifier loaded!")
        return cls(config)
    
    @staticmethod
    def requires() -> List[str]:
        return ["text"]
    
    @staticmethod
    def provides() -> List[str]:
        return ["intent", "intent_ranking"]
    
    @staticmethod
    def is_trainable() -> bool:
        # Prevent pruning
        return True

    def __init__(self, config: Dict[str, Any]):
        self.relevance_model_name = config.get(
            "relevance_model", "mrm8488/bert-tiny-finetuned-sms-spam-detection"
        )
        self.topic_model_name = config.get("topic_model", "facebook/bart-large-mnli")
        self.context_model_name = ""
        self.candidate_topics = ["transaction"]
        logger.debug("UserIntentClassifier loaded with model %s", self.relevance_model_name)
        self.irrelevant_classifier = pipeline("text-classification", model=self.relevance_model_name)
        self.topic_classifier = pipeline("zero-shot-classification", model=self.topic_model_name)
        self.relevance_label_map = {"LABEL_0": "relevant", "LABEL_1": "irrelevant"}
        self.context_labels = ["buyer", "vendor"]

    def process_training_data(self, training_data: TrainingData, **kwargs: Any) -> TrainingData:
        """No-op for training data."""
        return training_data

    def train(self, training_data: TrainingData, **kwargs: Any) -> Resource:
        """
        Required train() method.
        Return a PrecomputedValue so Rasa can fingerprint/cache this node.
        """
        logger.info("UserIntentClassifier train() called (no-op).")
        return Resource("user_intent_model")

    def process(self, messages: List[Message], **kwargs: Any) -> List[Message]:
        """
        * Route the conversation based on initial user prompt.
        """
        logger.info("in process()")
        for message in messages:
            text = message.get("text")
            if not text:
                continue
            # Check if is irrelevant:
            result = self.irrelevant_classifier(text)[0]
            label = result["label"].upper().strip()
            intent_name = self.relevance_label_map.get(label, "unknown")
            if intent_name == "unknown":
                raise RuntimeError(f"Could not determine intent_name for {text}.")
            confidence = result["score"]
            # If irrelevant then perform the notify_irrelevant:
            if intent_name == "irrelevant":
                logger.info("Routing conversation as irrelevant.")
                logger.info("Initiating irrelevant loop until relevant input provided.")
                logger.info(f"Predicted {intent_name} ({confidence:.3f}) for: {text}")

                message.set("intent", {"name": intent_name, "confidence": confidence})
                message.set(
                    "intent_ranking",
                    [
                        {"name": "notify_irrelevant", "confidence": confidence},
                        {"name": "relevant", "confidence": 1 - confidence},
                    ],
                )
            elif intent_name == "relevant":
                logger.info("Routing conversation as relevant. Routing next step based on message content.")
                intent, confidence = self.route_intent(text)
                logger.info("intent: %s", intent)
                logger.info("Routing conversation as %s (%s) for: %s", intent, confidence, text)
                message.set("intent", {"name": intent, "confidence": confidence})
        return messages
    
    def route_intent(self, text:str) -> Tuple[str, float]:
        """
        * Determine what the topic of the text is
        and route to the intent.
        """
        result = self.topic_classifier(text, candidate_labels=self.candidate_topics)
        max_score = max(result["scores"])
        max_idx = result["labels"].index(max_score)
        intent = result["labels"][max_idx]
        # If is not a transaction then mark as unclear:
        if intent != "transaction":
            intent = "unclear"
        return intent, max_score
    
    def extract_user_context(self, text:str) -> Tuple[str, float]:
        """
        * Determine if user is buyer or vendor, and switch
        context to appropriate based on output.
        By default all users are assumed to be buyers.
        """
        result = self.topic_classifier(text, candidate_labels=self.context_labels)
        max_score = max(result["scores"])
        max_idx = result["labels"].index(max_score)
        intent = result["labels"][max_idx]
        return intent, max_score
    
    def switch_context(self):
        """
        * Switch context from buyer to vendor in current session.
        """
        pass

        