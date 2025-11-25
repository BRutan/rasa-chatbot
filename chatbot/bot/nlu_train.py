import os
from rasa.engine.recipes.default_recipe import DefaultV1Recipe
from rasa.model_training import train_nlu
from rasa.shared.nlu.constants import INTENT, TEXT
from rasa.shared.nlu.training_data.training_data import TrainingData
from rasa.shared.nlu.training_data.message import Message
import re
import shutil
import tempfile
import yaml

def main():
    data = get_data()
    write_nlu_yaml(data)
    #train_model(data)

def get_data():
    """
    * Load preselected data from both the nlu.yml and the
    chosen datasets mapped to intents.
    """
    nlu_path = os.path.split(__file__)[0] + "/data/nlu.yml"
    with open(nlu_path, "rb") as f:
        nlu_yml = yaml.safe_load(f)
    # Load the current intents and examples:
    examples = []
    for r in nlu_yml["nlu"]:
        intent = r["intent"]
        example = re.sub(r"^-\s+|\s+$", "", r["examples"])
        examples.append(Message({TEXT: example, INTENT:intent}))
    # Load the spam/not spam data, map to the irrelevant intent:
    with open("/opt/chatbot/data/SMSSpamCollection", "r") as f:        
        line = f.readline()
        while line:
            cat = re.search("^(spam|ham)", line)[0]
            # Skip if is ham:
            if cat != "ham":
                text = re.sub(r"^(spam|ham)\s+", "", line)
                cleaned = text.encode("ascii", errors="ignore").decode()
                examples.append(Message({TEXT:cleaned, INTENT:"irrelevant"}))
            line = f.readline()
    return TrainingData(training_examples=examples)

def write_nlu_yaml(data:TrainingData):
    curr_dir = os.path.split(__file__)[0]
    nlu_yaml_path = os.path.join(curr_dir, "data/nlu_.yml")
    with open(nlu_yaml_path, "w") as f:
        f.write(data.nlu_as_yaml())

def train_model(data:TrainingData):
    """
    * Train the model and output to the models directory.
    """
    curr_dir = os.path.split(__file__)[0]
    config_path = os.path.join(curr_dir, "config.yml")
    model_dir = os.path.join(curr_dir, "models")
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".yml", encoding="utf-8") as tmpfile:
        tmpfile.write(data.nlu_as_yaml())
        with tempfile.TemporaryDirectory() as tmp:
            generated_path = train_nlu(
                config=config_path, 
                nlu_data=tmpfile.name,
                output=model_dir
            )
        print(f"generated_path: {generated_path}")
        final_model_path = os.path.join(model_dir, "nlu_model.tar.gz")
        shutil.move(generated_path, final_model_path)
        print(f"Model saved at: {final_model_path}")


if __name__ == "__main__":
    main()
