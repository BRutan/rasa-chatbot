from argparse import ArgumentParser, Namespace
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
from typing import Any

def main():
    """
    * Test the legal classification models.
    """
    args = get_args()
    classifier = make_classifier(args)
    test_classifier(classifier, args)

def get_args() -> Namespace:
    """
    * Retrieve model name to be worked with.
    """
    parser = ArgumentParser()
    parser.add_argument("--model_name", required=True, default="samkas125/bert-large-legal-sentence-classification")
    #args = parser.parse_args()
    args = Namespace(**{"model_name": "samkas125/bert-large-legal-sentence-classification"})
    
    # Validate arguments:

    return args

def make_classifier(args:Namespace):
    """
    * Generate a classifier using passed model name.
    """
    # Load tokenizer and model
    tokenizer = AutoTokenizer.from_pretrained(args.model_name)
    model = AutoModelForSequenceClassification.from_pretrained(args.model_name)

    # Create an inference pipeline
    return pipeline("text-classification", model=model, tokenizer=tokenizer)

def test_classifier(classifier:Any, args:Namespace):
    """
    * Accept input from stdin and test the classifier. 
    """
    print("Pass input, stop with 'X':")
    result_in = input()
    while result_in != "X":
        result_out = classifier(result_in)
        print(result_out)
        result_in = input()
    print("Finished testing.")

if __name__ == "__main__":
    main()