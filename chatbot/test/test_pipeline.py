from argparse import ArgumentParser, Namespace
import os
from transformers import pipeline
import yaml

def main():
    """
    * Test a model of choice.
    """
    args = get_args()
    clf = make_pipeline(args)
    test_pipeline(clf, args)

def get_args() -> Namespace:
    """
    * Extract command line arguments.
    """
    parser = ArgumentParser()
    parser.add_argument("--model", required=True, help="Model name.")

    args = parser.parse_args()

    #args = Namespace(**{"model": "mrm8488/bert-tiny-finetuned-sms-spam-detection"})
    # Validate:
    errs = []
    parent = os.path.split(__file__)[0]
    full_path = os.path.join(parent, "test_pipeline.yml")
    with open(full_path, "r") as f:
        config = yaml.safe_load(f)
    if args.model not in config:
        errs.append(f"Model {args.model} is invalid.")
    if errs:
        raise ValueError("\n".join(errs))
    for kwarg, value in config[args.model].items():
        setattr(args, kwarg, value)
    return args

def make_pipeline(args:Namespace):
    """
    * Generate pipeline for testing.
    """
    return pipeline(args.mode, model=args.model, candidate_labels=args.candidate_labels)

def test_pipeline(clf, args:Namespace):
    """
    * Test the pipeline until stopped.
    """
    print(f"Starting pipeline for {args.model}.")
    input_str = input("Enter text (stop with X): ").strip()
    while input_str != "X":
        prompt = f"Summarize the topic of this sentence in one word: {input_str}"
        model_result = clf(prompt)
        print("result: ")
        print(model_result)
        input_str = input("Enter text (stop with X): ").strip()

    print(f"Finished testing {args.model}.")

if __name__ == "__main__":
    main()