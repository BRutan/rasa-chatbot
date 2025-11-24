import asyncio
from chatbot.functions.shared import get_validation_patts
import exrex
import os
import pytest
from rasa.core.agent import Agent
from rasa.shared.core.events import SessionStarted
from rasa.shared.core.trackers import DialogueStateTracker
import sys

# Ensure the parent directory of 'chatbot/' is in the Python path
project_root = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    
@pytest.mark.asyncio
async def test_register_transaction_flow():
    """
    * Test new user registration and transaction 
    creation.
    """
    # Load your trained model (make sure `models/` exists)
    models_dir = os.path.realpath(os.path.join(project_root, "models"))
    
    # Load the validation slots:
    validation_patts = get_validation_patts()
    agent = Agent.load(models_dir)

    # Start a new session tracker:
    tracker = agent.create_processor().get_tracker("1")
    tracker.update(SessionStarted())

    # Simulate conversation turns:
    inputs = {}
    responses = []
    responses += await agent.handle_text("I want to start a transaction.", sender_id="1")
    for slot, patt in validation_patts["register_user_form"].items():
        val = exrex.getone(patt)
        responses += await agent.handle_text(val, sender_id="1")
        if slot == "user_name":
            inputs["buyer"] = val
    for slot, patt in validation_patts["new_transaction_form"].items():
        if slot in inputs:
            val = inputs[slot]
        else:
            val = exrex.getone(patt)
        responses += await agent.handle_text(val, sender_id="1")
    # Check that any exceptions occurreD:

if __name__ == "__main__":
    asyncio.run(test_register_transaction_flow())
