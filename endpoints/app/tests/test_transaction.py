import asyncio
from fastapi.testclient import TestClient
from functions.backend import BACKEND_CONN
from shared import send_file_to_endpoint
import logging
import json
import os
import re

try:
    from ..main import app
except ImportError:
    from main import app

from typing import Any, Dict

LOG = logging.getLogger()
logging.getLogger("httpx").setLevel(logging.WARNING)

_HEADER = {"Authorization": "Token 1"}
ENDPOINTS_URL = "http://localhost:{port}".format(port=os.environ["FASTAPI_PORT"])

sequence = [("I want to restart the conversation.", ["Conversation has been reset."]),
            ("<INITIAL_MESSAGE>", ["Welcome to Rasa Chatbot!"]),
            ("I want to open a transaction.", ["It looks like we don't have you registered. Let's get started with that.", "What is your full name (first and last)?"]),
            ("Ben Rutan", ["Please upload identification."]),
            ("IMG_0101.jpg", ["What is your email?"]),
            ("ben@gmail.com", ["What is your address?"]),
            ("200 Water St", ["What city do you live in?"]),
            ("New york", ["What state do you live in?"]),
            ("NY", ["What is your zip code?"]),
            ("10007", ["What is your phone number?"]),
            ("2037227128", ["What is the bank account number associated with the transaction?"]),
            ("942341234", ["What is the bank account routing number associated with the account?"]),
            ("021000021", ["You are now registered Ben Rutan.", "Who is the buyer?"]),
            ("I am", ["Who is the vendor (their corporation name)?"]),
            ("Regus Management Group LLC", ["What is the transaction amount?"]),
            ("$1,429", ["Please describe the transaction."]),
            ("I intend to lease a month-to-month shared unit.", ["Please provide documentation."]),
            ("Regus Lease Contract.docx", ["What is the vendor's full name (first and last)?"]),
            ("Joseph Perrone", ["What is the vendor's email?"]),
            ("Joseph.Perrone@iwgplc.com", ["What is the vendor's address?"]),
            ("140 Broadway", ["What city does the vendor live in?"]),
            ("New york", ["What state does the vendor live in?"]),
            ("ny", ["What is the vendor's zip code?"]),
            ("10005", ["What is the vendor's phone number?"]),
            ("4152322834", ["What is the vendor's account number for receiving transaction funds?"]),
            ("021000022", ["What is the vendor's routing number?"]),
            ("021000021", ["We have registered Joseph Perrone as a vendor (corp name Regus Management Group LLC).",
                           "Recorded transaction between buyer Ben Rutan\n        and vendor Joseph Perrone for amount $1,429.00\n        with transaction id 1.",
                           "Let me know if you would like to anything else."]),
            #Dispute:
            ("I want to start a dispute.", ["Which vendor is involved in the dispute?"]),
            ("Regus Management Group LLC", ["Please describe the dispute."]),
            ("They gave me a unit I did not ask for nor toured. The contract specifies two different prices for two different rooms.", ["What amount is in dispute?"]),
            ("$1,429", ["Created dispute with vendor Regus Management Group LLC with id 1.", "Please upload an evidence file or indicate you are finished."]),
            ("regus - chargeback email.png", ["Thank you.", "Please upload an evidence file or indicate you are finished."]),
            ("regus - office example.png", ["Thank you.", "Please upload an evidence file or indicate you are finished."]),
            ("That is all.", ["The evidence you have passed has been loaded."]),
            ("I want to check my dispute status", ["Based on the evidence uploaded by both parties, I am returning\n$1,429.00 (100% of the transaction amount $1,429.00) to the buyer."])]

files_folder = os.path.split(__file__)[0] + "/files"
files = {"Regus Lease Contract.docx": os.path.join(files_folder, "Regus Lease Contract.docx"),
         "IMG_0101.jpg": os.path.join(files_folder, "IMG_0101.jpg"),
         "IMG_0095.jpg": os.path.join(files_folder, "IMG_0095.jpg")}

async def test_transaction_dispute_upload(client: TestClient, token: str, messages: list[str]):
    """
    * Connect to the chatbot interface websocket. Perform the following:
    - Register new user.
    - Register new vendor.
    - Initiate transaction.
    - Load transaction documentation.
    - Initiate dispute.
    - Initiate multiple dispute evidence documents.
    """
    transaction_id = None
    with client.websocket_connect("/chatbot/interface", headers=_HEADER) as websocket:
        file_path = None
        for msg, expected_response in messages:
            if msg == "regus - chargeback email.png":
                i = 0
            file_path = files.get(msg, file_path)
            response = send_message(websocket, msg, token)
            if not response.get("message"):
                raise RuntimeError("Should be at least one message.")
            elif "<NO RESPONSE>" in response["message"]:
                raise RuntimeError("Should not be <NO RESPONSE>.")
            elif "I do not understand the input." in response["message"]:
                raise RuntimeError(f"Should not be returning {response['message']}.")
            elif response["message"] != expected_response:
                raise RuntimeError(f"Unexpected response for '{msg}'.")
            transaction_id = response.get("transaction_id")
            dispute_id = response.get("dispute_id")
            if (file_path is not None 
                and response["current_slot"] == "user_identification_filename"):
                file_response = await send_file_to_endpoint(file_path=file_path, 
                                                            url=ENDPOINTS_URL + "/users/identification/upload",
                                                            method_name="post",
                                                            header=_HEADER,
                                                            data={"id": 1})
            elif (file_path is not None and transaction_id and response["current_slot"] == "documentation"):
                file_response = await send_file_to_endpoint(file_path=file_path, 
                                                            url=ENDPOINTS_URL + "/transactions/documentation/upload",
                                                            method_name="post",
                                                            header=_HEADER,
                                                            data={"id": int(transaction_id)})
            elif (dispute_id is not None 
                  and file_path is not None
                  and response["active_loop"] == "dispute_form"
                  and response["current_slot"] == "evidence_file_name"):
                data = {"dispute_id": transaction_id}
                file_response = await send_file_to_endpoint(file_path, 
                                                            url=ENDPOINTS_URL + "/cases/evidence/upload",
                                                            method_name="post",
                                                            header=_HEADER,
                                                            data=data)
            print_chatbot_response(msg, response)

def send_message(websocket, msg, token):
    """
    * Send message from user to chatbot.
    Return the full chatbot payload.
    """
    payload = {"message": msg, "token": token}
    websocket.send_text(json.dumps(payload))
            
    response_text = websocket.receive_text()
    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        raise RuntimeError(f"Failed to decode response: {response_text}.")
    # Make sure the message is valid:
    if any(re.search(r"\s+valid\b", m, flags=re.IGNORECASE) for m in response_json["message"]):
        raise RuntimeError(f"Invalid message sent: {msg} ({response_json['message']})")
    if not response_json:
        raise RuntimeError("no response.")
    return response_json

def print_chatbot_response(msg:str, response:Dict[str, Any]):
    """
    * Print the chatbot response.
    """
    elems = ["=======" * 2, f"sent: '{msg}'"]
    elems += [f"response: '{m}'" for m in response["message"]]
    elems += [f"{k}: {str(v)}" for k,v in response.items() if k not in "message"]
    elems += ["=======" * 2]
    print("\n".join(elems))

with TestClient(app, base_url=ENDPOINTS_URL) as client:
    # Reset the backend:
    BACKEND_CONN.execute("call demo.reset_demo_tables()")
    asyncio.run(test_transaction_dispute_upload(client, "1", sequence))