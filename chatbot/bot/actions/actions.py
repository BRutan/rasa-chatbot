# actions.py
from datetime import datetime
from functions.shared import logger, log_execution, async_log_execution
import functions.cases as c_funcs
import functions.shared as s_funcs
import functions.transactions as t_funcs
import functions.users as u_funcs
import json
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.events import ActionExecuted, ActiveLoop, EventType, FollowupAction, Restarted, SlotSet, SessionStarted
from rasa_sdk.forms import FormValidationAction
import re
import string
from transformers import AutoTokenizer, AutoModelForCausalLM
from typing import Optional
#import torch

class ActionSessionStart(Action):
    """
    * Greet user and preload with any information
    that has been stored.
    """
    def name(self) -> str:
        return "action_session_start"

    @async_log_execution
    async def run(
        self, 
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: dict
    ) -> list[EventType]:
        # This ensures session initialization:
        events = [SessionStarted(), ActionExecuted("action_listen")]
        
        # Add utterance event:
        events.append(ActionExecuted("utter_greet"))

        # Finally, listen:
        events.append(ActionExecuted("action_listen"))

        return events

class ActionRestartConversation(Action):
    def name(self):
        return "action_restart_conversation"

    async def run(self, dispatcher, tracker, domain):
        dispatcher.utter_message(text="Conversation has been reset.")
        return [Restarted()]

class ActionCheckTransactionsOpenDispute(Action):
    """
    * Determine if the current user has any
    transactions outstanding, and output intent
    that will start the dispute form.
    """
    def name(self): 
        return "action_check_transactions_and_open_dispute"

    @async_log_execution
    async def run(self, dispatcher, tracker, domain):
        """
        * Determine if the user is registered or not.
        """
        user_token = u_funcs.get_user_token_from_tracker(tracker)
        user_info = await u_funcs.get_user_info_from_token(user_token)
        logger.info("user_token: %s", user_token)
        logger.info("user_info: ")
        logger.info(user_info)
        buyer_is_registered = user_info is not None
        if not buyer_is_registered:
            logger.info("User is not registered. Moving to register_user_form.")
            dispatcher.utter_message(response="utter_user_not_registered")
            return [ActiveLoop("register_user_form"),
                    FollowupAction("dispute_form")]
        has_transactions = await t_funcs.user_has_transactions(user_token)
        if not has_transactions:
            logger.info("User token %s does not have any transactions.", user_token)
            dispatcher.utter_message(response="utter_user_has_no_transactions")
            dispatcher.utter_message(response="utter_cancelling_dispute")
            return [SlotSet("has_transactions", has_transactions)]
        return [SlotSet("has_transactions", has_transactions), ActiveLoop("dispute_form")]

class ActionCheckRegistration(Action):
    """
    Check if the transaction buyer and vendor are registered,
    then determine which form to run.
    """
    def name(self):
        return "action_check_buyer_registration"

    @async_log_execution
    async def run(self, dispatcher, tracker, domain):
        logger.info(f"tracker.slots: {tracker.current_slot_values()}")
        user_token = u_funcs.get_user_token_from_tracker(tracker)
        user_info = await u_funcs.get_user_info_from_token(user_token)
        buyer_is_registered = user_info is not None
        logger.info("User registered? %s", buyer_is_registered)
        if not buyer_is_registered:
            dispatcher.utter_message(response="utter_user_not_registered")
            logger.info("User not registered. Triggering user registration form.")
            return [ActiveLoop("register_user_form")]
        return [ActiveLoop("new_transaction_form")]
    
class ActionCheckVendorRegistration(Action):
    """
    Check if the transaction buyer and vendor are registered,
    then determine which form to run.
    """
    def name(self):
        return "action_check_vendor_registration"
    
    @async_log_execution
    async def run(self, dispatcher, tracker, domain):
        """
        * Check that vendor is registered.
        """
        logger.info(f"tracker.slots: {tracker.current_slot_values()}")
        vendor_token = u_funcs.get_user_token_from_tracker(tracker, is_vendor=True)
        vendor_info = await u_funcs.get_user_info_from_token(vendor_token)
        vendor_is_registered = vendor_info is not None
        logger.info("Vendor registered? %s", vendor_is_registered)
        if not vendor_is_registered:
            dispatcher.utter_message(response="utter_vendor_not_registered")
            logger.info("Vendor not registered. Triggering vendor registration form.")
            return [ActiveLoop("register_vendor_form"), FollowupAction("new_transaction_form")]
        # 3️⃣ Both registered → proceed to transaction form
        dispatcher.utter_message(response="utter_ready_for_transaction")
        logger.info("Vendor is registered. Continuing transaction form.")
        return [ActiveLoop("new_transaction_form")]

class ActionLoadUserInfo(Action):
    def name(self):
        return "action_load_user_info"
    
    @async_log_execution
    async def run(self, dispatcher, tracker, domain):
        """
        * Push new user information to backend.
        """
        logger.info(f"tracker.slots: {tracker.current_slot_values()}")
        name = tracker.get_slot("user_name")
        first_name, last_name = name.split(" ")
        address = tracker.get_slot("user_address")
        email = tracker.get_slot("user_email")
        phone_number = tracker.get_slot("user_phone_number")
        city = tracker.get_slot("user_city")
        state = tracker.get_slot("user_state")
        zip_code = tracker.get_slot("user_zip_code")
        account_number = tracker.get_slot("user_account_number")
        routing_number = tracker.get_slot("user_routing_number")
        data = {"first_name": first_name, 
                "last_name": last_name,
                "email": email,
                "phone_number": phone_number,
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "account_number": account_number,
                "routing_number": routing_number}
        # Normalize data:
        data = {c: s_funcs.normalize_text(v) for c, v in data.items()}
        logger.info("data: ")
        logger.info(json.dumps(data, indent=2))
        user_token = u_funcs.get_user_token_from_tracker(tracker)
        logger.info("user_token: %s", user_token)
        if not await u_funcs.user_exists(user_token):
            logger.info("User does not exist. Loading into backend.")
            await u_funcs.load_user_info(user_token, data)
            first_name, last_name = data["first_name"], data["last_name"]
            user_name = f"{first_name.title()} {last_name.title()}"
            msg = f"You are now registered {user_name}."
            dispatcher.utter_message(text=msg)
        else:
            logger.info("It looks like we already have you registered.")
        # Clear active loop slots:
        events = s_funcs.clear_active_loop_slots(tracker, domain)
        if tracker.active_loop and tracker.active_loop.get("requested_slot") == "buyer":
            msg = f"Setting buyer to be {first_name} {last_name} in in-progress new_transaction_form following completion."
            logger.info(msg)
            events.extend([
                SlotSet("buyer", f"{first_name} {last_name}"),
                ActiveLoop(None),
                FollowupAction("new_transaction_form")
            ])
        else:
            events.extend([ActiveLoop(None), FollowupAction("new_transaction_form")])
        return events
    
class ActionLoadVendorInfoForm(Action):
    def name(self):
        return "action_load_vendor_info"
    
    @async_log_execution
    async def run(self, dispatcher, tracker, domain):
        """
        * Push new user information to backend.
        """
        logger.info(f"tracker.slots: {tracker.current_slot_values()}")
        name = tracker.get_slot("vendor_name")
        first_name, last_name = name.split(" ")
        email = tracker.get_slot("vendor_email")
        address = tracker.get_slot("vendor_address")
        phone_number = tracker.get_slot("vendor_phone_number")
        city = tracker.get_slot("vendor_city")
        state = tracker.get_slot("vendor_state")
        zip_code = tracker.get_slot("vendor_zip_code")
        account_number = tracker.get_slot("vendor_account_number")
        routing_number = tracker.get_slot("vendor_routing_number")
        corp_name = tracker.get_slot("corp_name")
        data = {"first_name": first_name, 
                "last_name": last_name,
                "email": email,
                "phone_number": phone_number,
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "account_number": account_number,
                "routing_number": routing_number,
                "corp_name": corp_name}
        # Normalize data:
        data = {c: v.lower() for c, v in data.items()}
        logger.info("vendor data to send: ")
        logger.info(json.dumps(data, indent=2))
        vendor_token = u_funcs.get_user_token_from_tracker(tracker, is_vendor=True)
        logger.info("user_token: %s", vendor_token)
        # Register the vendor:
        if not await u_funcs.vendor_exists(vendor_token):
            logger.info("Registering vendor with vendor_token %s.", vendor_token)
            await u_funcs.load_vendor_info(vendor_token, data)
            first_name, last_name = data["first_name"], data["last_name"]
            user_name = s_funcs.present_name(first_name + " " + last_name)
            corp_name = s_funcs.present_name(corp_name)
            msg = f"We have registered {user_name} as a vendor (corp name {corp_name})."
            dispatcher.utter_message(text=msg)
        else:
            logger.info("It looks like vendor is already registered.")
        logger.info("tracker.active_loop: %s", tracker.active_loop)
        #if tracker.active_loop and tracker.active_loop.get("requested_slot") == "vendor":
        logger.info("Resuming new_transaction_form.")
        return [
            SlotSet("vendor", f"{first_name} {last_name}"),
            SlotSet("needs_vendor_registration", False),
            ActiveLoop("new_transaction_form")
        ]
        
class ValidateRegisterUserForm(FormValidationAction):
    def name(self) -> str:
        return "validate_register_user_form"

    @log_execution
    def validate_user_name(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"\w+\s+\w+", normalized):
            dispatcher.utter_message(text="Please provide a valid name (first and last).")
            return {"user_name": None}
        return {"user_name": normalized}
    
    @log_execution
    def validate_user_identification_filename(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value, keep_punct=True)
        if not s_funcs.is_file_name(normalized):
            dispatcher.utter_message(text="Please upload a valid file.")
            return { "user_identification_filename": None}
        return { "user_identification_filename": normalized }
    
    @log_execution
    def validate_user_email(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value, keep_punct=True, keep_toks=["@", "."])
        if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", normalized):
            dispatcher.utter_message(text="Please provide a valid email address.")
            return {"user_email": None}
        return {"user_email": normalized}
    
    @log_execution
    def validate_user_phone_number(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\+?(\d{1,3})?[-.\s]?(\(?\d{1,4}\)?)[-.\s]?(\d{1,4})[-.\s]?(\d{1,4})[-.\s]?(\d{1,9})$", normalized):
            dispatcher.utter_message(text="Please provide a valid phone number.")
            return {"user_phone_number": None}
        return {"user_phone_number": normalized}

    @log_execution
    def validate_user_address(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"\d+\s+\w+(\s+\w+)?", normalized):
            dispatcher.utter_message(text="Please provide a valid address.")
            return {"user_address": None}
        return {"user_address": normalized}
    
    @log_execution
    def validate_user_city(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if re.search(r"[^\w\s]", normalized):
            dispatcher.utter_message(text="Please provide a valid city.")
            return {"user_city": None}
        return {"user_city": normalized}
    
    @log_execution
    def validate_user_state(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if re.search(r"[^\w\s]", normalized):
            dispatcher.utter_message(text="Please provide a valid state.")
            return {"user_state": None}
        return {"user_state": normalized}
    
    @log_execution
    def validate_user_zip_code(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\d{5}(-\d{4})?$", normalized):
            dispatcher.utter_message(text="Please provide a valid zip code.")
            return {"user_zip_code": None}
        return {"user_zip_code": normalized}
    
    @log_execution
    def validate_user_account_number(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\d{8,20}$", normalized):
            dispatcher.utter_message(text="Please provide a valid bank account number.")
            return {"user_account_number": None}
        return {"user_account_number": normalized}
    
    @log_execution
    def validate_user_routing_number(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\d{9}$", normalized):
            dispatcher.utter_message(text="Please provide a valid bank routing number.")
            return {"user_routing_number": None}
        return {"user_routing_number": normalized}
    
class ValidateRegisterVendorForm(FormValidationAction):
    def name(self) -> str:
        return "validate_register_vendor_form"
    
    @log_execution
    def validate_vendor_name(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"\w+\s+\w+", normalized):
            dispatcher.utter_message(text="Please provide a valid name (first and last).")
            return {"vendor_name": None}
        return {"vendor_name": normalized}
    
    @log_execution
    def validate_vendor_email(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value, keep_punct=True, keep_toks=["@", "."])
        if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", normalized):
            dispatcher.utter_message(text="Please provide a valid email address.")
            return {"vendor_email": None}
        return {"vendor_email": normalized}
    
    @log_execution
    def validate_vendor_phone_number(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\+?(\d{1,3})?[-.\s]?(\(?\d{1,4}\)?)[-.\s]?(\d{1,4})[-.\s]?(\d{1,4})[-.\s]?(\d{1,9})$", normalized):
            dispatcher.utter_message(text="Please provide a valid phone number.")
            return {"vendor_phone_number": None}
        return {"vendor_phone_number": normalized}

    @log_execution
    def validate_vendor_address(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"\d+\s+\w+(\s+\w+)?", normalized):
            dispatcher.utter_message(text="Please provide a valid address.")
            return {"vendor_address": None}
        return {"vendor_address": normalized}
    
    @log_execution
    def validate_vendor_city(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if re.search(r"[^\w\s]", normalized):
            dispatcher.utter_message(text="Please provide a valid city.")
            return {"vendor_city": None}
        return {"vendor_city": normalized}
    
    @log_execution
    def validate_vendor_state(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if re.search(r"[^\w\s]", normalized):
            dispatcher.utter_message(text="Please provide a valid state.")
            return {"vendor_state": None}
        return {"vendor_state": normalized}
    
    @log_execution
    def validate_vendor_zip_code(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\d{5}(-\d{4})?$", normalized):
            dispatcher.utter_message(text="Please provide a valid zip code.")
            return {"vendor_zip_code": None}
        return {"vendor_zip_code": normalized}
    
    @log_execution
    def validate_vendor_account_number(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\d{8,20}$", normalized):
            dispatcher.utter_message(text="Please provide a valid bank account number.")
            return {"vendor_account_number": None}
        return {"vendor_account_number": normalized}
    
    @log_execution
    def validate_vendor_routing_number(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_text(value)
        if not re.match(r"^\d{9}$", normalized):
            dispatcher.utter_message(text="Please provide a valid bank routing number.")
            return {"vendor_routing_number": None}
        return {"vendor_routing_number": normalized}
    
    def validate_corp_name(self, value, dispatcher, tracker, domain):
        """
        * Validate the corporation name.
        """
        normalized = s_funcs.normalize_text(value)
        corp_mtch = re.match(r"^\s*(?P<corp_name>[\w\s]+(inc|llc|corp))\s*$", normalized, flags=re.IGNORECASE)
        if not corp_mtch:
            dispatcher.utter_message(text="Please pass a valid corporation name.")
            return { "corp_name": None }
        return { "corp_name": corp_mtch["corp_name"] }
    
class ValidateTransactionForm(FormValidationAction):
    def name(self) -> str:
        return "validate_new_transaction_form"

    @log_execution
    def validate_transaction_amount(self, value, dispatcher, tracker, domain):
        normalized = s_funcs.normalize_numeric_text(value)
        normalized = s_funcs.try_convert(normalized, float)
        if normalized is None:
            dispatcher.utter_message(text="Transaction amount must be numeric.")
            return { "transaction_amount": None }
        elif normalized <= 0:
            dispatcher.utter_message(text="Transaction amount must be positive.")
            return { "transaction_amount": None }
        logger.info("value: %s", normalized)
        return {"transaction_amount": normalized }

    @async_log_execution
    async def validate_buyer(self, value, dispatcher, tracker, domain):
        """
        * Validate the buyer in the transaction.
        If the buyer does not exist then initiate the register_user_form.
        """
        # If specified that they are the buyer then
        # use currently slotted username
        normalized = s_funcs.normalize_text(value)
        if re.match(r"\s*i\s+am\s*", normalized, flags=re.IGNORECASE):
            buyer = await self.get_user_name_from_state(tracker)
            return { "buyer": buyer }
        mtch = re.match(r"^\s*(?P<fn>\w+)\s+(?P<ln>\w+)\s*$", normalized)
        if not mtch:
            dispatcher.utter_message(text="Please provide a valid buyer (ex: 'first_name last_name').")
            return {"buyer": None}
        first_name = mtch["fn"]
        last_name = mtch["ln"]
        return { "buyer": f"{first_name} {last_name}" }
    
    @async_log_execution
    async def validate_vendor(self, value, dispatcher, tracker, domain):
        """
        * Validate the vendor. If they are not registered
        then trigger the validate vendor form immediately
        """
        logger.info("tracker.active_loop: %s", tracker.active_loop)
        normalized = s_funcs.normalize_text(value)
        corp_mtch = re.match(r"^\s*(?P<corp_name>[\w\s]+\s+)(?P<struct>inc|llc|corp)\s*$", normalized, flags=re.IGNORECASE)
        full_name = self.get_vendor_full_name(corp_mtch)
        vendor_token = await u_funcs.lookup_vendor_token(corp_name=full_name) if corp_mtch else None
        if corp_mtch and vendor_token:
            logger.info("vendor_token: %s", vendor_token)
            logger.info("Passed vendor %s matches corporate name, and is already registered.", full_name)
            return {"vendor": s_funcs.normalize_text(full_name),
                    "needs_vendor_registration": False}
        elif corp_mtch and vendor_token is None:
            logger.info("Passed vendor %s matches corporate name, and needs to be registered.", full_name)
            return (
                {
                    "vendor": s_funcs.normalize_text(full_name),
                    "corp_name": s_funcs.normalize_text(full_name),
                    "needs_vendor_registration": True,
                }
            )
        name_mtch = re.match(r"^\s*(?P<fn>\w+)\s+(?P<ln>\w+)\s*$", normalized)
        if not name_mtch:
            dispatcher.utter_message(text="Please provide a valid vendor (ex: 'first_name last_name' or corp name).")
            return {"vendor": None}
        logger.info("Non corporate name matched for vendor: %s", name_mtch[0])
        full_name = self.get_vendor_full_name(name_mtch)
        first_name, last_name = full_name.split(" ")
        vendor_token = await u_funcs.lookup_vendor_token(first_name=first_name, last_name=last_name)
        if vendor_token is None:
            logger.info("Vendor %s is not registered. Moving to registration.", full_name)
            return {"vendor": s_funcs.normalize_text(full_name),
                    "vendor_name": s_funcs.normalize_text(full_name),
                    "needs_vendor_registration": True}
        logger.info("Vendor %s is registered. Skipping registration.", full_name)
        return {"vendor": s_funcs.normalize_text(full_name),
                "needs_vendor_registration": False}
    
    @log_execution
    def validate_description(self, value, dispatcher, tracker, domain):
        """
        * Validate transaction description.
        """
        normalized = s_funcs.normalize_text(value, keep_punct=True)
        return {"description": normalized}
    
    @log_execution
    def validate_documentation(self, value, dispatcher, tracker, domain):
        """
        * Validate the transaction documentation.
        Here we are expecting a dummy response that is the filename
        that was pushed to the backend.
        """
        normalized = s_funcs.normalize_text(value, keep_punct=True)
        if not s_funcs.is_file_name(normalized):
            dispatcher.utter_message(text="Please provide a valid file name.")
            return { "documentation": None }
        return { "documentation": normalized }
    
    # Helpers:
    @log_execution
    def get_vendor_full_name(self, mtch:re.Match) -> Optional[str]:
        """
        * Retrieve the normalized vendor full name 
        based on possible patterns.
        """
        if mtch is None:
            return None
        elif mtch.groupdict().get("corp_name") and mtch.groupdict().get("struct"):
            corp_name = re.sub(r"^\s+|\s+$", "", mtch["corp_name"]).title()
            struct_name = re.sub(r"\s", "", mtch["struct"]).upper()
            full_name = corp_name + " " + struct_name
            return s_funcs.normalize_text(full_name)
        elif mtch.groupdict().get("first_name") and mtch.groupdict().get("last_name"):
            full_name = mtch["first_name"].title() + " " + mtch["last_name"].upper()
            return s_funcs.normalize_text(full_name)

    @async_log_execution
    async def get_user_name_from_state(self, tracker):
        """
        * Retrieve the user name based on the current slots.
        """
        user_name = tracker.get_slot("user_name")
        if user_name:
            return user_name
        # Use the token if was not set prior:
        user_token = u_funcs.get_user_token_from_tracker(tracker)
        user_info = await u_funcs.get_user_info_from_token(user_token)
        if not user_info:
            raise ValueError("User should be registered already.")
        user_info = user_info[0]
        first_name = user_info["first_name"].title()
        last_name = user_info["last_name"].title()
        user_name = f"{first_name} {last_name}"
        logger.info("user_name: %s", user_name)
        return user_name

class ValidateDisputeForm(FormValidationAction):
    
    def __init__(self):
        super().__init__()
        self.files_loaded = False

    def name(self) -> str:
        return "validate_dispute_form"

    @log_execution
    async def validate_dispute_vendor(self, value, dispatcher, tracker, domain):
        """
        * Validate the vendor that is in the dispute.
        """
        buyer_token = u_funcs.get_user_token_from_tracker(tracker)
        vendor_name = s_funcs.normalize_text(value, keep_punct=True)
        vendor_token = await u_funcs.lookup_vendor_token(vendor_name=vendor_name)
        if vendor_token is None:
            msg = f"We do not have any vendors registered as {vendor_name}."
            msg += "\nPlease pass valid vendor."
            dispatcher.utter_message(text=msg)
            return { "dispute_vendor": None }
        has_transactions = await t_funcs.buyer_has_transaction_with_vendor(buyer_token, vendor_name)
        if not has_transactions:
            logger.info("Buyer does not have any transactions outstanding with vendor %s.", vendor_name)
            msg = f"You do not have any transactions with {vendor_name}."
            msg += "\nPlease pass a vendor with whom you have transactions with."
            dispatcher.utter_message(text=msg)
            return { "dispute_vendor": None, "dispute_vendor_token": None }
        logger.info("Buyer has at least one transaction outstanding with vendor %s.", vendor_name)
        return { "dispute_vendor": vendor_name, "dispute_vendor_token": vendor_token }
    
    @log_execution
    async def validate_dispute_amount(self, value, dispatcher, tracker, domain):
        """
        * Validate the dispute amount.
        """
        normalized = s_funcs.normalize_numeric_text(value)
        amount = s_funcs.try_convert(normalized, float)
        if not amount:
            dispatcher.utter_message(text="Please pass a valid amount.")
            return { "dispute_amount": None }
        buyer_token = u_funcs.get_user_token_from_tracker(tracker)
        vendor_name = tracker.get_slot("dispute_vendor")
        vendor_token = tracker.get_slot("dispute_vendor_token")
        transactions = await t_funcs.lookup_transactions(user_token=buyer_token, 
                                                         vendor_token=vendor_token,
                                                         amount=amount)
        logger.info("transactions: %s", transactions)
        if not transactions:
            msg = f"You do not have any transactions outstanding with vendor {vendor_name}"
            msg += f"\nwith an amount of {amount}. Please pass an existing transaction amount."
            dispatcher.utter_message(text=msg)
            return { "dispute_amount": None }
        elif transactions is not None and len(transactions) > 1:
            msg = f"You have {len(transactions)} transactions with vendor {vendor_name}"
            msg += f"\nwith an amount of {amount}. Which one are you referring to?"
            dispatcher.utter_message(text=msg)
            return { "dispute_amount": None }
        else:
            transaction_id = transactions[0]["transaction_id"]
        # Get and set the transaction_id:
        logger.info("dispute_amount: %s", amount)
        # Generate the dispute and set the current dispute id:
        vendor_name = s_funcs.present_name(vendor_name)
        dispute_id = await c_funcs.create_dispute_from_slots(buyer_token, tracker)
        dispatcher.utter_message(text=f"Created dispute with vendor {vendor_name} with id {dispute_id}.")
        return { "dispute_id": dispute_id, "dispute_amount": amount, "dispute_transaction_id": transaction_id }
    
    @log_execution
    def validate_dispute_description(self, value, dispatcher, tracker, domain):
        """
        * Validate the vendor that is in the dispute.
        """
        normalized = s_funcs.normalize_text(value, keep_punct=True)
        return { "dispute_description": normalized }
    
    @log_execution
    def validate_evidence_file_name(self, value, dispatcher, tracker, domain):
        """
        * Ensure that a valid filename, or negative statement
        terminating the upload sequence, was passed.
        """
        normalized = s_funcs.normalize_text(value, keep_toks=["."])
        logger.info("file_name: %s", normalized)
        if self.is_negative_statement(normalized) and not self.files_loaded:
            dispatcher.utter_message(text="Please upload at least one file as evidence.")
            return {"evidence_file_name": None}
        elif self.is_negative_statement(normalized):
            logger.info("Stopping evidence aggregation.")
            dispatcher.utter_message(text="The evidence you have passed has been loaded.")
            return {"evidence_file_name": normalized}
        elif not s_funcs.is_file_name(normalized):
            dispatcher.utter_message(text="I do not understand the input.")
            return {"evidence_file_name": None}
        self.files_loaded = True
        logger.info("Continuing evidence aggregation until negative statement used.")
        dispatcher.utter_message("Thank you.")
        return {"evidence_file_name": None}

    def is_negative_statement(self, msg:str) -> bool:
        """
        * Indicate if should stop the evidence aggregation form.
        """
        return re.search(r"\bthat\s+is\s+all\b", msg, flags=re.IGNORECASE) is not None

class ActionCreateDispute(Action):
    """
    * Generate the dispute, and initialize
    the dispute evidence aggregation form.
    """
    def name(self):
        return "action_create_dispute"
    
    @async_log_execution
    async def run(self, 
                  dispatcher: CollectingDispatcher,
                  tracker: Tracker,
                  domain: dict):
        buyer_token = u_funcs.get_user_token_from_tracker(tracker)
        transaction_id = tracker.get_slot("dispute_transaction_id")
        vendor_name = tracker.get_slot("dispute_vendor")
        vendor_token = tracker.get_slot("dispute_vendor_token")
        description = tracker.get_slot("dispute_description")
        amount = tracker.get_slot("dispute_amount")
        data = {"transaction_id": transaction_id,
                "buyer_token": buyer_token,
                "vendor_token": vendor_token,
                "description": description,
                "amount": amount}
        logger.info("data: ")
        logger.info(json.dumps(data, indent=2))
        dispute_id = await c_funcs.create_dispute(data)
        logger.info("Created dispute with id %s.", dispute_id)
        logger.info("Moving to evidence aggregation form.")
        vendor_name = s_funcs.present_name(vendor_name)
        dispatcher.utter_message(text=f"Created dispute with vendor {vendor_name} with id {dispute_id}.")
        return [SlotSet("dispute_id", dispute_id)]
    
class ActionOutputDisputeJudgement(Action):
    """
    * Make a determination on what percentage of
    buyer's funds should be returned given the evidence
    uploaded by both vendor and buyer.
    """
    def name(self):
        return "action_output_dispute_judgement"
    
    @async_log_execution
    async def run(self, 
                  dispatcher: CollectingDispatcher,
                  tracker: Tracker,
                  domain: dict):
        # TMP:
        dispute_amount = tracker.get_slot("dispute_amount")
        returned_amt = round(dispute_amount * 1, 2)
        formatted = s_funcs.present_money(returned_amt)
        msg = "Based on the evidence uploaded by both parties, I am returning"
        msg += f"\n{formatted} (100% of the transaction amount {formatted}) to the buyer."
        dispatcher.utter_message(text=msg)

class ActionLoadTransactionForm(Action):
    """
    * Push new transaction information to
    the backend. 
    """
    def name(self):
        return "action_load_transaction_form"

    @async_log_execution
    async def run(self, 
                  dispatcher: CollectingDispatcher,
                  tracker: Tracker,
                  domain: dict):
        vendor = tracker.get_slot("vendor")
        needs_vendor_registration = tracker.get_slot("needs_vendor_registration")
        logger.info("vendor: %s", vendor)
        logger.info("needs_vendor_registration: %s", needs_vendor_registration)
        if needs_vendor_registration:            
            is_corp_name = u_funcs.vendor_is_corp_name(vendor)
            slot_name = "corp_name" if is_corp_name else "vendor_name"
            logger.info("Moving to register vendor with (%s %s)", slot_name, vendor)
            return [
                SlotSet(slot_name, vendor),
                FollowupAction("register_vendor_form")
            ]
        description = tracker.get_slot("description")
        amount = tracker.get_slot("transaction_amount")
        buyer = tracker.get_slot("buyer")
        documentation = tracker.get_slot("documentation")
        logger.info("buyer: %s", buyer)
        logger.info("vendor: %s", vendor)
        # Retrieve the buyer token and pass into the query:
        first_name, last_name = buyer.split(" ")
        buyer_token = await u_funcs.lookup_user_token(first_name=first_name, last_name=last_name)
        if u_funcs.vendor_is_corp_name(vendor):
            vendor_lookup = {"corp_name": vendor}
        else:
            first_name, last_name = vendor.split(" ")
            vendor_lookup = {"first_name": first_name, "last_name": last_name}
        logger.info("vendor_lookup: %s", vendor_lookup)
        vendor_token = await u_funcs.lookup_vendor_token(**vendor_lookup)
        # Retrieve the vendor token and pass into the query:
        data = {"description": description,
                "transaction_amount": amount,
                "buyer_token": buyer_token, 
                "vendor_token": vendor_token,
                "documentation": documentation}
        logger.info("data: ")
        logger.info(json.dumps(data, indent=2))
        # Check if the transaction already exists.
        # If no collision then generate the new transaction, returning the transaction unique key.
        # If there is a collision:
        #   1. Ask if it is a new transaction with the same vendor (since collides with the amount).
        #   - If is then load the new transaction and return the new transaction id.
        #   - If isn't then clarify if is new transaction, and rerun the load:
        transaction_id = await t_funcs.get_transaction_id(data)
        if transaction_id is None:
            data["buyer"] = buyer
            data["vendor"] = vendor
            logger.info("Transaction does not exist. Registering to backend.")
            transaction_id = await t_funcs.load_transaction(data)
            summary = self.generate_transaction_summary(transaction_id, data)
            dispatcher.utter_message(text=summary)
            return [SlotSet("transaction_id", transaction_id)]
        else:
            dispatcher.utter_message(text="A similar transaction already exists. Is this a new one?")

    def generate_transaction_summary(self, transaction_id:str, data:dict):
        """
        * Output the new transaction summary with key details
        """
        data["transaction_id"] = transaction_id
        buyer = s_funcs.present_name(data["buyer"])
        vendor = s_funcs.present_name(data["vendor"])
        trans_amt = s_funcs.present_money(data["transaction_amount"])
        summary = f"""Recorded transaction between buyer {buyer}
        and vendor {vendor} for amount {trans_amt}
        with transaction id {transaction_id}.
        """.format(**data)
        return summary

class ActionVendorContext(Action):
    """
    * Switch chat context to vendor instead of consumer.
    """
    def name(self):
        return "action_vendor_interface"

    @async_log_execution
    async def run(self, 
                  dispatcher: CollectingDispatcher,
                  tracker: Tracker,
                  domain: dict):
        user_token = u_funcs.get_user_token_from_tracker(tracker)
        logger.info("user_token: %s", user_token)
        await self.display_vendor_context_switch(user_token, dispatcher)
        # List options with feedback
        #return [SlotSet("requested_slot", None), FollowupAction("transaction_form")]
    
    @log_execution
    async def display_vendor_context_switch(self, 
                                            user_token:str, 
                                            dispatcher: CollectingDispatcher):
        """
        * Display welcome message for a vendor.
        """
        metadata = await u_funcs.get_vendor_meta(user_token)
        length = (datetime.now() - metadata["timestamp"]).days
        text = f"Thank you for being a dedicated vendor for {length} days!"
        text += "\nWe really appreciate your service"
        if metadata["n_strikes"] == 0:
            text += " with no issues!"
        else:
            text += "!"
        dispatcher.utter_message(text=text)

class ActionSarcasm(Action):
    """
    * Display sarcastic responses when user passes in
    irrelevant text.
    """
    def __init__(self, *args, **kwargs):
        self.model_id = "Sriram-Gov/Sarcastic-Headline-Llama2"
        self.tokenizer = None# AutoTokenizer.from_pretrained(self.model_id)
        self.model = None
        """AutoModelForCausalLM.from_pretrained(
            self.model_id,
            torch_dtype=torch.float16,
            device_map="auto",
            offload_folder="/tmp/model_offload"
        )"""
        super().__init__()

    def name(self):
        return "action_sarcasm"

    @async_log_execution
    async def run(self, 
                  dispatcher: CollectingDispatcher,
                  tracker: Tracker,
                  domain: dict):
        self.display_sarcasm(dispatcher, tracker)
    
    @log_execution
    def display_sarcasm(self, dispatcher: CollectingDispatcher, tracker: Tracker):
        """
        * Display sarcastic text until context is switched.
        """
        input_text = tracker.latest_message.get("text", "")
        prompt = (
            "You are a witty, sarcastic AI. "
            "Rewrite the following statement in a funny, sarcastic way:\n\n"
            f"Input: {input_text}\n"
            "Sarcastic response:"
        )
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=80,    # how long the reply can be
            temperature=0.9,      # randomness (0.7–1.0 gives creative responses)
            top_p=0.9,            # nucleus sampling (probability mass cutoff)
            do_sample=True,       # enables randomness
            repetition_penalty=1.1,
            pad_token_id=self.tokenizer.eos_token_id,
        )
        logger.info("outputs: %s", outputs)
        text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        cleaned = re.search(r"Sarcastic response:\s+(?P<txt>.+)$", text)["txt"]
        dispatcher.utter_message(text=cleaned)