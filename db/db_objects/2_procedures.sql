CREATE OR REPLACE PROCEDURE demo.reset_demo_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM transactions.transaction_status WHERE 1 = 1;
    ALTER SEQUENCE transactions.transaction_status_id_seq RESTART WITH 1;

    DELETE FROM transactions.contract where 1 = 1;
    ALTER SEQUENCE transactions.contract_id_seq RESTART WITH 1;

    DELETE FROM transactions.transactions where 1 = 1;
    ALTER SEQUENCE transactions.transactions_id_seq RESTART WITH 1;

    DELETE FROM transactions.transaction_documents where 1 = 1;
    ALTER SEQUENCE transactions.transaction_documents_id_seq RESTART WITH 1;

    DELETE FROM users.user_info WHERE 1 = 1;
    ALTER SEQUENCE users.user_info_id_seq RESTART WITH 1;

    DELETE FROM users.vendors WHERE 1 = 1;
    ALTER SEQUENCE users.vendors_id_seq RESTART WITH 1;

    DELETE FROM evidence.videos WHERE 1 = 1;
    ALTER SEQUENCE evidence.videos_id_seq RESTART WITH 1;

    DELETE FROM evidence.texts WHERE 1 = 1;
    ALTER SEQUENCE evidence.texts_id_seq RESTART WITH 1;

    DELETE FROM evidence.emails WHERE 1 = 1;
    ALTER SEQUENCE evidence.emails_id_seq RESTART WITH 1;

    DELETE FROM evidence.images WHERE 1 = 1;
    ALTER SEQUENCE evidence.images_id_seq RESTART WITH 1;

    DELETE FROM cases.disputes WHERE 1 = 1;
    ALTER SEQUENCE cases.disputes_id_seq RESTART WITH 1;

    DELETE FROM accounts.escrow WHERE 1 = 1;
    ALTER SEQUENCE accounts.escrow_id_seq RESTART WITH 1;

    DELETE FROM accounts.bank_accounts WHERE 1 = 1;
    ALTER SEQUENCE accounts.bank_accounts_id_seq RESTART WITH 1;
    
    DELETE FROM users.credentials WHERE 1 = 1;
    ALTER SEQUENCE users.credentials_id_seq RESTART WITH 1;

    -- Delete all records from the rasa tracker table (events)
    -- if it exists:
    IF EXISTS (
        SELECT 1 
        FROM pg_tables 
        WHERE schemaname = 'public' 
          AND tablename = 'events'
    ) THEN
        DELETE FROM public.events WHERE 1 = 1;
    END IF;
    
END;
$$;