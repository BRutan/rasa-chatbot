-- schemas:
CREATE SCHEMA accounts;
CREATE SCHEMA auth;
CREATE SCHEMA chatbot;
CREATE SCHEMA demo;
CREATE SCHEMA evidence;
CREATE SCHEMA cases;
CREATE SCHEMA transactions;
CREATE SCHEMA users;

CREATE TABLE users.credentials
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    token VARCHAR NOT NULL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- accounts:
CREATE TABLE accounts.bank_accounts
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    user_token VARCHAR PRIMARY KEY,
    account_number VARCHAR NOT NULL,
    routing_number VARCHAR NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE accounts.escrow
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_number VARCHAR NOT NULL,
    routing_number VARCHAR NOT NULL,
    source_account_id INT NOT NULL REFERENCES accounts.bank_accounts(id) ON UPDATE CASCADE ON DELETE CASCADE NOT NULL,
    dest_account_id INT NOT NULL REFERENCES accounts.bank_accounts(id) ON UPDATE CASCADE ON DELETE CASCADE NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (account_number, routing_number)
);

-- auth:
CREATE TABLE auth.admin_tokens
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    token VARCHAR NOT NULL UNIQUE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- cases:
CREATE TABLE cases.disputes
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id INT NOT NULL,
    buyer_token VARCHAR NOT NULL,
    vendor_token VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    amount MONEY NOT NULL,
    opened_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    closed_ts TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (transaction_id, buyer_token, vendor_token, amount)
);

-- chatbot:
CREATE TABLE chatbot.sarcasm
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    text VARCHAR NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (text)
);

-- evidence:
CREATE TABLE evidence.images
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    uploaded_user_token VARCHAR NOT NULL,
    case_id INT NOT NULL REFERENCES cases.disputes(id) ON UPDATE CASCADE ON DELETE CASCADE,
    image_hash VARCHAR NOT NULL,
    image_bytes BYTEA NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (uploaded_user_token, case_id, image_hash),
    FOREIGN KEY (uploaded_user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE evidence.emails
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    uploaded_user_token VARCHAR NOT NULL,
    case_id INT NOT NULL REFERENCES cases.disputes(id) ON UPDATE CASCADE ON DELETE CASCADE,
    email_to VARCHAR NOT NULL,
    email_from VARCHAR NOT NULL,
    email_text VARCHAR NOT NULL,
    email_hash VARCHAR NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (uploaded_user_token, case_id, email_to, email_from, email_hash),
    FOREIGN KEY (uploaded_user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE evidence.texts
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    uploaded_user_token VARCHAR NOT NULL,
    case_id INT NOT NULL REFERENCES cases.disputes(id) ON UPDATE CASCADE ON DELETE CASCADE,
    number_to VARCHAR NOT NULL,
    number_from VARCHAR NOT NULL,
    content VARCHAR NOT NULL,
    content_hash VARCHAR NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (uploaded_user_token, case_id, number_to, number_from, content_hash),
    FOREIGN KEY (uploaded_user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE evidence.videos
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    uploaded_user_token VARCHAR NOT NULL,
    case_id INT NOT NULL REFERENCES cases.disputes(id) ON UPDATE CASCADE ON DELETE CASCADE,
    s3_path VARCHAR NOT NULL UNIQUE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY (uploaded_user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE
);

-- users:
CREATE TABLE users.vendors
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    user_token VARCHAR NOT NULL PRIMARY KEY,
    corp_name VARCHAR,
    n_strikes INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY (user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE users.user_info
(
    id INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    user_token VARCHAR NOT NULL PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL UNIQUE,
    phone_number VARCHAR NOT NULL UNIQUE,
    drivers_license BYTEA,
    passport BYTEA,
    non_drivers_license BYTEA,
    address VARCHAR NOT NULL,
    city VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    zip_code VARCHAR NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY (user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE
);

-- transactions:
-- fact table:
CREATE TABLE transactions.transactions
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    buyer_token VARCHAR REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE NOT NULL,
    vendor_token VARCHAR REFERENCES users.vendors(user_token) ON UPDATE CASCADE ON DELETE CASCADE NOT NULL,
    escrow_account_id INT REFERENCES accounts.escrow(id) ON UPDATE CASCADE ON DELETE CASCADE NOT NULL UNIQUE,
    transaction_amount MONEY NOT NULL,
    description VARCHAR NOT NULL,
    documentation VARCHAR,
    opened_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);  

CREATE TABLE transactions.transaction_status
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id INT NOT NULL REFERENCES transactions.transactions(id) ON UPDATE CASCADE ON DELETE CASCADE NOT NULL,
    status VARCHAR NOT NULL,
    status_change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE transactions.contract
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id INT NOT NULL REFERENCES transactions.transactions(id) ON UPDATE CASCADE ON DELETE CASCADE NOT NULL,
    recitals VARCHAR NOT NULL,
    scope_of_services VARCHAR NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- dimension tables:
CREATE TABLE transactions.transaction_documents
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id INT REFERENCES transactions.transactions(id) NOT NULL,
    uploaded_user_token VARCHAR REFERENCES users.credentials(token) NOT NULL,
    s3_path VARCHAR NOT NULL,
    raw_text VARCHAR NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transactions.transactions(id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (uploaded_user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE
);

-- Add foreign keys:
ALTER TABLE accounts.bank_accounts ADD CONSTRAINT fk_user_token FOREIGN KEY (user_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE cases.disputes ADD CONSTRAINT fk_vendor FOREIGN KEY (vendor_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE;
ALTER TABLE cases.disputes ADD CONSTRAINT fk_buyer FOREIGN KEY (buyer_token) REFERENCES users.credentials(token) ON UPDATE CASCADE ON DELETE CASCADE;