--
-- PostgreSQL database dump
--

-- Dumped from database version 14.3
-- Dumped by pg_dump version 14.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: explorer; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA explorer;


--
-- Name: event_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.event_type AS ENUM (
    'Custom',
    'RecordViewKey',
    'Operation'
    );


--
-- Name: function_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.function_type AS ENUM (
    'Noop',
    'Insert',
    'Update',
    'Remove',
    'DoubleInsert',
    'DoubleRemove',
    'Join',
    'Split',
    'Full'
    );


--
-- Name: operation_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.operation_type AS ENUM (
    'Noop',
    'Coinbase',
    'Transfer',
    'Evaluate'
    );


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: block; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.block
(
    id                   integer              NOT NULL,
    height               integer              NOT NULL,
    block_hash           text                 NOT NULL,
    previous_block_hash  text                 NOT NULL,
    previous_ledger_root text                 NOT NULL,
    transactions_root    text                 NOT NULL,
    "timestamp"          bigint               NOT NULL,
    difficulty_target    numeric(20, 0)       NOT NULL,
    cumulative_weight    numeric(40, 0)       NOT NULL,
    nonce                text                 NOT NULL,
    proof                text                 NOT NULL,
    is_canonical         boolean DEFAULT true NOT NULL
);


--
-- Name: blocks_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.blocks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: blocks_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.blocks_id_seq OWNED BY explorer.block.id;


--
-- Name: ciphertext; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.ciphertext
(
    id            integer NOT NULL,
    transition_id integer NOT NULL,
    index         integer NOT NULL,
    ciphertext    text    NOT NULL
);


--
-- Name: ciphertext_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.ciphertext_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ciphertext_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.ciphertext_id_seq OWNED BY explorer.ciphertext.id;


--
-- Name: coinbase_operation; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.coinbase_operation
(
    id                 integer NOT NULL,
    operation_event_id integer NOT NULL,
    recipient          text    NOT NULL,
    amount             bigint  NOT NULL
);


--
-- Name: coinbase_operation_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.coinbase_operation_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: coinbase_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.coinbase_operation_id_seq OWNED BY explorer.coinbase_operation.id;


--
-- Name: custom_event; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.custom_event
(
    id       integer NOT NULL,
    event_id integer NOT NULL,
    bytes    bytea   NOT NULL
);


--
-- Name: custom_event_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.custom_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: custom_event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.custom_event_id_seq OWNED BY explorer.custom_event.id;


--
-- Name: evaluate_operation; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.evaluate_operation
(
    id                 integer                NOT NULL,
    operation_event_id integer                NOT NULL,
    function_id        text                   NOT NULL,
    function_type      explorer.function_type NOT NULL,
    caller             text                   NOT NULL,
    recipient          text                   NOT NULL,
    amount             bigint                 NOT NULL,
    record_payload     bytea                  NOT NULL
);


--
-- Name: evaluate_operation_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.evaluate_operation_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: evaluate_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.evaluate_operation_id_seq OWNED BY explorer.evaluate_operation.id;


--
-- Name: event; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.event
(
    id            integer             NOT NULL,
    transition_id integer             NOT NULL,
    index         integer             NOT NULL,
    event_type    explorer.event_type NOT NULL
);


--
-- Name: event_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.event_id_seq OWNED BY explorer.event.id;


--
-- Name: operation_event; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.operation_event
(
    id             integer                 NOT NULL,
    event_id       integer                 NOT NULL,
    operation_type explorer.operation_type NOT NULL
);


--
-- Name: operation_event_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.operation_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: operation_event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.operation_event_id_seq OWNED BY explorer.operation_event.id;


--
-- Name: record; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.record
(
    id                       integer NOT NULL,
    output_transition_id     integer NOT NULL,
    record_view_key_event_id integer NOT NULL,
    ciphertext_id            integer NOT NULL,
    owner                    text    NOT NULL,
    value                    bigint  NOT NULL,
    payload                  bytea   NOT NULL,
    program_id               text    NOT NULL,
    randomizer               text    NOT NULL,
    commitment               text    NOT NULL
);


--
-- Name: record_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.record_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: record_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.record_id_seq OWNED BY explorer.record.id;


--
-- Name: record_view_key_event; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.record_view_key_event
(
    id              integer  NOT NULL,
    event_id        integer  NOT NULL,
    index           smallint NOT NULL,
    record_view_key text     NOT NULL
);


--
-- Name: record_view_key_event_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.record_view_key_event_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: record_view_key_event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.record_view_key_event_id_seq OWNED BY explorer.record_view_key_event.id;


--
-- Name: serial_number; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.serial_number
(
    id            integer  NOT NULL,
    transition_id integer  NOT NULL,
    index         smallint NOT NULL,
    serial_number text     NOT NULL
);


--
-- Name: serial_number_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.serial_number_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: serial_number_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.serial_number_id_seq OWNED BY explorer.serial_number.id;


--
-- Name: transaction; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transaction
(
    id               integer NOT NULL,
    block_id         integer NOT NULL,
    transaction_id   text    NOT NULL,
    inner_circuit_id text    NOT NULL,
    ledger_root      text
);


--
-- Name: transaction_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transaction_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transaction_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transaction_id_seq OWNED BY explorer.transaction.id;


--
-- Name: transfer_operation; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transfer_operation
(
    id                 integer NOT NULL,
    operation_event_id integer NOT NULL,
    caller             text    NOT NULL,
    recipient          text    NOT NULL,
    amount             bigint  NOT NULL
);


--
-- Name: transfer_operation_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transfer_operation_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transfer_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transfer_operation_id_seq OWNED BY explorer.transfer_operation.id;


--
-- Name: transition_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition
(
    id             integer DEFAULT nextval('explorer.transition_id_seq'::regclass) NOT NULL,
    transaction_id integer,
    transition_id  text                                                            NOT NULL,
    value_balance  bigint,
    proof          text
);


--
-- Name: block id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block
    ALTER COLUMN id SET DEFAULT nextval('explorer.blocks_id_seq'::regclass);


--
-- Name: ciphertext id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ciphertext
    ALTER COLUMN id SET DEFAULT nextval('explorer.ciphertext_id_seq'::regclass);


--
-- Name: coinbase_operation id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_operation
    ALTER COLUMN id SET DEFAULT nextval('explorer.coinbase_operation_id_seq'::regclass);


--
-- Name: custom_event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.custom_event
    ALTER COLUMN id SET DEFAULT nextval('explorer.custom_event_id_seq'::regclass);


--
-- Name: evaluate_operation id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.evaluate_operation
    ALTER COLUMN id SET DEFAULT nextval('explorer.evaluate_operation_id_seq'::regclass);


--
-- Name: event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.event
    ALTER COLUMN id SET DEFAULT nextval('explorer.event_id_seq'::regclass);


--
-- Name: operation_event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.operation_event
    ALTER COLUMN id SET DEFAULT nextval('explorer.operation_event_id_seq'::regclass);


--
-- Name: record id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ALTER COLUMN id SET DEFAULT nextval('explorer.record_id_seq'::regclass);


--
-- Name: record_view_key_event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record_view_key_event
    ALTER COLUMN id SET DEFAULT nextval('explorer.record_view_key_event_id_seq'::regclass);


--
-- Name: serial_number id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.serial_number
    ALTER COLUMN id SET DEFAULT nextval('explorer.serial_number_id_seq'::regclass);


--
-- Name: transaction id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ALTER COLUMN id SET DEFAULT nextval('explorer.transaction_id_seq'::regclass);


--
-- Name: transfer_operation id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transfer_operation
    ALTER COLUMN id SET DEFAULT nextval('explorer.transfer_operation_id_seq'::regclass);


--
-- Name: block block_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block
    ADD CONSTRAINT block_pk PRIMARY KEY (id);


--
-- Name: ciphertext ciphertext_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ciphertext
    ADD CONSTRAINT ciphertext_pk PRIMARY KEY (id);


--
-- Name: coinbase_operation coinbase_operation_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_operation
    ADD CONSTRAINT coinbase_operation_pkey PRIMARY KEY (id);


--
-- Name: custom_event custom_event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.custom_event
    ADD CONSTRAINT custom_event_pk PRIMARY KEY (id);


--
-- Name: evaluate_operation evaluate_operation_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.evaluate_operation
    ADD CONSTRAINT evaluate_operation_pkey PRIMARY KEY (id);


--
-- Name: event event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.event
    ADD CONSTRAINT event_pk PRIMARY KEY (id);


--
-- Name: operation_event operation_event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.operation_event
    ADD CONSTRAINT operation_event_pk PRIMARY KEY (id);


--
-- Name: record record_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_pk PRIMARY KEY (id);


--
-- Name: record_view_key_event record_view_key_event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record_view_key_event
    ADD CONSTRAINT record_view_key_event_pk PRIMARY KEY (id);


--
-- Name: serial_number serial_number_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.serial_number
    ADD CONSTRAINT serial_number_pk PRIMARY KEY (id);


--
-- Name: transaction transaction_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ADD CONSTRAINT transaction_pk PRIMARY KEY (id);


--
-- Name: transfer_operation transfer_operation_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transfer_operation
    ADD CONSTRAINT transfer_operation_pkey PRIMARY KEY (id);


--
-- Name: transition transition_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition
    ADD CONSTRAINT transition_pk PRIMARY KEY (id);


--
-- Name: block_block_hash_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX block_block_hash_uindex ON explorer.block USING btree (block_hash text_pattern_ops);


--
-- Name: block_height_block_hash_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX block_height_block_hash_uindex ON explorer.block USING btree (height, block_hash);


--
-- Name: block_height_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX block_height_index ON explorer.block USING btree (height DESC);


--
-- Name: ciphertext_transition_id_index_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX ciphertext_transition_id_index_uindex ON explorer.ciphertext USING btree (transition_id, index);


--
-- Name: coinbase_operation_operation_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX coinbase_operation_operation_event_id_uindex ON explorer.coinbase_operation USING btree (operation_event_id);


--
-- Name: custom_event_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX custom_event_event_id_uindex ON explorer.custom_event USING btree (event_id);


--
-- Name: evaluate_operation_operation_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX evaluate_operation_operation_event_id_uindex ON explorer.evaluate_operation USING btree (operation_event_id);


--
-- Name: event_transition_id_index_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX event_transition_id_index_uindex ON explorer.event USING btree (transition_id, index);


--
-- Name: operation_event_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX operation_event_event_id_uindex ON explorer.operation_event USING btree (event_id);


--
-- Name: record_output_transition_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX record_output_transition_id_index ON explorer.record USING btree (output_transition_id);


--
-- Name: record_owner_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX record_owner_index ON explorer.record USING btree (owner text_pattern_ops);


--
-- Name: record_record_view_key_event_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX record_record_view_key_event_id_index ON explorer.record USING btree (record_view_key_event_id);


--
-- Name: record_view_key_event_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX record_view_key_event_event_id_uindex ON explorer.record_view_key_event USING btree (event_id);


--
-- Name: serial_number_serial_number_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX serial_number_serial_number_index ON explorer.serial_number USING btree (serial_number text_pattern_ops);


--
-- Name: serial_number_transition_id_index_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX serial_number_transition_id_index_uindex ON explorer.serial_number USING btree (transition_id, index);


--
-- Name: transaction_block_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transaction_block_id_index ON explorer.transaction USING btree (block_id);


--
-- Name: transaction_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transaction_transaction_id_index ON explorer.transaction USING btree (transaction_id text_pattern_ops);


--
-- Name: transfer_operation_operation_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX transfer_operation_operation_event_id_uindex ON explorer.transfer_operation USING btree (operation_event_id);


--
-- Name: transition_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_transaction_id_index ON explorer.transition USING btree (transaction_id);


--
-- Name: transition_transition_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_transition_id_index ON explorer.transition USING btree (transition_id text_pattern_ops);


--
-- Name: ciphertext ciphertext_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ciphertext
    ADD CONSTRAINT ciphertext_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition (id);


--
-- Name: coinbase_operation coinbase_operation_operation_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_operation
    ADD CONSTRAINT coinbase_operation_operation_event_id_fk FOREIGN KEY (operation_event_id) REFERENCES explorer.operation_event (id);


--
-- Name: custom_event custom_event_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.custom_event
    ADD CONSTRAINT custom_event_event_id_fk FOREIGN KEY (event_id) REFERENCES explorer.event (id);


--
-- Name: evaluate_operation evaluate_operation_operation_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.evaluate_operation
    ADD CONSTRAINT evaluate_operation_operation_event_id_fk FOREIGN KEY (operation_event_id) REFERENCES explorer.operation_event (id);


--
-- Name: event event_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.event
    ADD CONSTRAINT event_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition (id);


--
-- Name: operation_event operation_event_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.operation_event
    ADD CONSTRAINT operation_event_event_id_fk FOREIGN KEY (event_id) REFERENCES explorer.event (id);


--
-- Name: record record_ciphertext_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_ciphertext_id_fk FOREIGN KEY (ciphertext_id) REFERENCES explorer.ciphertext (id);


--
-- Name: record record_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_event_id_fk FOREIGN KEY (record_view_key_event_id) REFERENCES explorer.event (id);


--
-- Name: record record_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_transition_id_fk FOREIGN KEY (output_transition_id) REFERENCES explorer.transition (id);


--
-- Name: record_view_key_event record_view_key_event_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record_view_key_event
    ADD CONSTRAINT record_view_key_event_event_id_fk FOREIGN KEY (event_id) REFERENCES explorer.event (id);


--
-- Name: serial_number serial_number_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.serial_number
    ADD CONSTRAINT serial_number_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition (id);


--
-- Name: transaction transaction_block_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ADD CONSTRAINT transaction_block_id_fk FOREIGN KEY (block_id) REFERENCES explorer.block (id);


--
-- Name: transfer_operation transfer_operation_operation_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transfer_operation
    ADD CONSTRAINT transfer_operation_operation_event_id_fk FOREIGN KEY (operation_event_id) REFERENCES explorer.operation_event (id);


--
-- Name: transition transition_transaction_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition
    ADD CONSTRAINT transition_transaction_id_fk FOREIGN KEY (transaction_id) REFERENCES explorer.transaction (id);


--
-- PostgreSQL database dump complete
--

