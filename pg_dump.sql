--
-- PostgreSQL database dump
--

-- Dumped from database version 14.3
-- Dumped by pg_dump version 14.2

-- Started on 2022-06-16 02:15:12 JST

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
-- TOC entry 4 (class 2615 OID 16386)
-- Name: explorer; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA explorer;


--
-- TOC entry 871 (class 1247 OID 17090)
-- Name: event_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.event_type AS ENUM (
    'Custom',
    'RecordViewKey',
    'Operation'
    );


--
-- TOC entry 889 (class 1247 OID 17128)
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
-- TOC entry 868 (class 1247 OID 17081)
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
-- TOC entry 210 (class 1259 OID 16387)
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
-- TOC entry 211 (class 1259 OID 16392)
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
-- TOC entry 3717 (class 0 OID 0)
-- Dependencies: 211
-- Name: blocks_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.blocks_id_seq OWNED BY explorer.block.id;


--
-- TOC entry 219 (class 1259 OID 16485)
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
-- TOC entry 218 (class 1259 OID 16484)
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
-- TOC entry 3718 (class 0 OID 0)
-- Dependencies: 218
-- Name: ciphertext_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.ciphertext_id_seq OWNED BY explorer.ciphertext.id;


--
-- TOC entry 229 (class 1259 OID 17098)
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
-- TOC entry 228 (class 1259 OID 17097)
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
-- TOC entry 3719 (class 0 OID 0)
-- Dependencies: 228
-- Name: coinbase_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.coinbase_operation_id_seq OWNED BY explorer.coinbase_operation.id;


--
-- TOC entry 223 (class 1259 OID 17019)
-- Name: custom_event; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.custom_event
(
    id       integer NOT NULL,
    event_id integer NOT NULL,
    bytes    bytea   NOT NULL
);


--
-- TOC entry 222 (class 1259 OID 17018)
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
-- TOC entry 3720 (class 0 OID 0)
-- Dependencies: 222
-- Name: custom_event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.custom_event_id_seq OWNED BY explorer.custom_event.id;


--
-- TOC entry 233 (class 1259 OID 17148)
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
-- TOC entry 232 (class 1259 OID 17147)
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
-- TOC entry 3721 (class 0 OID 0)
-- Dependencies: 232
-- Name: evaluate_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.evaluate_operation_id_seq OWNED BY explorer.evaluate_operation.id;


--
-- TOC entry 221 (class 1259 OID 16508)
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
-- TOC entry 220 (class 1259 OID 16507)
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
-- TOC entry 3722 (class 0 OID 0)
-- Dependencies: 220
-- Name: event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.event_id_seq OWNED BY explorer.event.id;


--
-- TOC entry 227 (class 1259 OID 17068)
-- Name: operation_event; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.operation_event
(
    id             integer                 NOT NULL,
    event_id       integer                 NOT NULL,
    operation_type explorer.operation_type NOT NULL
);


--
-- TOC entry 226 (class 1259 OID 17067)
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
-- TOC entry 3723 (class 0 OID 0)
-- Dependencies: 226
-- Name: operation_event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.operation_event_id_seq OWNED BY explorer.operation_event.id;


--
-- TOC entry 235 (class 1259 OID 17166)
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
-- TOC entry 234 (class 1259 OID 17165)
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
-- TOC entry 3724 (class 0 OID 0)
-- Dependencies: 234
-- Name: record_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.record_id_seq OWNED BY explorer.record.id;


--
-- TOC entry 225 (class 1259 OID 17034)
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
-- TOC entry 224 (class 1259 OID 17033)
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
-- TOC entry 3725 (class 0 OID 0)
-- Dependencies: 224
-- Name: record_view_key_event_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.record_view_key_event_id_seq OWNED BY explorer.record_view_key_event.id;


--
-- TOC entry 217 (class 1259 OID 16469)
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
-- TOC entry 216 (class 1259 OID 16468)
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
-- TOC entry 3726 (class 0 OID 0)
-- Dependencies: 216
-- Name: serial_number_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.serial_number_id_seq OWNED BY explorer.serial_number.id;


--
-- TOC entry 213 (class 1259 OID 16406)
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
-- TOC entry 212 (class 1259 OID 16405)
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
-- TOC entry 3727 (class 0 OID 0)
-- Dependencies: 212
-- Name: transaction_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transaction_id_seq OWNED BY explorer.transaction.id;


--
-- TOC entry 231 (class 1259 OID 17113)
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
-- TOC entry 230 (class 1259 OID 17112)
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
-- TOC entry 3728 (class 0 OID 0)
-- Dependencies: 230
-- Name: transfer_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transfer_operation_id_seq OWNED BY explorer.transfer_operation.id;


--
-- TOC entry 214 (class 1259 OID 16442)
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
-- TOC entry 215 (class 1259 OID 16443)
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
-- TOC entry 3501 (class 2604 OID 16393)
-- Name: block id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block
    ALTER COLUMN id SET DEFAULT nextval('explorer.blocks_id_seq'::regclass);


--
-- TOC entry 3506 (class 2604 OID 16488)
-- Name: ciphertext id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ciphertext
    ALTER COLUMN id SET DEFAULT nextval('explorer.ciphertext_id_seq'::regclass);


--
-- TOC entry 3511 (class 2604 OID 17101)
-- Name: coinbase_operation id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_operation
    ALTER COLUMN id SET DEFAULT nextval('explorer.coinbase_operation_id_seq'::regclass);


--
-- TOC entry 3508 (class 2604 OID 17022)
-- Name: custom_event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.custom_event
    ALTER COLUMN id SET DEFAULT nextval('explorer.custom_event_id_seq'::regclass);


--
-- TOC entry 3513 (class 2604 OID 17151)
-- Name: evaluate_operation id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.evaluate_operation
    ALTER COLUMN id SET DEFAULT nextval('explorer.evaluate_operation_id_seq'::regclass);


--
-- TOC entry 3507 (class 2604 OID 16511)
-- Name: event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.event
    ALTER COLUMN id SET DEFAULT nextval('explorer.event_id_seq'::regclass);


--
-- TOC entry 3510 (class 2604 OID 17071)
-- Name: operation_event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.operation_event
    ALTER COLUMN id SET DEFAULT nextval('explorer.operation_event_id_seq'::regclass);


--
-- TOC entry 3514 (class 2604 OID 17169)
-- Name: record id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ALTER COLUMN id SET DEFAULT nextval('explorer.record_id_seq'::regclass);


--
-- TOC entry 3509 (class 2604 OID 17037)
-- Name: record_view_key_event id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record_view_key_event
    ALTER COLUMN id SET DEFAULT nextval('explorer.record_view_key_event_id_seq'::regclass);


--
-- TOC entry 3505 (class 2604 OID 16472)
-- Name: serial_number id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.serial_number
    ALTER COLUMN id SET DEFAULT nextval('explorer.serial_number_id_seq'::regclass);


--
-- TOC entry 3503 (class 2604 OID 16409)
-- Name: transaction id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ALTER COLUMN id SET DEFAULT nextval('explorer.transaction_id_seq'::regclass);


--
-- TOC entry 3512 (class 2604 OID 17116)
-- Name: transfer_operation id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transfer_operation
    ALTER COLUMN id SET DEFAULT nextval('explorer.transfer_operation_id_seq'::regclass);


--
-- TOC entry 3518 (class 2606 OID 16451)
-- Name: block block_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block
    ADD CONSTRAINT block_pk PRIMARY KEY (id);


--
-- TOC entry 3532 (class 2606 OID 16493)
-- Name: ciphertext ciphertext_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ciphertext
    ADD CONSTRAINT ciphertext_pk PRIMARY KEY (id);


--
-- TOC entry 3548 (class 2606 OID 17105)
-- Name: coinbase_operation coinbase_operation_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_operation
    ADD CONSTRAINT coinbase_operation_pkey PRIMARY KEY (id);


--
-- TOC entry 3539 (class 2606 OID 17026)
-- Name: custom_event custom_event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.custom_event
    ADD CONSTRAINT custom_event_pk PRIMARY KEY (id);


--
-- TOC entry 3554 (class 2606 OID 17155)
-- Name: evaluate_operation evaluate_operation_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.evaluate_operation
    ADD CONSTRAINT evaluate_operation_pkey PRIMARY KEY (id);


--
-- TOC entry 3535 (class 2606 OID 16516)
-- Name: event event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.event
    ADD CONSTRAINT event_pk PRIMARY KEY (id);


--
-- TOC entry 3545 (class 2606 OID 17073)
-- Name: operation_event operation_event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.operation_event
    ADD CONSTRAINT operation_event_pk PRIMARY KEY (id);


--
-- TOC entry 3557 (class 2606 OID 17173)
-- Name: record record_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_pk PRIMARY KEY (id);


--
-- TOC entry 3542 (class 2606 OID 17041)
-- Name: record_view_key_event record_view_key_event_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record_view_key_event
    ADD CONSTRAINT record_view_key_event_pk PRIMARY KEY (id);


--
-- TOC entry 3528 (class 2606 OID 16478)
-- Name: serial_number serial_number_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.serial_number
    ADD CONSTRAINT serial_number_pk PRIMARY KEY (id);


--
-- TOC entry 3521 (class 2606 OID 16441)
-- Name: transaction transaction_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ADD CONSTRAINT transaction_pk PRIMARY KEY (id);


--
-- TOC entry 3551 (class 2606 OID 17120)
-- Name: transfer_operation transfer_operation_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transfer_operation
    ADD CONSTRAINT transfer_operation_pkey PRIMARY KEY (id);


--
-- TOC entry 3524 (class 2606 OID 16461)
-- Name: transition transition_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition
    ADD CONSTRAINT transition_pk PRIMARY KEY (id);


--
-- TOC entry 3515 (class 1259 OID 16452)
-- Name: block_block_hash_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX block_block_hash_uindex ON explorer.block USING btree (block_hash);


--
-- TOC entry 3516 (class 1259 OID 16449)
-- Name: block_height_block_hash_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX block_height_block_hash_uindex ON explorer.block USING btree (height, block_hash);


--
-- TOC entry 3533 (class 1259 OID 16491)
-- Name: ciphertext_transition_id_index_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX ciphertext_transition_id_index_uindex ON explorer.ciphertext USING btree (transition_id, index);


--
-- TOC entry 3546 (class 1259 OID 17106)
-- Name: coinbase_operation_operation_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX coinbase_operation_operation_event_id_uindex ON explorer.coinbase_operation USING btree (operation_event_id);


--
-- TOC entry 3537 (class 1259 OID 17032)
-- Name: custom_event_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX custom_event_event_id_uindex ON explorer.custom_event USING btree (event_id);


--
-- TOC entry 3552 (class 1259 OID 17156)
-- Name: evaluate_operation_operation_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX evaluate_operation_operation_event_id_uindex ON explorer.evaluate_operation USING btree (operation_event_id);


--
-- TOC entry 3536 (class 1259 OID 16514)
-- Name: event_transition_id_index_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX event_transition_id_index_uindex ON explorer.event USING btree (transition_id, index);


--
-- TOC entry 3543 (class 1259 OID 17074)
-- Name: operation_event_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX operation_event_event_id_uindex ON explorer.operation_event USING btree (event_id);


--
-- TOC entry 3555 (class 1259 OID 17189)
-- Name: record_output_transition_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX record_output_transition_id_index ON explorer.record USING btree (output_transition_id);


--
-- TOC entry 3558 (class 1259 OID 17190)
-- Name: record_record_view_key_event_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX record_record_view_key_event_id_index ON explorer.record USING btree (record_view_key_event_id);


--
-- TOC entry 3540 (class 1259 OID 17047)
-- Name: record_view_key_event_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX record_view_key_event_event_id_uindex ON explorer.record_view_key_event USING btree (event_id);


--
-- TOC entry 3529 (class 1259 OID 16475)
-- Name: serial_number_serial_number_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX serial_number_serial_number_uindex ON explorer.serial_number USING btree (serial_number);


--
-- TOC entry 3530 (class 1259 OID 16476)
-- Name: serial_number_transition_id_index_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX serial_number_transition_id_index_uindex ON explorer.serial_number USING btree (transition_id, index);


--
-- TOC entry 3519 (class 1259 OID 17346)
-- Name: transaction_block_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transaction_block_id_index ON explorer.transaction USING btree (block_id);


--
-- TOC entry 3522 (class 1259 OID 16453)
-- Name: transaction_transaction_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX transaction_transaction_id_uindex ON explorer.transaction USING btree (transaction_id);


--
-- TOC entry 3549 (class 1259 OID 17121)
-- Name: transfer_operation_operation_event_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX transfer_operation_operation_event_id_uindex ON explorer.transfer_operation USING btree (operation_event_id);


--
-- TOC entry 3525 (class 1259 OID 17345)
-- Name: transition_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_transaction_id_index ON explorer.transition USING btree (transaction_id);


--
-- TOC entry 3526 (class 1259 OID 16459)
-- Name: transition_transition_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX transition_transition_id_uindex ON explorer.transition USING btree (transition_id);


--
-- TOC entry 3562 (class 2606 OID 16494)
-- Name: ciphertext ciphertext_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ciphertext
    ADD CONSTRAINT ciphertext_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition (id);


--
-- TOC entry 3567 (class 2606 OID 17107)
-- Name: coinbase_operation coinbase_operation_operation_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_operation
    ADD CONSTRAINT coinbase_operation_operation_event_id_fk FOREIGN KEY (operation_event_id) REFERENCES explorer.operation_event (id);


--
-- TOC entry 3564 (class 2606 OID 17027)
-- Name: custom_event custom_event_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.custom_event
    ADD CONSTRAINT custom_event_event_id_fk FOREIGN KEY (event_id) REFERENCES explorer.event (id);


--
-- TOC entry 3569 (class 2606 OID 17157)
-- Name: evaluate_operation evaluate_operation_operation_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.evaluate_operation
    ADD CONSTRAINT evaluate_operation_operation_event_id_fk FOREIGN KEY (operation_event_id) REFERENCES explorer.operation_event (id);


--
-- TOC entry 3563 (class 2606 OID 16517)
-- Name: event event_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.event
    ADD CONSTRAINT event_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition (id);


--
-- TOC entry 3566 (class 2606 OID 17075)
-- Name: operation_event operation_event_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.operation_event
    ADD CONSTRAINT operation_event_event_id_fk FOREIGN KEY (event_id) REFERENCES explorer.event (id);


--
-- TOC entry 3570 (class 2606 OID 17174)
-- Name: record record_ciphertext_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_ciphertext_id_fk FOREIGN KEY (ciphertext_id) REFERENCES explorer.ciphertext (id);


--
-- TOC entry 3571 (class 2606 OID 17179)
-- Name: record record_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_event_id_fk FOREIGN KEY (record_view_key_event_id) REFERENCES explorer.event (id);


--
-- TOC entry 3572 (class 2606 OID 17184)
-- Name: record record_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record
    ADD CONSTRAINT record_transition_id_fk FOREIGN KEY (output_transition_id) REFERENCES explorer.transition (id);


--
-- TOC entry 3565 (class 2606 OID 17042)
-- Name: record_view_key_event record_view_key_event_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.record_view_key_event
    ADD CONSTRAINT record_view_key_event_event_id_fk FOREIGN KEY (event_id) REFERENCES explorer.event (id);


--
-- TOC entry 3561 (class 2606 OID 16479)
-- Name: serial_number serial_number_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.serial_number
    ADD CONSTRAINT serial_number_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition (id);


--
-- TOC entry 3559 (class 2606 OID 16454)
-- Name: transaction transaction_block_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ADD CONSTRAINT transaction_block_id_fk FOREIGN KEY (block_id) REFERENCES explorer.block (id);


--
-- TOC entry 3568 (class 2606 OID 17122)
-- Name: transfer_operation transfer_operation_operation_event_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transfer_operation
    ADD CONSTRAINT transfer_operation_operation_event_id_fk FOREIGN KEY (operation_event_id) REFERENCES explorer.operation_event (id);


--
-- TOC entry 3560 (class 2606 OID 16462)
-- Name: transition transition_transaction_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition
    ADD CONSTRAINT transition_transaction_id_fk FOREIGN KEY (transaction_id) REFERENCES explorer.transaction (id);


-- Completed on 2022-06-16 02:15:13 JST

--
-- PostgreSQL database dump complete
--

