--
-- PostgreSQL database dump
--

-- Dumped from database version 15.1
-- Dumped by pg_dump version 15.3

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
-- Name: argument_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.argument_type AS ENUM (
    'Plaintext',
    'Future'
);


--
-- Name: authority_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.authority_type AS ENUM (
    'Beacon',
    'Quorum'
);


--
-- Name: confirmed_transaction_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.confirmed_transaction_type AS ENUM (
    'AcceptedDeploy',
    'AcceptedExecute',
    'RejectedDeploy',
    'RejectedExecute'
);


--
-- Name: finalize_operation_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.finalize_operation_type AS ENUM (
    'InitializeMapping',
    'InsertKeyValue',
    'UpdateKeyValue',
    'RemoveKeyValue',
    'RemoveMapping'
);


--
-- Name: future_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.future_type AS ENUM (
    'Output',
    'Argument'
);


--
-- Name: ratification_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.ratification_type AS ENUM (
    'Genesis',
    'BlockReward',
    'PuzzleReward'
);


--
-- Name: transaction_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.transaction_type AS ENUM (
    'Deploy',
    'Execute',
    'Fee'
);


--
-- Name: transition_data_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.transition_data_type AS ENUM (
    'Constant',
    'Public',
    'Private',
    'Record',
    'ExternalRecord',
    'Future'
);


--
-- Name: transmission_id_type; Type: TYPE; Schema: explorer; Owner: -
--

CREATE TYPE explorer.transmission_id_type AS ENUM (
    'Ratification',
    'Solution',
    'Transaction'
);


--
-- Name: get_block_target_sum(bigint); Type: FUNCTION; Schema: explorer; Owner: -
--

CREATE FUNCTION explorer.get_block_target_sum(block_height bigint) RETURNS numeric
    LANGUAGE sql STABLE
    AS $$
SELECT SUM(target) FROM explorer.partial_solution ps
JOIN explorer.coinbase_solution cs ON cs.id = ps.coinbase_solution_id
JOIN explorer.block b ON b.id = cs.block_id
WHERE height = block_height
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _migration; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer._migration (
    migrated_id integer NOT NULL
);


--
-- Name: authority; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.authority (
    id integer NOT NULL,
    block_id integer NOT NULL,
    type explorer.authority_type NOT NULL,
    signature text
);


--
-- Name: authority_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.authority_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: authority_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.authority_id_seq OWNED BY explorer.authority.id;


--
-- Name: block; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.block (
    id integer NOT NULL,
    height bigint NOT NULL,
    block_hash text NOT NULL,
    previous_hash text NOT NULL,
    previous_state_root text NOT NULL,
    transactions_root text NOT NULL,
    finalize_root text NOT NULL,
    ratifications_root text NOT NULL,
    solutions_root text NOT NULL,
    subdag_root text NOT NULL,
    round numeric(20,0) NOT NULL,
    cumulative_weight numeric(40,0) NOT NULL,
    cumulative_proof_target numeric(40,0) NOT NULL,
    coinbase_target numeric(20,0) NOT NULL,
    proof_target numeric(20,0) NOT NULL,
    last_coinbase_target numeric(20,0) NOT NULL,
    last_coinbase_timestamp bigint NOT NULL,
    "timestamp" bigint NOT NULL,
    block_reward numeric(20,0) NOT NULL,
    coinbase_reward numeric(20,0) NOT NULL
);


--
-- Name: block_aborted_transaction_id; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.block_aborted_transaction_id (
    id integer NOT NULL,
    block_id integer NOT NULL,
    transaction_id text NOT NULL
);


--
-- Name: block_aborted_transaction_id_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.block_aborted_transaction_id_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: block_aborted_transaction_id_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.block_aborted_transaction_id_id_seq OWNED BY explorer.block_aborted_transaction_id.id;


--
-- Name: block_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.block_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: block_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.block_id_seq OWNED BY explorer.block.id;


--
-- Name: coinbase_solution; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.coinbase_solution (
    id integer NOT NULL,
    block_id integer NOT NULL,
    target_sum numeric(20,0) DEFAULT 0 NOT NULL
);


--
-- Name: coinbase_solution_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.coinbase_solution_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: coinbase_solution_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.coinbase_solution_id_seq OWNED BY explorer.coinbase_solution.id;


--
-- Name: committee_history; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.committee_history (
    id integer NOT NULL,
    height bigint NOT NULL,
    starting_round numeric(20,0) NOT NULL,
    total_stake numeric(20,0) NOT NULL
);


--
-- Name: committee_history_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.committee_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: committee_history_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.committee_history_id_seq OWNED BY explorer.committee_history.id;


--
-- Name: committee_history_member; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.committee_history_member (
    id integer NOT NULL,
    committee_id integer NOT NULL,
    address text NOT NULL,
    stake numeric(20,0) NOT NULL,
    is_open boolean NOT NULL
);


--
-- Name: committee_history_member_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.committee_history_member_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: committee_history_member_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.committee_history_member_id_seq OWNED BY explorer.committee_history_member.id;


--
-- Name: confirmed_transaction; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.confirmed_transaction (
    id integer NOT NULL,
    block_id integer,
    index integer NOT NULL,
    type explorer.confirmed_transaction_type NOT NULL,
    reject_reason text
);


--
-- Name: confirmed_transaction_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.confirmed_transaction_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: confirmed_transaction_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.confirmed_transaction_id_seq OWNED BY explorer.confirmed_transaction.id;


--
-- Name: dag_vertex; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.dag_vertex (
    id bigint NOT NULL,
    authority_id integer NOT NULL,
    round numeric(20,0) NOT NULL,
    batch_certificate_id text NOT NULL,
    batch_id text NOT NULL,
    author text NOT NULL,
    "timestamp" bigint NOT NULL,
    author_signature text NOT NULL,
    index integer NOT NULL
);


--
-- Name: dag_vertex_adjacency; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.dag_vertex_adjacency (
    id bigint NOT NULL,
    vertex_id bigint NOT NULL,
    previous_vertex_id bigint NOT NULL,
    index integer NOT NULL
);


--
-- Name: dag_vertex_adjacency_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.dag_vertex_adjacency_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dag_vertex_adjacency_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.dag_vertex_adjacency_id_seq OWNED BY explorer.dag_vertex_adjacency.id;


--
-- Name: dag_vertex_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.dag_vertex_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dag_vertex_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.dag_vertex_id_seq OWNED BY explorer.dag_vertex.id;


--
-- Name: dag_vertex_signature; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.dag_vertex_signature (
    id bigint NOT NULL,
    vertex_id bigint NOT NULL,
    signature text NOT NULL,
    "timestamp" bigint NOT NULL,
    index integer NOT NULL
);


--
-- Name: dag_vertex_signature_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.dag_vertex_signature_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dag_vertex_signature_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.dag_vertex_signature_id_seq OWNED BY explorer.dag_vertex_signature.id;


--
-- Name: dag_vertex_transmission_id; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.dag_vertex_transmission_id (
    id bigint NOT NULL,
    vertex_id bigint NOT NULL,
    type explorer.transmission_id_type NOT NULL,
    index integer NOT NULL,
    commitment text,
    transaction_id text
);


--
-- Name: dag_vertex_transmission_id_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.dag_vertex_transmission_id_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dag_vertex_transmission_id_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.dag_vertex_transmission_id_id_seq OWNED BY explorer.dag_vertex_transmission_id.id;


--
-- Name: transaction_execute; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transaction_execute (
    id integer NOT NULL,
    transaction_id integer NOT NULL,
    global_state_root text NOT NULL,
    proof text
);


--
-- Name: execute_transaction_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.execute_transaction_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: execute_transaction_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.execute_transaction_id_seq OWNED BY explorer.transaction_execute.id;


--
-- Name: fee; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.fee (
    id integer NOT NULL,
    transaction_id integer NOT NULL,
    global_state_root text NOT NULL,
    proof text
);


--
-- Name: fee_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.fee_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fee_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.fee_id_seq OWNED BY explorer.fee.id;


--
-- Name: feedback; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.feedback (
    id integer NOT NULL,
    contact text NOT NULL,
    content text NOT NULL
);


--
-- Name: feedback_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.feedback_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: feedback_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.feedback_id_seq OWNED BY explorer.feedback.id;


--
-- Name: finalize_operation; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.finalize_operation (
    id integer NOT NULL,
    confirmed_transaction_id integer NOT NULL,
    type explorer.finalize_operation_type NOT NULL,
    index integer NOT NULL
);


--
-- Name: finalize_operation_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.finalize_operation_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finalize_operation_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.finalize_operation_id_seq OWNED BY explorer.finalize_operation.id;


--
-- Name: finalize_operation_initialize_mapping; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.finalize_operation_initialize_mapping (
    id integer NOT NULL,
    finalize_operation_id integer NOT NULL,
    mapping_id text NOT NULL
);


--
-- Name: finalize_operation_initialize_mapping_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.finalize_operation_initialize_mapping_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finalize_operation_initialize_mapping_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.finalize_operation_initialize_mapping_id_seq OWNED BY explorer.finalize_operation_initialize_mapping.id;


--
-- Name: finalize_operation_insert_kv; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.finalize_operation_insert_kv (
    id integer NOT NULL,
    finalize_operation_id integer NOT NULL,
    mapping_id text NOT NULL,
    key_id text NOT NULL,
    value_id text NOT NULL
);


--
-- Name: finalize_operation_insert_kv_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.finalize_operation_insert_kv_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finalize_operation_insert_kv_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.finalize_operation_insert_kv_id_seq OWNED BY explorer.finalize_operation_insert_kv.id;


--
-- Name: finalize_operation_remove_kv; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.finalize_operation_remove_kv (
    id integer NOT NULL,
    finalize_operation_id integer NOT NULL,
    mapping_id text NOT NULL
);


--
-- Name: finalize_operation_remove_kv_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.finalize_operation_remove_kv_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finalize_operation_remove_kv_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.finalize_operation_remove_kv_id_seq OWNED BY explorer.finalize_operation_remove_kv.id;


--
-- Name: finalize_operation_remove_mapping; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.finalize_operation_remove_mapping (
    id integer NOT NULL,
    finalize_operation_id integer NOT NULL,
    mapping_id text NOT NULL
);


--
-- Name: finalize_operation_remove_mapping_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.finalize_operation_remove_mapping_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finalize_operation_remove_mapping_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.finalize_operation_remove_mapping_id_seq OWNED BY explorer.finalize_operation_remove_mapping.id;


--
-- Name: finalize_operation_update_kv; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.finalize_operation_update_kv (
    id integer NOT NULL,
    finalize_operation_id integer NOT NULL,
    mapping_id text NOT NULL,
    key_id text NOT NULL,
    value_id text NOT NULL
);


--
-- Name: finalize_operation_update_kv_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.finalize_operation_update_kv_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: finalize_operation_update_kv_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.finalize_operation_update_kv_id_seq OWNED BY explorer.finalize_operation_update_kv.id;


--
-- Name: future; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.future (
    id integer NOT NULL,
    type explorer.future_type NOT NULL,
    transition_output_future_id integer,
    future_argument_id integer,
    program_id text NOT NULL,
    function_name text NOT NULL
);


--
-- Name: future_argument; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.future_argument (
    id integer NOT NULL,
    future_id integer NOT NULL,
    type explorer.argument_type NOT NULL,
    plaintext bytea
);


--
-- Name: future_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.future_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: future_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.future_id_seq OWNED BY explorer.future.id;


--
-- Name: leaderboard; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.leaderboard (
    address text NOT NULL,
    total_reward numeric(20,0) DEFAULT 0 NOT NULL,
    total_incentive numeric(20,0) DEFAULT 0 NOT NULL
);


--
-- Name: leaderboard_total; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.leaderboard_total (
    total_credit numeric(20,0) DEFAULT 0 NOT NULL
);


--
-- Name: mapping; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.mapping (
    id integer NOT NULL,
    mapping_id text NOT NULL,
    program_id text NOT NULL,
    mapping text NOT NULL
);


--
-- Name: mapping_bonded_history; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.mapping_bonded_history (
    id integer NOT NULL,
    height bigint NOT NULL,
    content jsonb NOT NULL
);


--
-- Name: mapping_bonded_history_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.mapping_bonded_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mapping_bonded_history_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.mapping_bonded_history_id_seq OWNED BY explorer.mapping_bonded_history.id;


--
-- Name: mapping_history; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.mapping_history (
    id bigint NOT NULL,
    mapping_id integer NOT NULL,
    height integer NOT NULL,
    key_id text NOT NULL,
    value bytea
);


--
-- Name: mapping_history_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.mapping_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mapping_history_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.mapping_history_id_seq OWNED BY explorer.mapping_history.id;


--
-- Name: mapping_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.mapping_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mapping_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.mapping_id_seq OWNED BY explorer.mapping.id;


--
-- Name: mapping_value; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.mapping_value (
    id integer NOT NULL,
    mapping_id integer NOT NULL,
    key_id text NOT NULL,
    value_id text NOT NULL,
    key bytea NOT NULL,
    value bytea NOT NULL
);


--
-- Name: mapping_value_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.mapping_value_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: mapping_value_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.mapping_value_id_seq OWNED BY explorer.mapping_value.id;


--
-- Name: prover_solution; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.prover_solution (
    id bigint NOT NULL,
    coinbase_solution_id integer NOT NULL,
    dag_vertex_id bigint NOT NULL,
    address text NOT NULL,
    nonce numeric(20,0) NOT NULL,
    commitment text NOT NULL,
    target numeric(20,0) NOT NULL,
    reward integer NOT NULL,
    proof_x text NOT NULL,
    proof_y_is_positive boolean NOT NULL
);


--
-- Name: partial_solution_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.partial_solution_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: partial_solution_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.partial_solution_id_seq OWNED BY explorer.prover_solution.id;


--
-- Name: transition_input_private; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_input_private (
    id integer NOT NULL,
    transition_input_id integer NOT NULL,
    ciphertext_hash text NOT NULL,
    ciphertext text
);


--
-- Name: private_transition_input_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.private_transition_input_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: private_transition_input_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.private_transition_input_id_seq OWNED BY explorer.transition_input_private.id;


--
-- Name: program; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.program (
    id integer NOT NULL,
    transaction_deploy_id integer,
    program_id text NOT NULL,
    import text[],
    mapping text[],
    interface text[],
    record text[],
    closure text[],
    function text[],
    raw_data bytea NOT NULL,
    is_helloworld boolean DEFAULT false NOT NULL,
    feature_hash bytea NOT NULL,
    owner text,
    signature text,
    leo_source text
);


--
-- Name: program_filter_hash; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.program_filter_hash (
    hash bytea NOT NULL
);


--
-- Name: program_function; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.program_function (
    id integer NOT NULL,
    program_id integer NOT NULL,
    name text NOT NULL,
    input text[] NOT NULL,
    input_mode text[] NOT NULL,
    output text[] NOT NULL,
    output_mode text[] NOT NULL,
    finalize text[] NOT NULL,
    called integer DEFAULT 0 NOT NULL
);


--
-- Name: program_function_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.program_function_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: program_function_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.program_function_id_seq OWNED BY explorer.program_function.id;


--
-- Name: program_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.program_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: program_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.program_id_seq OWNED BY explorer.program.id;


--
-- Name: ratification; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.ratification (
    id integer NOT NULL,
    block_id integer NOT NULL,
    index integer NOT NULL,
    type explorer.ratification_type NOT NULL,
    amount numeric(20,0)
);


--
-- Name: ratification_genesis_balance; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.ratification_genesis_balance (
    id integer NOT NULL,
    address text NOT NULL,
    amount numeric(20,0) NOT NULL
);


--
-- Name: ratification_genesis_balance_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.ratification_genesis_balance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ratification_genesis_balance_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.ratification_genesis_balance_id_seq OWNED BY explorer.ratification_genesis_balance.id;


--
-- Name: ratification_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.ratification_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ratification_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.ratification_id_seq OWNED BY explorer.ratification.id;


--
-- Name: transaction; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transaction (
    id integer NOT NULL,
    confimed_transaction_id integer NOT NULL,
    dag_vertex_id bigint,
    transaction_id text NOT NULL,
    type explorer.transaction_type NOT NULL
);


--
-- Name: transaction_deploy; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transaction_deploy (
    id integer NOT NULL,
    transaction_id integer NOT NULL,
    edition integer NOT NULL,
    verifying_keys bytea NOT NULL
);


--
-- Name: transaction_deployment_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transaction_deployment_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transaction_deployment_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transaction_deployment_id_seq OWNED BY explorer.transaction_deploy.id;


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
-- Name: transition; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition (
    id integer NOT NULL,
    transition_id text NOT NULL,
    transaction_execute_id integer,
    fee_id integer,
    program_id text NOT NULL,
    function_name text NOT NULL,
    tpk text NOT NULL,
    tcm text NOT NULL,
    index integer NOT NULL
);


--
-- Name: transition_finalize_future_argument_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_finalize_future_argument_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_finalize_future_argument_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_finalize_future_argument_id_seq OWNED BY explorer.future_argument.id;


--
-- Name: transition_output_future; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_output_future (
    id integer NOT NULL,
    transition_output_id integer NOT NULL,
    future_hash text NOT NULL
);


--
-- Name: transition_finalize_future_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_finalize_future_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_finalize_future_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_finalize_future_id_seq OWNED BY explorer.transition_output_future.id;


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
-- Name: transition_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_id_seq OWNED BY explorer.transition.id;


--
-- Name: transition_input; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_input (
    id integer NOT NULL,
    transition_id integer NOT NULL,
    type explorer.transition_data_type NOT NULL,
    index integer NOT NULL
);


--
-- Name: transition_input_external_record; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_input_external_record (
    id integer NOT NULL,
    transition_input_id integer NOT NULL,
    commitment text NOT NULL
);


--
-- Name: transition_input_external_record_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_input_external_record_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_input_external_record_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_input_external_record_id_seq OWNED BY explorer.transition_input_external_record.id;


--
-- Name: transition_input_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_input_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_input_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_input_id_seq OWNED BY explorer.transition_input.id;


--
-- Name: transition_input_public; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_input_public (
    id integer NOT NULL,
    transition_input_id integer NOT NULL,
    plaintext_hash text NOT NULL,
    plaintext bytea
);


--
-- Name: transition_input_public_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_input_public_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_input_public_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_input_public_id_seq OWNED BY explorer.transition_input_public.id;


--
-- Name: transition_input_record; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_input_record (
    id integer NOT NULL,
    transition_input_id integer NOT NULL,
    serial_number text NOT NULL,
    tag text NOT NULL
);


--
-- Name: transition_input_record_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_input_record_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_input_record_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_input_record_id_seq OWNED BY explorer.transition_input_record.id;


--
-- Name: transition_output; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_output (
    id integer NOT NULL,
    transition_id integer NOT NULL,
    type explorer.transition_data_type NOT NULL,
    index integer NOT NULL
);


--
-- Name: transition_output_external_record; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_output_external_record (
    id integer NOT NULL,
    transition_output_id integer NOT NULL,
    commitment text NOT NULL
);


--
-- Name: transition_output_external_record_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_output_external_record_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_output_external_record_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_output_external_record_id_seq OWNED BY explorer.transition_output_external_record.id;


--
-- Name: transition_output_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_output_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_output_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_output_id_seq OWNED BY explorer.transition_output.id;


--
-- Name: transition_output_private; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_output_private (
    id integer NOT NULL,
    transition_output_id integer NOT NULL,
    ciphertext_hash text NOT NULL,
    ciphertext text
);


--
-- Name: transition_output_private_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_output_private_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_output_private_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_output_private_id_seq OWNED BY explorer.transition_output_private.id;


--
-- Name: transition_output_public; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_output_public (
    id integer NOT NULL,
    transition_output_id integer NOT NULL,
    plaintext_hash text NOT NULL,
    plaintext bytea
);


--
-- Name: transition_output_public_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_output_public_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_output_public_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_output_public_id_seq OWNED BY explorer.transition_output_public.id;


--
-- Name: transition_output_record; Type: TABLE; Schema: explorer; Owner: -
--

CREATE TABLE explorer.transition_output_record (
    id integer NOT NULL,
    transition_output_id integer NOT NULL,
    commitment text NOT NULL,
    checksum text NOT NULL,
    record_ciphertext text
);


--
-- Name: transition_output_record_id_seq; Type: SEQUENCE; Schema: explorer; Owner: -
--

CREATE SEQUENCE explorer.transition_output_record_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transition_output_record_id_seq; Type: SEQUENCE OWNED BY; Schema: explorer; Owner: -
--

ALTER SEQUENCE explorer.transition_output_record_id_seq OWNED BY explorer.transition_output_record.id;


--
-- Name: authority id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.authority ALTER COLUMN id SET DEFAULT nextval('explorer.authority_id_seq'::regclass);


--
-- Name: block id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block ALTER COLUMN id SET DEFAULT nextval('explorer.block_id_seq'::regclass);


--
-- Name: block_aborted_transaction_id id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block_aborted_transaction_id ALTER COLUMN id SET DEFAULT nextval('explorer.block_aborted_transaction_id_id_seq'::regclass);


--
-- Name: coinbase_solution id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_solution ALTER COLUMN id SET DEFAULT nextval('explorer.coinbase_solution_id_seq'::regclass);


--
-- Name: committee_history id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.committee_history ALTER COLUMN id SET DEFAULT nextval('explorer.committee_history_id_seq'::regclass);


--
-- Name: committee_history_member id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.committee_history_member ALTER COLUMN id SET DEFAULT nextval('explorer.committee_history_member_id_seq'::regclass);


--
-- Name: confirmed_transaction id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.confirmed_transaction ALTER COLUMN id SET DEFAULT nextval('explorer.confirmed_transaction_id_seq'::regclass);


--
-- Name: dag_vertex id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex ALTER COLUMN id SET DEFAULT nextval('explorer.dag_vertex_id_seq'::regclass);


--
-- Name: dag_vertex_adjacency id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_adjacency ALTER COLUMN id SET DEFAULT nextval('explorer.dag_vertex_adjacency_id_seq'::regclass);


--
-- Name: dag_vertex_signature id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_signature ALTER COLUMN id SET DEFAULT nextval('explorer.dag_vertex_signature_id_seq'::regclass);


--
-- Name: dag_vertex_transmission_id id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_transmission_id ALTER COLUMN id SET DEFAULT nextval('explorer.dag_vertex_transmission_id_id_seq'::regclass);


--
-- Name: fee id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.fee ALTER COLUMN id SET DEFAULT nextval('explorer.fee_id_seq'::regclass);


--
-- Name: feedback id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.feedback ALTER COLUMN id SET DEFAULT nextval('explorer.feedback_id_seq'::regclass);


--
-- Name: finalize_operation id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation ALTER COLUMN id SET DEFAULT nextval('explorer.finalize_operation_id_seq'::regclass);


--
-- Name: finalize_operation_initialize_mapping id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_initialize_mapping ALTER COLUMN id SET DEFAULT nextval('explorer.finalize_operation_initialize_mapping_id_seq'::regclass);


--
-- Name: finalize_operation_insert_kv id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_insert_kv ALTER COLUMN id SET DEFAULT nextval('explorer.finalize_operation_insert_kv_id_seq'::regclass);


--
-- Name: finalize_operation_remove_kv id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_remove_kv ALTER COLUMN id SET DEFAULT nextval('explorer.finalize_operation_remove_kv_id_seq'::regclass);


--
-- Name: finalize_operation_remove_mapping id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_remove_mapping ALTER COLUMN id SET DEFAULT nextval('explorer.finalize_operation_remove_mapping_id_seq'::regclass);


--
-- Name: finalize_operation_update_kv id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_update_kv ALTER COLUMN id SET DEFAULT nextval('explorer.finalize_operation_update_kv_id_seq'::regclass);


--
-- Name: future id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.future ALTER COLUMN id SET DEFAULT nextval('explorer.future_id_seq'::regclass);


--
-- Name: future_argument id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.future_argument ALTER COLUMN id SET DEFAULT nextval('explorer.transition_finalize_future_argument_id_seq'::regclass);


--
-- Name: mapping id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping ALTER COLUMN id SET DEFAULT nextval('explorer.mapping_id_seq'::regclass);


--
-- Name: mapping_bonded_history id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_bonded_history ALTER COLUMN id SET DEFAULT nextval('explorer.mapping_bonded_history_id_seq'::regclass);


--
-- Name: mapping_history id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_history ALTER COLUMN id SET DEFAULT nextval('explorer.mapping_history_id_seq'::regclass);


--
-- Name: mapping_value id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_value ALTER COLUMN id SET DEFAULT nextval('explorer.mapping_value_id_seq'::regclass);


--
-- Name: program id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.program ALTER COLUMN id SET DEFAULT nextval('explorer.program_id_seq'::regclass);


--
-- Name: program_function id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.program_function ALTER COLUMN id SET DEFAULT nextval('explorer.program_function_id_seq'::regclass);


--
-- Name: prover_solution id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.prover_solution ALTER COLUMN id SET DEFAULT nextval('explorer.partial_solution_id_seq'::regclass);


--
-- Name: ratification id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ratification ALTER COLUMN id SET DEFAULT nextval('explorer.ratification_id_seq'::regclass);


--
-- Name: ratification_genesis_balance id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ratification_genesis_balance ALTER COLUMN id SET DEFAULT nextval('explorer.ratification_genesis_balance_id_seq'::regclass);


--
-- Name: transaction id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction ALTER COLUMN id SET DEFAULT nextval('explorer.transaction_id_seq'::regclass);


--
-- Name: transaction_deploy id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction_deploy ALTER COLUMN id SET DEFAULT nextval('explorer.transaction_deployment_id_seq'::regclass);


--
-- Name: transaction_execute id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction_execute ALTER COLUMN id SET DEFAULT nextval('explorer.execute_transaction_id_seq'::regclass);


--
-- Name: transition id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition ALTER COLUMN id SET DEFAULT nextval('explorer.transition_id_seq'::regclass);


--
-- Name: transition_input id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input ALTER COLUMN id SET DEFAULT nextval('explorer.transition_input_id_seq'::regclass);


--
-- Name: transition_input_external_record id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_external_record ALTER COLUMN id SET DEFAULT nextval('explorer.transition_input_external_record_id_seq'::regclass);


--
-- Name: transition_input_private id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_private ALTER COLUMN id SET DEFAULT nextval('explorer.private_transition_input_id_seq'::regclass);


--
-- Name: transition_input_public id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_public ALTER COLUMN id SET DEFAULT nextval('explorer.transition_input_public_id_seq'::regclass);


--
-- Name: transition_input_record id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_record ALTER COLUMN id SET DEFAULT nextval('explorer.transition_input_record_id_seq'::regclass);


--
-- Name: transition_output id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output ALTER COLUMN id SET DEFAULT nextval('explorer.transition_output_id_seq'::regclass);


--
-- Name: transition_output_external_record id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_external_record ALTER COLUMN id SET DEFAULT nextval('explorer.transition_output_external_record_id_seq'::regclass);


--
-- Name: transition_output_future id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_future ALTER COLUMN id SET DEFAULT nextval('explorer.transition_finalize_future_id_seq'::regclass);


--
-- Name: transition_output_private id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_private ALTER COLUMN id SET DEFAULT nextval('explorer.transition_output_private_id_seq'::regclass);


--
-- Name: transition_output_public id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_public ALTER COLUMN id SET DEFAULT nextval('explorer.transition_output_public_id_seq'::regclass);


--
-- Name: transition_output_record id; Type: DEFAULT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_record ALTER COLUMN id SET DEFAULT nextval('explorer.transition_output_record_id_seq'::regclass);


--
-- Name: authority authority_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.authority
    ADD CONSTRAINT authority_pk PRIMARY KEY (id);


--
-- Name: block_aborted_transaction_id block_aborted_transaction_id_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block_aborted_transaction_id
    ADD CONSTRAINT block_aborted_transaction_id_pk PRIMARY KEY (id);


--
-- Name: block block_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block
    ADD CONSTRAINT block_pk PRIMARY KEY (id);


--
-- Name: coinbase_solution coinbase_solution_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_solution
    ADD CONSTRAINT coinbase_solution_pk PRIMARY KEY (id);


--
-- Name: committee_history_member committee_history_member_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.committee_history_member
    ADD CONSTRAINT committee_history_member_pk PRIMARY KEY (id);


--
-- Name: committee_history committee_history_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.committee_history
    ADD CONSTRAINT committee_history_pk PRIMARY KEY (id);


--
-- Name: confirmed_transaction confirmed_transaction_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.confirmed_transaction
    ADD CONSTRAINT confirmed_transaction_pk PRIMARY KEY (id);


--
-- Name: dag_vertex_adjacency dag_vertex_adjacency_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_adjacency
    ADD CONSTRAINT dag_vertex_adjacency_pk PRIMARY KEY (id);


--
-- Name: dag_vertex dag_vertex_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex
    ADD CONSTRAINT dag_vertex_pk PRIMARY KEY (id);


--
-- Name: dag_vertex_signature dag_vertex_signature_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_signature
    ADD CONSTRAINT dag_vertex_signature_pk PRIMARY KEY (id);


--
-- Name: dag_vertex_transmission_id dag_vertex_transmission_id_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_transmission_id
    ADD CONSTRAINT dag_vertex_transmission_id_pk PRIMARY KEY (id);


--
-- Name: fee fee_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.fee
    ADD CONSTRAINT fee_pk PRIMARY KEY (id);


--
-- Name: feedback feedback_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.feedback
    ADD CONSTRAINT feedback_pkey PRIMARY KEY (id);


--
-- Name: finalize_operation_initialize_mapping finalize_operation_initialize_mapping_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_initialize_mapping
    ADD CONSTRAINT finalize_operation_initialize_mapping_pk PRIMARY KEY (id);


--
-- Name: finalize_operation_insert_kv finalize_operation_insert_kv_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_insert_kv
    ADD CONSTRAINT finalize_operation_insert_kv_pk PRIMARY KEY (id);


--
-- Name: finalize_operation finalize_operation_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation
    ADD CONSTRAINT finalize_operation_pk PRIMARY KEY (id);


--
-- Name: finalize_operation_remove_kv finalize_operation_remove_kv_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_remove_kv
    ADD CONSTRAINT finalize_operation_remove_kv_pk PRIMARY KEY (id);


--
-- Name: finalize_operation_remove_mapping finalize_operation_remove_mapping_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_remove_mapping
    ADD CONSTRAINT finalize_operation_remove_mapping_pk PRIMARY KEY (id);


--
-- Name: finalize_operation_update_kv finalize_operation_update_kv_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_update_kv
    ADD CONSTRAINT finalize_operation_update_kv_pk PRIMARY KEY (id);


--
-- Name: future future_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.future
    ADD CONSTRAINT future_pk PRIMARY KEY (id);


--
-- Name: leaderboard leaderboard_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.leaderboard
    ADD CONSTRAINT leaderboard_pk PRIMARY KEY (address);


--
-- Name: mapping_bonded_history mapping_bonded_history_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_bonded_history
    ADD CONSTRAINT mapping_bonded_history_pk PRIMARY KEY (id);


--
-- Name: mapping_history mapping_history_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_history
    ADD CONSTRAINT mapping_history_pk PRIMARY KEY (id);


--
-- Name: mapping mapping_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping
    ADD CONSTRAINT mapping_pk PRIMARY KEY (id);


--
-- Name: mapping mapping_pk2; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping
    ADD CONSTRAINT mapping_pk2 UNIQUE (mapping_id);


--
-- Name: mapping mapping_pk3; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping
    ADD CONSTRAINT mapping_pk3 UNIQUE (program_id, mapping);


--
-- Name: mapping_value mapping_value_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_value
    ADD CONSTRAINT mapping_value_pk PRIMARY KEY (id);


--
-- Name: mapping_value mapping_value_pk2; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_value
    ADD CONSTRAINT mapping_value_pk2 UNIQUE (mapping_id, key_id);


--
-- Name: prover_solution partial_solution_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.prover_solution
    ADD CONSTRAINT partial_solution_pk PRIMARY KEY (id);


--
-- Name: program_function program_function_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.program_function
    ADD CONSTRAINT program_function_pk PRIMARY KEY (id);


--
-- Name: program program_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.program
    ADD CONSTRAINT program_pk PRIMARY KEY (id);


--
-- Name: program program_pk2; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.program
    ADD CONSTRAINT program_pk2 UNIQUE (program_id);


--
-- Name: ratification ratification_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ratification
    ADD CONSTRAINT ratification_pk PRIMARY KEY (id);


--
-- Name: transaction_deploy transaction_deployment_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction_deploy
    ADD CONSTRAINT transaction_deployment_pk PRIMARY KEY (id);


--
-- Name: transaction_execute transaction_execute_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction_execute
    ADD CONSTRAINT transaction_execute_pk PRIMARY KEY (id);


--
-- Name: transaction transaction_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ADD CONSTRAINT transaction_pk PRIMARY KEY (id);


--
-- Name: future_argument transition_finalize_future_argument_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.future_argument
    ADD CONSTRAINT transition_finalize_future_argument_pk PRIMARY KEY (id);


--
-- Name: transition_input_external_record transition_input_external_record_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_external_record
    ADD CONSTRAINT transition_input_external_record_pk PRIMARY KEY (id);


--
-- Name: transition_input transition_input_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input
    ADD CONSTRAINT transition_input_pk PRIMARY KEY (id);


--
-- Name: transition_input_private transition_input_private_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_private
    ADD CONSTRAINT transition_input_private_pk PRIMARY KEY (id);


--
-- Name: transition_input_public transition_input_public_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_public
    ADD CONSTRAINT transition_input_public_pk PRIMARY KEY (id);


--
-- Name: transition_input_record transition_input_record_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_record
    ADD CONSTRAINT transition_input_record_pk PRIMARY KEY (id);


--
-- Name: transition_output_external_record transition_output_external_record_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_external_record
    ADD CONSTRAINT transition_output_external_record_pk PRIMARY KEY (id);


--
-- Name: transition_output_future transition_output_future_id_uindex; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_future
    ADD CONSTRAINT transition_output_future_id_uindex PRIMARY KEY (id);


--
-- Name: transition_output transition_output_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output
    ADD CONSTRAINT transition_output_pk PRIMARY KEY (id);


--
-- Name: transition_output_private transition_output_private_pkey; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_private
    ADD CONSTRAINT transition_output_private_pkey PRIMARY KEY (id);


--
-- Name: transition_output_public transition_output_public_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_public
    ADD CONSTRAINT transition_output_public_pk PRIMARY KEY (id);


--
-- Name: transition_output_record transition_output_record_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_record
    ADD CONSTRAINT transition_output_record_pk PRIMARY KEY (id);


--
-- Name: transition transition_pk; Type: CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition
    ADD CONSTRAINT transition_pk PRIMARY KEY (id);


--
-- Name: authority_block_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX authority_block_id_index ON explorer.authority USING btree (block_id);


--
-- Name: authority_type_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX authority_type_index ON explorer.authority USING btree (type);


--
-- Name: block_aborted_transaction_id_block_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX block_aborted_transaction_id_block_id_index ON explorer.block_aborted_transaction_id USING btree (block_id);


--
-- Name: block_block_hash_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX block_block_hash_uindex ON explorer.block USING btree (block_hash text_pattern_ops);


--
-- Name: block_height_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX block_height_uindex ON explorer.block USING btree (height);


--
-- Name: block_timestamp_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX block_timestamp_index ON explorer.block USING btree ("timestamp");


--
-- Name: coinbase_solution_block_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX coinbase_solution_block_id_index ON explorer.coinbase_solution USING btree (block_id);


--
-- Name: committee_history_height_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX committee_history_height_index ON explorer.committee_history USING btree (height);


--
-- Name: committee_history_member_address_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX committee_history_member_address_index ON explorer.committee_history_member USING btree (address);


--
-- Name: committee_history_member_committee_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX committee_history_member_committee_id_index ON explorer.committee_history_member USING btree (committee_id);


--
-- Name: confirmed_transaction_block_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX confirmed_transaction_block_id_index ON explorer.confirmed_transaction USING btree (block_id);


--
-- Name: confirmed_transaction_type_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX confirmed_transaction_type_index ON explorer.confirmed_transaction USING btree (type);


--
-- Name: dag_vertex_adjacency_end_vertex_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX dag_vertex_adjacency_end_vertex_index ON explorer.dag_vertex_adjacency USING btree (previous_vertex_id);


--
-- Name: dag_vertex_adjacency_index_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX dag_vertex_adjacency_index_index ON explorer.dag_vertex_adjacency USING btree (index);


--
-- Name: dag_vertex_author_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX dag_vertex_author_index ON explorer.dag_vertex USING btree (author);


--
-- Name: dag_vertex_authority_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX dag_vertex_authority_id_index ON explorer.dag_vertex USING btree (authority_id);


--
-- Name: dag_vertex_batch_certificate_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX dag_vertex_batch_certificate_id_uindex ON explorer.dag_vertex USING btree (batch_certificate_id);


--
-- Name: dag_vertex_round_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX dag_vertex_round_index ON explorer.dag_vertex USING btree (round);


--
-- Name: dag_vertex_signature_vertex_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX dag_vertex_signature_vertex_id_index ON explorer.dag_vertex_signature USING btree (vertex_id);


--
-- Name: dag_vertex_transmission_id_vertex_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX dag_vertex_transmission_id_vertex_id_index ON explorer.dag_vertex_transmission_id USING btree (vertex_id);


--
-- Name: fee_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX fee_transaction_id_index ON explorer.fee USING btree (transaction_id);


--
-- Name: finalize_operation_confirmed_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_confirmed_transaction_id_index ON explorer.finalize_operation USING btree (confirmed_transaction_id);


--
-- Name: finalize_operation_initialize_mapping_finalize_operation_id_ind; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_initialize_mapping_finalize_operation_id_ind ON explorer.finalize_operation_initialize_mapping USING btree (finalize_operation_id);


--
-- Name: finalize_operation_initialize_mapping_mapping_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_initialize_mapping_mapping_id_index ON explorer.finalize_operation_initialize_mapping USING btree (mapping_id);


--
-- Name: finalize_operation_insert_kv_finalize_operation_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_insert_kv_finalize_operation_id_index ON explorer.finalize_operation_insert_kv USING btree (finalize_operation_id);


--
-- Name: finalize_operation_insert_kv_mapping_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_insert_kv_mapping_id_index ON explorer.finalize_operation_insert_kv USING btree (mapping_id);


--
-- Name: finalize_operation_remove_kv_finalize_operation_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_remove_kv_finalize_operation_id_index ON explorer.finalize_operation_remove_kv USING btree (finalize_operation_id);


--
-- Name: finalize_operation_remove_kv_mapping_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_remove_kv_mapping_id_index ON explorer.finalize_operation_remove_kv USING btree (mapping_id);


--
-- Name: finalize_operation_remove_mapping_finalize_operation_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_remove_mapping_finalize_operation_id_index ON explorer.finalize_operation_remove_mapping USING btree (finalize_operation_id);


--
-- Name: finalize_operation_remove_mapping_mapping_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_remove_mapping_mapping_id_index ON explorer.finalize_operation_remove_mapping USING btree (mapping_id);


--
-- Name: finalize_operation_type_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_type_index ON explorer.finalize_operation USING btree (type);


--
-- Name: finalize_operation_update_kv_finalize_operation_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_update_kv_finalize_operation_id_index ON explorer.finalize_operation_update_kv USING btree (finalize_operation_id);


--
-- Name: finalize_operation_update_kv_mapping_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX finalize_operation_update_kv_mapping_id_index ON explorer.finalize_operation_update_kv USING btree (mapping_id);


--
-- Name: future_argument_future_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX future_argument_future_id_index ON explorer.future_argument USING btree (future_id);


--
-- Name: future_future_argument_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX future_future_argument_id_index ON explorer.future USING btree (future_argument_id);


--
-- Name: future_program_id_function_name_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX future_program_id_function_name_index ON explorer.future USING btree (program_id, function_name);


--
-- Name: future_transition_output_future_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX future_transition_output_future_id_index ON explorer.future USING btree (transition_output_future_id);


--
-- Name: future_type_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX future_type_index ON explorer.future USING btree (type);


--
-- Name: leaderboard_address_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX leaderboard_address_index ON explorer.leaderboard USING btree (address text_pattern_ops);


--
-- Name: leaderboard_total_incentive_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX leaderboard_total_incentive_index ON explorer.leaderboard USING btree (total_incentive);


--
-- Name: leaderboard_total_reward_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX leaderboard_total_reward_index ON explorer.leaderboard USING btree (total_reward);


--
-- Name: mapping_bonded_history_content_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX mapping_bonded_history_content_index ON explorer.mapping_bonded_history USING gin (content);


--
-- Name: mapping_bonded_history_height_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX mapping_bonded_history_height_index ON explorer.mapping_bonded_history USING btree (height);


--
-- Name: mapping_history_height_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX mapping_history_height_index ON explorer.mapping_history USING btree (height);


--
-- Name: mapping_history_key_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX mapping_history_key_id_index ON explorer.mapping_history USING btree (key_id);


--
-- Name: mapping_history_mapping_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX mapping_history_mapping_id_index ON explorer.mapping_history USING btree (mapping_id);


--
-- Name: mapping_value_key_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX mapping_value_key_id_index ON explorer.mapping_value USING btree (key_id);


--
-- Name: mapping_value_mapping_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX mapping_value_mapping_id_index ON explorer.mapping_value USING btree (mapping_id);


--
-- Name: partial_solution_address_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX partial_solution_address_index ON explorer.prover_solution USING btree (address text_pattern_ops);


--
-- Name: partial_solution_coinbase_solution_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX partial_solution_coinbase_solution_id_index ON explorer.prover_solution USING btree (coinbase_solution_id);


--
-- Name: partial_solution_dag_vertex_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX partial_solution_dag_vertex_id_index ON explorer.prover_solution USING btree (dag_vertex_id);


--
-- Name: program_feature_hash_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_feature_hash_index ON explorer.program USING btree (feature_hash);


--
-- Name: program_filter_hash_hash_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_filter_hash_hash_index ON explorer.program_filter_hash USING btree (hash);


--
-- Name: program_function_name_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_function_name_index ON explorer.program_function USING btree (name);


--
-- Name: program_function_program_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_function_program_id_index ON explorer.program_function USING btree (program_id);


--
-- Name: program_import_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_import_index ON explorer.program USING gin (import);


--
-- Name: program_is_helloworld_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_is_helloworld_index ON explorer.program USING btree (is_helloworld);


--
-- Name: program_owner_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_owner_index ON explorer.program USING btree (owner);


--
-- Name: program_transaction_deploy_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX program_transaction_deploy_id_index ON explorer.program USING btree (transaction_deploy_id);


--
-- Name: ratification_block_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX ratification_block_id_index ON explorer.ratification USING btree (block_id);


--
-- Name: ratification_type_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX ratification_type_index ON explorer.ratification USING btree (type);


--
-- Name: transaction_confimed_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transaction_confimed_transaction_id_index ON explorer.transaction USING btree (confimed_transaction_id);


--
-- Name: transaction_dag_vertex_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transaction_dag_vertex_id_index ON explorer.transaction USING btree (dag_vertex_id);


--
-- Name: transaction_deployment_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transaction_deployment_transaction_id_index ON explorer.transaction_deploy USING btree (transaction_id);


--
-- Name: transaction_execute_transaction_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transaction_execute_transaction_id_index ON explorer.transaction_execute USING btree (transaction_id);


--
-- Name: transaction_transaction_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX transaction_transaction_id_uindex ON explorer.transaction USING btree (transaction_id text_pattern_ops);


--
-- Name: transition_fee_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_fee_id_index ON explorer.transition USING btree (fee_id);


--
-- Name: transition_finalize_future_argument_type_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_finalize_future_argument_type_index ON explorer.future_argument USING btree (type);


--
-- Name: transition_function_name_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_function_name_index ON explorer.transition USING btree (function_name);


--
-- Name: transition_input_external_record_transition_input_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_input_external_record_transition_input_id_index ON explorer.transition_input_external_record USING btree (transition_input_id);


--
-- Name: transition_input_private_transition_input_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_input_private_transition_input_id_index ON explorer.transition_input_private USING btree (transition_input_id);


--
-- Name: transition_input_public_transition_input_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_input_public_transition_input_id_index ON explorer.transition_input_public USING btree (transition_input_id);


--
-- Name: transition_input_record_transition_input_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_input_record_transition_input_id_index ON explorer.transition_input_record USING btree (transition_input_id);


--
-- Name: transition_input_transition_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_input_transition_id_index ON explorer.transition_input USING btree (transition_id);


--
-- Name: transition_output_external_record_transition_output_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_output_external_record_transition_output_id_index ON explorer.transition_output_external_record USING btree (transition_output_id);


--
-- Name: transition_output_future_transition_output_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_output_future_transition_output_id_index ON explorer.transition_output_future USING btree (transition_output_id);


--
-- Name: transition_output_private_transition_output_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_output_private_transition_output_id_index ON explorer.transition_output_private USING btree (transition_output_id);


--
-- Name: transition_output_public_transition_output_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_output_public_transition_output_id_index ON explorer.transition_output_public USING btree (transition_output_id);


--
-- Name: transition_output_record_transition_output_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_output_record_transition_output_id_index ON explorer.transition_output_record USING btree (transition_output_id);


--
-- Name: transition_output_transition_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_output_transition_id_index ON explorer.transition_output USING btree (transition_id);


--
-- Name: transition_program_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_program_id_index ON explorer.transition USING btree (program_id);


--
-- Name: transition_transaction_execute_id_index; Type: INDEX; Schema: explorer; Owner: -
--

CREATE INDEX transition_transaction_execute_id_index ON explorer.transition USING btree (transaction_execute_id);


--
-- Name: transition_transition_id_uindex; Type: INDEX; Schema: explorer; Owner: -
--

CREATE UNIQUE INDEX transition_transition_id_uindex ON explorer.transition USING btree (transition_id text_pattern_ops);


--
-- Name: authority authority_block_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.authority
    ADD CONSTRAINT authority_block_id_fk FOREIGN KEY (block_id) REFERENCES explorer.block(id);


--
-- Name: block_aborted_transaction_id block_aborted_transaction_id_block_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.block_aborted_transaction_id
    ADD CONSTRAINT block_aborted_transaction_id_block_id_fk FOREIGN KEY (block_id) REFERENCES explorer.block(id);


--
-- Name: coinbase_solution coinbase_solution_block_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.coinbase_solution
    ADD CONSTRAINT coinbase_solution_block_id_fk FOREIGN KEY (block_id) REFERENCES explorer.block(id);


--
-- Name: committee_history_member committee_history_member_committee_history_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.committee_history_member
    ADD CONSTRAINT committee_history_member_committee_history_id_fk FOREIGN KEY (committee_id) REFERENCES explorer.committee_history(id);


--
-- Name: confirmed_transaction confirmed_transaction_block_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.confirmed_transaction
    ADD CONSTRAINT confirmed_transaction_block_id_fk FOREIGN KEY (block_id) REFERENCES explorer.block(id);


--
-- Name: dag_vertex_adjacency dag_vertex_adjacency_dag_vertex_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_adjacency
    ADD CONSTRAINT dag_vertex_adjacency_dag_vertex_id_fk FOREIGN KEY (vertex_id) REFERENCES explorer.dag_vertex(id);


--
-- Name: dag_vertex_adjacency dag_vertex_adjacency_dag_vertex_id_fk2; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_adjacency
    ADD CONSTRAINT dag_vertex_adjacency_dag_vertex_id_fk2 FOREIGN KEY (previous_vertex_id) REFERENCES explorer.dag_vertex(id);


--
-- Name: dag_vertex dag_vertex_authority_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex
    ADD CONSTRAINT dag_vertex_authority_id_fk FOREIGN KEY (authority_id) REFERENCES explorer.authority(id);


--
-- Name: dag_vertex_signature dag_vertex_signature_dag_vertex_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_signature
    ADD CONSTRAINT dag_vertex_signature_dag_vertex_id_fk FOREIGN KEY (vertex_id) REFERENCES explorer.dag_vertex(id);


--
-- Name: dag_vertex_transmission_id dag_vertex_transmission_id_dag_vertex_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.dag_vertex_transmission_id
    ADD CONSTRAINT dag_vertex_transmission_id_dag_vertex_id_fk FOREIGN KEY (vertex_id) REFERENCES explorer.dag_vertex(id);


--
-- Name: fee fee_transaction_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.fee
    ADD CONSTRAINT fee_transaction_id_fk FOREIGN KEY (transaction_id) REFERENCES explorer.transaction(id);


--
-- Name: finalize_operation finalize_operation_confirmed_transaction_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation
    ADD CONSTRAINT finalize_operation_confirmed_transaction_id_fk FOREIGN KEY (confirmed_transaction_id) REFERENCES explorer.confirmed_transaction(id);


--
-- Name: finalize_operation_initialize_mapping finalize_operation_initialize_mapping_finalize_operation_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_initialize_mapping
    ADD CONSTRAINT finalize_operation_initialize_mapping_finalize_operation_id_fk FOREIGN KEY (finalize_operation_id) REFERENCES explorer.finalize_operation(id);


--
-- Name: finalize_operation_insert_kv finalize_operation_insert_kv_finalize_operation_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_insert_kv
    ADD CONSTRAINT finalize_operation_insert_kv_finalize_operation_id_fk FOREIGN KEY (finalize_operation_id) REFERENCES explorer.finalize_operation(id);


--
-- Name: finalize_operation_remove_kv finalize_operation_remove_kv_finalize_operation_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_remove_kv
    ADD CONSTRAINT finalize_operation_remove_kv_finalize_operation_id_fk FOREIGN KEY (finalize_operation_id) REFERENCES explorer.finalize_operation(id);


--
-- Name: finalize_operation_remove_mapping finalize_operation_remove_mapping_finalize_operation_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_remove_mapping
    ADD CONSTRAINT finalize_operation_remove_mapping_finalize_operation_id_fk FOREIGN KEY (finalize_operation_id) REFERENCES explorer.finalize_operation(id);


--
-- Name: finalize_operation_update_kv finalize_operation_update_kv_finalize_operation_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.finalize_operation_update_kv
    ADD CONSTRAINT finalize_operation_update_kv_finalize_operation_id_fk FOREIGN KEY (finalize_operation_id) REFERENCES explorer.finalize_operation(id);


--
-- Name: future_argument future_argument_future_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.future_argument
    ADD CONSTRAINT future_argument_future_id_fk FOREIGN KEY (future_id) REFERENCES explorer.future(id);


--
-- Name: future future_future_argument_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.future
    ADD CONSTRAINT future_future_argument_id_fk FOREIGN KEY (future_argument_id) REFERENCES explorer.future_argument(id);


--
-- Name: future future_transition_output_future_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.future
    ADD CONSTRAINT future_transition_output_future_id_fk FOREIGN KEY (transition_output_future_id) REFERENCES explorer.transition_output_future(id);


--
-- Name: mapping_history mapping_history_mapping_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_history
    ADD CONSTRAINT mapping_history_mapping_id_fk FOREIGN KEY (mapping_id) REFERENCES explorer.mapping(id);


--
-- Name: mapping_value mapping_value_mapping_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.mapping_value
    ADD CONSTRAINT mapping_value_mapping_id_fk FOREIGN KEY (mapping_id) REFERENCES explorer.mapping(id);


--
-- Name: prover_solution partial_solution_coinbase_solution_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.prover_solution
    ADD CONSTRAINT partial_solution_coinbase_solution_id_fk FOREIGN KEY (coinbase_solution_id) REFERENCES explorer.coinbase_solution(id);


--
-- Name: prover_solution partial_solution_dag_vertex_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.prover_solution
    ADD CONSTRAINT partial_solution_dag_vertex_id_fk FOREIGN KEY (dag_vertex_id) REFERENCES explorer.dag_vertex(id);


--
-- Name: program_function program_function_program_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.program_function
    ADD CONSTRAINT program_function_program_id_fk FOREIGN KEY (program_id) REFERENCES explorer.program(id);


--
-- Name: program program_transaction_deployment_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.program
    ADD CONSTRAINT program_transaction_deployment_id_fk FOREIGN KEY (transaction_deploy_id) REFERENCES explorer.transaction_deploy(id);


--
-- Name: ratification ratification_block_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.ratification
    ADD CONSTRAINT ratification_block_id_fk FOREIGN KEY (block_id) REFERENCES explorer.block(id);


--
-- Name: transaction transaction_confirmed_transaction_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ADD CONSTRAINT transaction_confirmed_transaction_id_fk FOREIGN KEY (confimed_transaction_id) REFERENCES explorer.confirmed_transaction(id);


--
-- Name: transaction transaction_dag_vertex_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction
    ADD CONSTRAINT transaction_dag_vertex_id_fk FOREIGN KEY (dag_vertex_id) REFERENCES explorer.dag_vertex(id);


--
-- Name: transaction_deploy transaction_deployment_transaction_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction_deploy
    ADD CONSTRAINT transaction_deployment_transaction_id_fk FOREIGN KEY (transaction_id) REFERENCES explorer.transaction(id);


--
-- Name: transaction_execute transaction_execute_transaction_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transaction_execute
    ADD CONSTRAINT transaction_execute_transaction_id_fk FOREIGN KEY (transaction_id) REFERENCES explorer.transaction(id);


--
-- Name: transition transition_fee_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition
    ADD CONSTRAINT transition_fee_id_fk FOREIGN KEY (fee_id) REFERENCES explorer.fee(id);


--
-- Name: transition_input_external_record transition_input_external_record_transition_input_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_external_record
    ADD CONSTRAINT transition_input_external_record_transition_input_id_fk FOREIGN KEY (transition_input_id) REFERENCES explorer.transition_input(id);


--
-- Name: transition_input_private transition_input_private_transition_input_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_private
    ADD CONSTRAINT transition_input_private_transition_input_id_fk FOREIGN KEY (transition_input_id) REFERENCES explorer.transition_input(id);


--
-- Name: transition_input_public transition_input_public_transition_input_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_public
    ADD CONSTRAINT transition_input_public_transition_input_id_fk FOREIGN KEY (transition_input_id) REFERENCES explorer.transition_input(id);


--
-- Name: transition_input_record transition_input_record_transition_input_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input_record
    ADD CONSTRAINT transition_input_record_transition_input_id_fk FOREIGN KEY (transition_input_id) REFERENCES explorer.transition_input(id);


--
-- Name: transition_input transition_input_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_input
    ADD CONSTRAINT transition_input_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition(id);


--
-- Name: transition_output_external_record transition_output_external_record_transition_output_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_external_record
    ADD CONSTRAINT transition_output_external_record_transition_output_id_fk FOREIGN KEY (transition_output_id) REFERENCES explorer.transition_output(id);


--
-- Name: transition_output_future transition_output_future_transition_output_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_future
    ADD CONSTRAINT transition_output_future_transition_output_id_fk FOREIGN KEY (transition_output_id) REFERENCES explorer.transition_output(id);


--
-- Name: transition_output_private transition_output_private_transition_output_id_fkey; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_private
    ADD CONSTRAINT transition_output_private_transition_output_id_fkey FOREIGN KEY (transition_output_id) REFERENCES explorer.transition_output(id);


--
-- Name: transition_output_public transition_output_public_transition_output_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_public
    ADD CONSTRAINT transition_output_public_transition_output_id_fk FOREIGN KEY (transition_output_id) REFERENCES explorer.transition_output(id);


--
-- Name: transition_output_record transition_output_record_transition_output_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output_record
    ADD CONSTRAINT transition_output_record_transition_output_id_fk FOREIGN KEY (transition_output_id) REFERENCES explorer.transition_output(id);


--
-- Name: transition_output transition_output_transition_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition_output
    ADD CONSTRAINT transition_output_transition_id_fk FOREIGN KEY (transition_id) REFERENCES explorer.transition(id);


--
-- Name: transition transition_transaction_execute_id_fk; Type: FK CONSTRAINT; Schema: explorer; Owner: -
--

ALTER TABLE ONLY explorer.transition
    ADD CONSTRAINT transition_transaction_execute_id_fk FOREIGN KEY (transaction_execute_id) REFERENCES explorer.transaction_execute(id);


--
-- PostgreSQL database dump complete
--

