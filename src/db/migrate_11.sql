ALTER TYPE transition_data_type ADD VALUE 'DynamicRecord';
ALTER TYPE transition_data_type ADD VALUE 'RecordWithDynamicID';
ALTER TYPE transition_data_type ADD VALUE 'ExternalRecordWithDynamicID';

DROP FUNCTION get_transition_inputs(integer);
DROP FUNCTION get_transition_outputs(integer);

CREATE TABLE transition_input_dynamic_record (
    id serial PRIMARY KEY,
    transition_input_id integer NOT NULL REFERENCES transition_input(id),
    input_hash text NOT NULL
);

CREATE TABLE transition_input_record_with_dynamic_id (
    id serial PRIMARY KEY,
    transition_input_id integer NOT NULL REFERENCES transition_input(id),
    serial_number text NOT NULL,
    tag text NOT NULL,
    dynamic_id text NOT NULL
);

CREATE TABLE transition_input_external_record_with_dynamic_id (
    id serial PRIMARY KEY,
    transition_input_id integer NOT NULL REFERENCES transition_input(id),
    external_hash text NOT NULL,
    dynamic_id text NOT NULL
);

CREATE TABLE transition_output_dynamic_record (
    id serial PRIMARY KEY,
    transition_output_id integer NOT NULL REFERENCES transition_output(id),
    commitment text NOT NULL
);

CREATE TABLE transition_output_record_with_dynamic_id (
    id serial PRIMARY KEY,
    transition_output_id integer NOT NULL REFERENCES transition_output(id),
    commitment text NOT NULL,
    checksum text NOT NULL,
    record_ciphertext text,
    sender_ciphertext text,
    dynamic_id text NOT NULL
);

CREATE TABLE transition_output_external_record_with_dynamic_id (
    id serial PRIMARY KEY,
    transition_output_id integer NOT NULL REFERENCES transition_output(id),
    external_hash text NOT NULL,
    dynamic_id text NOT NULL
);

CREATE OR REPLACE FUNCTION get_transition_inputs(transition_db_id integer)
RETURNS TABLE(type transition_data_type, index integer, plaintext_hash text, plaintext bytea,
              ciphertext_hash text, ciphertext text, serial_number text, tag text, commitment text,
              input_hash text, dynamic_id text, external_hash text)
LANGUAGE plpgsql AS $$
declare
    transition_input_db_id transition_input.id%type;
begin
    for transition_input_db_id, type, index in
        select id, t.type, t.index from transition_input t where transition_id = transition_db_id order by id
        loop
            if type = 'Public' then
                select t.plaintext_hash, t.plaintext from transition_input_public t where transition_input_id = transition_input_db_id into plaintext_hash, plaintext;
                return next;
            elsif type = 'Private' then
                select t.ciphertext_hash, t.ciphertext from transition_input_private t where transition_input_id = transition_input_db_id into ciphertext_hash, ciphertext;
                return next;
            elsif type = 'Record' then
                select t.serial_number, t.tag from transition_input_record t where transition_input_id = transition_input_db_id into serial_number, tag;
                return next;
            elsif type = 'ExternalRecord' then
                select t.commitment from transition_input_external_record t where transition_input_id = transition_input_db_id into commitment;
                return next;
            elsif type = 'DynamicRecord' then
                select t.input_hash from transition_input_dynamic_record t where transition_input_id = transition_input_db_id into input_hash;
                return next;
            elsif type = 'RecordWithDynamicID' then
                select t.serial_number, t.tag, t.dynamic_id from transition_input_record_with_dynamic_id t where transition_input_id = transition_input_db_id into serial_number, tag, dynamic_id;
                return next;
            elsif type = 'ExternalRecordWithDynamicID' then
                select t.external_hash, t.dynamic_id from transition_input_external_record_with_dynamic_id t where transition_input_id = transition_input_db_id into external_hash, dynamic_id;
                return next;
            else
                raise exception 'unsupported transition input type: %', type;
            end if;
        end loop;
end;
$$;

CREATE OR REPLACE FUNCTION get_transition_outputs(transition_db_id integer)
RETURNS TABLE(type transition_data_type, index integer, plaintext_hash text, plaintext bytea,
              ciphertext_hash text, ciphertext text, record_commitment text, checksum text,
              record_ciphertext text, sender_ciphertext text, external_record_commitment text,
              future_id integer, future_hash text, dynamic_commitment text,
              dynamic_record_commitment text, dynamic_record_checksum text,
              dynamic_record_ciphertext text, dynamic_record_sender_ciphertext text,
              dynamic_record_dynamic_id text, external_dynamic_hash text,
              external_dynamic_id text)
LANGUAGE plpgsql AS $$
declare
    transition_output_db_id transition_output.id%type;
begin
    for transition_output_db_id, type, index in
        select id, t.type, t.index from transition_output t where transition_id = transition_db_id order by id
        loop
            if type = 'Public' then
                select t.plaintext_hash, t.plaintext from transition_output_public t where transition_output_id = transition_output_db_id into plaintext_hash, plaintext;
                return next;
            elsif type = 'Private' then
                select t.ciphertext_hash, t.ciphertext from transition_output_private t where transition_output_id = transition_output_db_id into ciphertext_hash, ciphertext;
                return next;
            elsif type = 'Record' then
                select t.commitment, t.checksum, t.record_ciphertext, t.sender_ciphertext from transition_output_record t where transition_output_id = transition_output_db_id into record_commitment, checksum, record_ciphertext, sender_ciphertext;
                return next;
            elsif type = 'ExternalRecord' then
                select t.commitment from transition_output_external_record t where transition_output_id = transition_output_db_id into external_record_commitment;
                return next;
            elsif type = 'Future' then
                select t.id, t.future_hash from transition_output_future t where transition_output_id = transition_output_db_id into future_id, future_hash;
                return next;
            elsif type = 'DynamicRecord' then
                select t.commitment from transition_output_dynamic_record t where transition_output_id = transition_output_db_id into dynamic_commitment;
                return next;
            elsif type = 'RecordWithDynamicID' then
                select t.commitment, t.checksum, t.record_ciphertext, t.sender_ciphertext, t.dynamic_id from transition_output_record_with_dynamic_id t where transition_output_id = transition_output_db_id into dynamic_record_commitment, dynamic_record_checksum, dynamic_record_ciphertext, dynamic_record_sender_ciphertext, dynamic_record_dynamic_id;
                return next;
            elsif type = 'ExternalRecordWithDynamicID' then
                select t.external_hash, t.dynamic_id from transition_output_external_record_with_dynamic_id t where transition_output_id = transition_output_db_id into external_dynamic_hash, external_dynamic_id;
                return next;
            else
                raise exception 'unsupported transition output type: %', type;
            end if;
        end loop;
end;
$$;
