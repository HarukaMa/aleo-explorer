create or replace function get_transition_inputs(transition_db_id integer)
    returns TABLE(type transition_data_type, index integer, plaintext_hash text, plaintext bytea, ciphertext_hash text, ciphertext text, serial_number text, tag text, commitment text)
    language plpgsql
as
$$
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
            else
                raise exception 'unsupported transition input type: %', type;
            end if;
        end loop;
end;
$$;

create or replace function get_finalize_operations(confirmed_transaction_db_id integer)
    returns TABLE(type finalize_operation_type, index integer, mapping_id text, key_id text, value_id text)
    language plpgsql
as
$$
declare
    finalize_operation_db_id finalize_operation.id%type;
begin
    for finalize_operation_db_id, type, index in
        select t.id, t.type, t.index from finalize_operation t where t.confirmed_transaction_id = confirmed_transaction_db_id order by id
        loop
            if type = 'InitializeMapping' then
                select t.mapping_id from finalize_operation_initialize_mapping t where t.finalize_operation_id = finalize_operation_db_id into mapping_id;
                return next;
            elsif type = 'InsertKeyValue' then
                select t.mapping_id, t.key_id, t.value_id from finalize_operation_insert_kv t where t.finalize_operation_id = finalize_operation_db_id into mapping_id, key_id, value_id;
                return next;
            elsif type = 'UpdateKeyValue' then
                select t.mapping_id, t.key_id, t.value_id from finalize_operation_update_kv t where t.finalize_operation_id = finalize_operation_db_id into mapping_id, key_id, value_id;
                return next;
            elsif type = 'RemoveKeyValue' then
                select t.mapping_id, t.key_id from finalize_operation_remove_kv t where t.finalize_operation_id = finalize_operation_db_id into mapping_id, key_id;
                return next;
            elsif type = 'RemoveMapping' then
                select t.mapping_id from finalize_operation_remove_mapping t where t.finalize_operation_id = finalize_operation_db_id into mapping_id;
                return next;
            else
                raise exception 'unsupported finalize operation type: %', type;
            end if;
        end loop;
end;
$$;

create or replace function explorer.get_transition_outputs(transition_db_id integer)
    returns TABLE(type explorer.transition_data_type, index integer, plaintext_hash text, plaintext bytea, ciphertext_hash text, ciphertext text, record_commitment text, checksum text, record_ciphertext text, external_record_commitment text, future_id integer, future_hash text)
    language plpgsql
as
$$
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
                select t.commitment, t.checksum, t.record_ciphertext from transition_output_record t where transition_output_id = transition_output_db_id into record_commitment, checksum, record_ciphertext;
                return next;
            elsif type = 'ExternalRecord' then
                select t.commitment from transition_output_external_record t where transition_output_id = transition_output_db_id into external_record_commitment;
                return next;
            elsif type = 'Future' then
                select t.id, t.future_hash from transition_output_future t where transition_output_id = transition_output_db_id into future_id, future_hash;
                return next;
            else
                raise exception 'unsupported transition output type: %', type;
            end if;
        end loop;
end;
$$;

