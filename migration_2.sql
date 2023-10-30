create function get_transition_inputs(transition_db_id transition_input.transition_id%type)
    returns table (
                      type transition_input.type%type,
                      index transition_input.index%type,
                      plaintext_hash transition_input_public.plaintext_hash%type,
                      plaintext transition_input_public.plaintext%type,
                      ciphertext_hash transition_input_private.ciphertext_hash%type,
                      ciphertext transition_input_private.ciphertext%type,
                      serial_number transition_input_record.serial_number%type,
                      tag transition_input_record.tag%type,
                      commitment transition_input_external_record.commitment%type
                  )
    language plpgsql
as $$
declare
    transition_input_db_id transition_input.id%type;
begin
    for transition_input_db_id, type, index in
        select id, t.type, t.index from transition_input t where transition_id = transition_db_id
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
            elsif type = 'External' then
                select t.commitment from transition_input_external_record t where transition_input_id = transition_input_db_id into commitment;
                return next;
            else
                raise exception 'unsupported transition input type: %', type;
            end if;
        end loop;
end;
$$;

create function get_transition_outputs(transition_db_id transition_output.transition_id%type)
    returns table (
                      type transition_output.type%type,
                      index transition_output.index%type,
                      plaintext_hash transition_output_public.plaintext_hash%type,
                      plaintext transition_output_public.plaintext%type,
                      ciphertext_hash transition_output_private.ciphertext_hash%type,
                      ciphertext transition_output_private.ciphertext%type,
                      record_commitment transition_output_record.commitment%type,
                      checksum transition_output_record.checksum%type,
                      record_ciphertext transition_output_record.record_ciphertext%type,
                      external_record_commitment transition_output_external_record.commitment%type,
                      future_id transition_output_future.id%type,
                      future_hash transition_output_future.future_hash%type
                  )
    language plpgsql
as $$
declare
    transition_output_db_id transition_output.id%type;
begin
    for transition_output_db_id, type, index in
        select id, t.type, t.index from transition_output t where transition_id = transition_db_id
        loop
            if type = 'Public' then
                select t.plaintext_hash, t.plaintext from transition_output_public t where id = transition_output_db_id into plaintext_hash, plaintext;
                return next;
            elsif type = 'Private' then
                select t.ciphertext_hash, t.ciphertext from transition_output_private t where id = transition_output_db_id into ciphertext_hash, ciphertext;
                return next;
            elsif type = 'Record' then
                select t.commitment, t.checksum, t.record_ciphertext from transition_output_record t where id = transition_output_db_id into record_commitment, checksum, record_ciphertext;
                return next;
            elsif type = 'ExternalRecord' then
                select t.commitment from transition_output_external_record t where id = transition_output_db_id into external_record_commitment;
                return next;
            elsif type = 'Future' then
                select t.id, t.future_hash from transition_output_future t where id = transition_output_db_id into future_id, future_hash;
                return next;
            else
                raise exception 'unsupported transition output type: %', type;
            end if;
        end loop;
end;
$$;

create function get_finalize_operations(confirmed_transaction_db_id confirmed_transaction.id%type)
    returns table (
                      type finalize_operation.type%type,
                      index finalize_operation.index%type,
                      mapping_id text,
                      key_id text,
                      value_id text
                  )
    language plpgsql
as $$
declare
    finalize_operation_db_id finalize_operation.id%type;
begin
    for finalize_operation_db_id, type, index in
        select t.id, t.type, t.index from finalize_operation t where t.confirmed_transaction_id = confirmed_transaction_db_id
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
                select t.mapping_id from finalize_operation_remove_kv t where t.finalize_operation_id = finalize_operation_db_id into mapping_id;
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

create function get_confirmed_transactions(block_db_id block.id%type)
    returns table (
                      confirmed_transaction_id confirmed_transaction.id%type,
                      confirmed_transaction_type confirmed_transaction.type%type,
                      index confirmed_transaction.index%type,
                      reject_reason confirmed_transaction.reject_reason%type,
                      dag_vertex_id transaction.dag_vertex_id%type,
                      transaction_id transaction.transaction_id%type,
                      transaction_type transaction.type%type,
                      edition transaction_deploy.edition%type,
                      verifying_keys transaction_deploy.verifying_keys%type,
                      transaction_execute_id transaction_execute.id%type,
                      global_state_root transaction_execute.global_state_root%type,
                      proof transaction_execute.proof%type,
                      fee_id fee.id%type,
                      fee_global_state_root fee.global_state_root%type,
                      fee_proof fee.proof%type
                  )
    language plpgsql
as $$
declare
    transaction_db_id transaction.id%type;
begin
    for confirmed_transaction_id, confirmed_transaction_type, index, reject_reason in
        select t.id, t.type, t.index, t.reject_reason from confirmed_transaction t where t.block_id = block_db_id
        loop
            select t.id, t.dag_vertex_id, t.transaction_id, t.type from transaction t where t.confimed_transaction_id = confirmed_transaction_id into transaction_db_id, dag_vertex_id, transaction_id, transaction_type;
            if confirmed_transaction_type = 'AcceptedDeploy' or confirmed_transaction_type = 'RejectedDeploy' then
                if confirmed_transaction_type = 'RejectedDeploy' then
                    raise exception 'rejected deploy not supported yet';
                end if;
                select t.edition, t.verifying_keys from transaction_deploy t where t.transaction_id = transaction_db_id into edition, verifying_keys;
                select t.id, t.global_state_root, t.proof from fee t where t.transaction_id = transaction_db_id into fee_id, fee_global_state_root, fee_proof;
                return next;
            elsif confirmed_transaction_type = 'AcceptedExecute' or confirmed_transaction_type = 'RejectedExecute' then
                select t.id, t.global_state_root, t.proof from transaction_execute t where t.transaction_id = transaction_db_id into transaction_execute_id, global_state_root, proof;
                select t.id, t.global_state_root, t.proof from fee t where t.transaction_id = transaction_db_id into fee_id, fee_global_state_root, fee_proof;
                return next;
            end if;
        end loop;
end;
$$