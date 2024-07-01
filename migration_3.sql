create or replace function get_finalize_operations(confirmed_transaction_db_id integer)
    returns TABLE(type finalize_operation_type, index integer, mapping_id text, key_id text, value_id text)
    language plpgsql
as
$$
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
