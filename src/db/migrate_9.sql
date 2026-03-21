CREATE OR REPLACE FUNCTION get_confirmed_transactions(block_db_id integer)
RETURNS TABLE(confirmed_transaction_id integer, confirmed_transaction_type confirmed_transaction_type,
              index integer, reject_reason text, transaction_id text, transaction_type transaction_type,
              transaction_deploy_id integer, edition integer, verifying_keys bytea, program_id text,
              owner text, transaction_execute_id integer, global_state_root text, proof text,
              fee_id integer, fee_global_state_root text, fee_proof text)
LANGUAGE plpgsql AS $$
declare
    transaction_db_id transaction.id%type;
    confirmed_transaction_db_id confirmed_transaction.id%type;
begin
    for confirmed_transaction_db_id, confirmed_transaction_type, index, reject_reason in
        select t.id, t.type, t.index, t.reject_reason from confirmed_transaction t where t.block_id = block_db_id
        loop
            confirmed_transaction_id := confirmed_transaction_db_id;
            select t.id, t.transaction_id, t.type from transaction t where t.confirmed_transaction_id = confirmed_transaction_db_id into transaction_db_id, transaction_id, transaction_type;
            if confirmed_transaction_type = 'AcceptedDeploy' or confirmed_transaction_type = 'RejectedDeploy' then
                if confirmed_transaction_type = 'RejectedDeploy' then
                    select t.id, t.edition, t.verifying_keys, t.program_id, t.owner from transaction_deploy t where t.transaction_id = transaction_db_id order by id limit 1 into transaction_deploy_id, edition, verifying_keys, program_id, owner;
                else
                    select t.id, t.edition, t.verifying_keys, t.program_id from transaction_deploy t where t.transaction_id = transaction_db_id into transaction_deploy_id, edition, verifying_keys, program_id;
                end if;
                select t.id, t.global_state_root, t.proof from fee t where t.transaction_id = transaction_db_id order by id limit 1 into fee_id, fee_global_state_root, fee_proof;
                return next;
            elsif confirmed_transaction_type = 'AcceptedExecute' or confirmed_transaction_type = 'RejectedExecute' then
                select t.id, t.global_state_root, t.proof from transaction_execute t where t.transaction_id = transaction_db_id order by id limit 1 into transaction_execute_id, global_state_root, proof;
                select t.id, t.global_state_root, t.proof from fee t where t.transaction_id = transaction_db_id order by id limit 1 into fee_id, fee_global_state_root, fee_proof;
                return next;
            end if;
        end loop;
end;
$$;
