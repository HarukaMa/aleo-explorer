drop function get_confirmed_transactions(block.id%type);


create function get_confirmed_transactions(block_db_id block.id%type)
    returns table (
        confirmed_transaction_id confirmed_transaction.id%type,
        confirmed_transaction_type confirmed_transaction.type%type,
        index confirmed_transaction.index%type,
        reject_reason confirmed_transaction.reject_reason%type,
        dag_vertex_id transaction.dag_vertex_id%type,
        transaction_id transaction.transaction_id%type,
        transaction_type transaction.type%type,
        transaction_deploy_id transaction_deploy.id%type,
        edition transaction_deploy.edition%type,
        verifying_keys transaction_deploy.verifying_keys%type,
        program_id transaction_deploy.program_id%type,
        owner transaction_deploy.owner%type,
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
                    select t.id, t.edition, t.verifying_keys, t.program_id, t.owner from transaction_deploy t where t.transaction_id = transaction_db_id order by id limit 1 into transaction_deploy_id, edition, verifying_keys, program_id, owner;
                else
                    select t.id, t.edition, t.verifying_keys from transaction_deploy t where t.transaction_id = transaction_db_id into transaction_deploy_id, edition, verifying_keys;
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
$$