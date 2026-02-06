alter table transition_output_record
    add sender_ciphertext text;

drop function get_transition_outputs(integer);

create function get_transition_outputs(transition_db_id integer)
    returns TABLE(type transition_data_type, index integer, plaintext_hash text, plaintext bytea, ciphertext_hash text, ciphertext text, record_commitment text, checksum text, record_ciphertext text, sender_ciphertext text, external_record_commitment text, future_id integer, future_hash text)
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
                select t.commitment, t.checksum, t.record_ciphertext, t.sender_ciphertext from transition_output_record t where transition_output_id = transition_output_db_id into record_commitment, checksum, record_ciphertext, sender_ciphertext;
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


