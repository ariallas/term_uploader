DO $$
declare row_count bigint;
substance_id bigint;
begin
select id from ont.chemical_substances cs into substance_id where chemical_formula='Oxygen' or substance_name='O';
if substance_id is NULL then
select count(*) into row_count from ont.chemical_substances;
select id into substance_id from (insert into ont.chemical_substances values (row_count + 1, 'Oxygen', 'O') returning id);
end if;
END $$
LANGUAGE plpgsql;