DO $$
declare row_count bigint;
begin
select count(*) into row_count from ont.chemical_substances;
insert into ont.chemical_substances values (row_count + 1, 'Bla bla', 'Bla');
END $$
LANGUAGE plpgsql;