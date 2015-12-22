create sequence chemical_substances_id_seq;
create sequence data_sets_id_seq;
create sequence substances_in_states_id_seq;
create sequence points_of_measure_id_seq;
create sequence data_sources_id_seq;
create sequence measurement_uncertainties_id_seq;

select setval('chemical_substances_id_seq', (select max(id) + 1 from ont.chemical_substances));
select setval('data_sets_id_seq', (select max(id) + 1 from ont.data_sets));
select setval('substances_in_states_id_seq', (select max(id) + 1 from ont.substances_in_states));
select setval('points_of_measure_id_seq', (select max(id) + 1 from ont.points_of_measure));
select setval('data_sources_id_seq', (select max(id) + 1 from ont.data_sources));
select setval('measurement_uncertainties_id_seq', (select max(id) + 1 from ont.measurement_uncertainties));