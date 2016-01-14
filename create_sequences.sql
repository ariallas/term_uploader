create sequence chemical_substances_id_seq;
create sequence data_sets_id_seq;
create sequence substances_in_states_id_seq;
create sequence points_of_measure_id_seq;
create sequence data_sources_id_seq;
create sequence measurement_uncertainties_id_seq;
create sequence physical_quantities_id_seq;
create sequence dimensions_id_seq;
create sequence states_id_seq;

select setval('chemical_substances_id_seq', (select max(id) from ont.chemical_substances));
select setval('data_sets_id_seq', (select max(id) from ont.data_sets));
select setval('substances_in_states_id_seq', (select max(id) from ont.substances_in_states));
select setval('points_of_measure_id_seq', (select max(id) from ont.points_of_measure));
select setval('data_sources_id_seq', (select max(id) from ont.data_sources));
select setval('measurement_uncertainties_id_seq', (select max(id) from ont.measurement_uncertainties));
select setval('physical_quantities_id_seq', (select max(id) from ont.physical_quantities));
select setval('dimensions_id_seq', (select max(id) from ont.dimensions));
select setval('states_id_seq', (select max(id) from ont.states));