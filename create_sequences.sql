create sequence chemical_substances_id_seq;

setval('chemical_substances_id_seq', (select max(id) + 1 from ont.chemical_substances))