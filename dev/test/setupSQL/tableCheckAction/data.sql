insert into SYS.SYSUSERPERM (user_id, user_name) values (1, 'dc');
insert into SYS.SYSUSERPERM (user_id, user_name) values (2, 'guest');

insert into SYS.SYSTABLE (creator, table_type, table_name) values (1, 'BASE', 'DC_TABLE_A');
insert into SYS.SYSTABLE (creator, table_type, table_name) values (1, 'VIEW', 'DC_TABLE_B');
insert into SYS.SYSTABLE (creator, table_type, table_name) values (1, 'BASE', 'DC_TABLE_C');
insert into SYS.SYSTABLE (creator, table_type, table_name) values (1, 'VIEW', 'DC_TABLE_C');
insert into SYS.SYSTABLE (creator, table_type, table_name) values (2, 'BASE', 'GUEST_TABLE_A');
insert into SYS.SYSTABLE (creator, table_type, table_name) values (2, 'VIEW', 'GUEST_TABLE_B');


insert into DWHPARTITION (STORAGEID, TABLENAME, STARTTIME, STATUS, LOADORDER) values ('DC_TABLE_A:PLAIN', 'DC_TABLE_B', '1970-01-01 01:00:00.0', 'ACTIVE', 0);
insert into DWHPARTITION (STORAGEID, TABLENAME, STARTTIME, STATUS, LOADORDER) values ('DC_TABLE_A:PLAIN', 'DC_TABLE_C', '1970-01-01 01:00:00.0', 'ACTIVE', 0);

