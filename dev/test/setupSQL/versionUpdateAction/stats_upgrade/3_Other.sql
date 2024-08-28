truncate table TPActivation;
insert into TPActivation (TECHPACK_NAME, STATUS, VERSIONID, TYPE, MODIFIED) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST:((2))', 'PM', 0);
insert into DWHPartition (STORAGEID, TABLENAME, STARTTIME, ENDTIME, STATUS, LOADORDER) values ('DC_E_TEST_SECURITYHANDLING:DAY', 'DC_E_TEST_SECURITYHANDLING_DAY_01', '2011-01-15 00:00:00', '2011-04-15 00:00:00', 'ACTIVE', null);
insert into DWHPartition (STORAGEID, TABLENAME, STARTTIME, ENDTIME, STATUS, LOADORDER) values ('DC_E_TEST_SECURITYHANDLING:DAY', 'DC_E_TEST_SECURITYHANDLING_DAY_02', '2010-10-17 00:00:00', '2011-01-15 00:00:00', 'ACTIVE', null);
insert into DWHPartition (STORAGEID, TABLENAME, STARTTIME, ENDTIME, STATUS, LOADORDER) values ('DC_E_TEST_SECURITYHANDLING:RAW', 'DC_E_TEST_SECURITYHANDLING_RAW_01', '2011-04-28 00:00:00', '2011-05-05 00:00:00', 'ACTIVE', null);
insert into DWHPartition (STORAGEID, TABLENAME, STARTTIME, ENDTIME, STATUS, LOADORDER) values ('DC_E_TEST_SECURITYHANDLING:RAW', 'DC_E_TEST_SECURITYHANDLING_RAW_02', '2011-04-21 00:00:00', '2011-04-28 00:00:00', 'ACTIVE', null);
insert into SYS.SYSTABLE (table_id, table_type, table_name) values (1, 'BASE', 'DC_E_TEST_SECURITYHANDLING_RAW_01');
insert into SYS.SYSTABLE (table_id, table_type, table_name) values (2, 'BASE', 'DC_E_TEST_SECURITYHANDLING_RAW_02');
insert into SYS.SYSCOLUMNS (cname, creator, tname, coltype, length, syslength, colno) values ('MOID', 'dc', 'DC_E_TEST_SECURITYHANDLING_RAW_01', 'numeric', 16, 2, 1);
insert into SYS.SYSCOLUMNS (cname, creator, tname, coltype, length, syslength, colno) values ('MOID', 'dc', 'DC_E_TEST_SECURITYHANDLING_RAW_02', 'numeric', 16, 2, 1);
-- Dont insert the SYSTABLE or SYSCOLUMNS entries for DC_E_TEST_SECURITYHANDLING_DAY_01/02 : Covers VUA.checkRealPartitions() == false
