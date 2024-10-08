insert into TPActivation (TECHPACK_NAME, STATUS, VERSIONID, TYPE, MODIFIED) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST:((102))', 'PM', 0);
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTP', 'RAW', -1, 'Measurement', 'extrasmall_raw');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTP', 'DAYBH', -1, 'Measurement', 'extrasmall_daybh');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTP', 'DAY', -1, 'Measurement', 'extrasmall_day');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTP', 'COUNT', -1, 'Measurement', 'extrasmall_count');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTPBH', 'RANKBH', -1, 'Measurement', 'extrasmall_rankbh');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTP_V', 'RAW', -1, 'Measurement', 'extrasmall_raw');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTP_V', 'DAYBH', -1, 'Measurement', 'extrasmall_daybh');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_VCLTP_V', 'DAY', -1, 'Measurement', 'extrasmall_day');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_INTERNALLINKGROUP_V', 'RAW', -1, 'Measurement', 'extrasmall_raw');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_INTERNALLINKGROUP_V', 'DAY', -1, 'Measurement', 'extrasmall_day');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_ELEMBH', 'RANKBH', -1, 'Measurement', 'extrasmall_rankbh');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_SERVICE', 'RAW', -1, 'Measurement', 'extrasmall_raw');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_SERVICE', 'DAY', -1, 'Measurement', 'extrasmall_day');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_SERVICE', 'DAYBH', -1, 'Measurement', 'extrasmall_daybh');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_SERVICE', 'COUNT', -1, 'Measurement', 'extrasmall_count');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_ETHERNETLINK', 'COUNT', -1, 'Measurement', 'extrasmall_count');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_ETHERNETLINK', 'DAY', -1, 'Measurement', 'extrasmall_day');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DC_E_TEST_ETHERNETLINK', 'RAW', -1, 'Measurement', 'extrasmall_raw');
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DIM_E_TEST_ELEMBH_BHTYPE', 'PLAIN', -1, 'Reference', null);
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DIM_E_TEST_INTERNALLINKGROUP_V_pmPeakBwLevel', 'PLAIN', -1, 'Reference', null);
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DIM_E_TEST_VCLTP_V_pmBwUtilizationRx', 'PLAIN', -1, 'Reference', null);
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DIM_E_TEST_VCLTPBH_BHTYPE', 'PLAIN', -1, 'Reference', null);
insert into TypeActivation (TECHPACK_NAME, STATUS, TYPENAME, TABLELEVEL, STORAGETIME, TYPE, PARTITIONPLAN) values ('DC_E_TEST', 'ACTIVE', 'DIM_E_TEST_VCLTP_V_pmBwUtilizationTx', 'PLAIN', -1, 'Reference', null);

insert into PartitionPlan (PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE) values ('extrasmall_count', 30, 744, 90, 0);
insert into PartitionPlan (PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE) values ('extrasmall_day', 400, 9624, 1095, 0);
insert into PartitionPlan (PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE) values ('extrasmall_daybh', 400, 9624, 1095, 0);
insert into PartitionPlan (PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE) values ('extrasmall_plain', 90, 384, 250, 0);
insert into PartitionPlan (PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE) values ('extrasmall_rankbh', 400, 9624, 1095, 0);
insert into PartitionPlan (PARTITIONPLAN, DEFAULTSTORAGETIME, DEFAULTPARTITIONSIZE, MAXSTORAGETIME, PARTITIONTYPE) values ('extrasmall_raw', 30, 744, 90, 0);

