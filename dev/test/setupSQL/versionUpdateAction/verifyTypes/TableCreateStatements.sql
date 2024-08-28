CREATE TABLE META_DATABASES (
	USERNAME VARCHAR(30), 
	VERSION_NUMBER VARCHAR(32), 
	TYPE_NAME VARCHAR(15), 
	CONNECTION_ID NUMERIC(38), 
	CONNECTION_NAME VARCHAR(30), 
	CONNECTION_STRING VARCHAR(200), 
	PASSWORD VARCHAR(30), 
	DESCRIPTION VARCHAR(32000), 
	DRIVER_NAME VARCHAR(100), 
	DB_LINK_NAME VARCHAR(128)
);
CREATE TABLE Versioning (
	VERSIONID varchar(128),
	DESCRIPTION varchar(50),
	STATUS numeric(9, 0),
	TECHPACK_NAME varchar(30),
	TECHPACK_VERSION varchar(32),
	TECHPACK_TYPE varchar(10),
	PRODUCT_NUMBER varchar(255),
	LOCKEDBY varchar(255),
	LOCKDATE timestamp,
	BASEDEFINITION varchar(128),
	BASEVERSION varchar(16),
	INSTALLDESCRIPTION varchar(32000),
	UNIVERSENAME varchar(30),
	UNIVERSEEXTENSION varchar(16),
	ENIQ_LEVEL varchar(12),
	LICENSENAME varchar(255)
);
CREATE TABLE Measurementtypeclass (
	TYPECLASSID varchar(255),
	VERSIONID varchar(128),
	DESCRIPTION varchar(32000)
);
CREATE TABLE Measurementtype (
	TYPEID varchar(255),
	TYPECLASSID varchar(255),
	TYPENAME varchar(255),
	VENDORID varchar(128),
	FOLDERNAME varchar(128),
	DESCRIPTION varchar(32000),
	STATUS numeric(9, 0),
	VERSIONID varchar(128),
	OBJECTID varchar(255),
	OBJECTNAME varchar(255),
	OBJECTVERSION int,
	OBJECTTYPE varchar(255),
	JOINABLE varchar(255),
	SIZING varchar(16),
	TOTALAGG int,
	ELEMENTBHSUPPORT int,
	RANKINGTABLE int,
	DELTACALCSUPPORT int,
	PLAINTABLE int,
	UNIVERSEEXTENSION varchar(12),
	VECTORSUPPORT int,
	DATAFORMATSUPPORT int,
	FOLLOWJOHN int,
	ONEMINAGG int,
	FIFTEENMINAGG int,
	EVENTSCALCTABLE int,
	MIXEDPARTITIONSTABLE int,
	LOADFILE_DUP_CHECK int,
	SONAGG int null,
	SONFIFTEENMINAGG int null,
	ROPGRPCELL varchar(255) null
);
CREATE TABLE Measurementkey (
	TYPEID varchar(255),
	DATANAME varchar(128),
	DESCRIPTION varchar(32000),
	ISELEMENT int,
	UNIQUEKEY int,
	COLNUMBER numeric(9, 0),
	DATATYPE varchar(50),
	DATASIZE int,
	DATASCALE int,
	UNIQUEVALUE numeric(9, 0),
	NULLABLE int,
	INDEXES varchar(20),
	INCLUDESQL int,
	UNIVOBJECT varchar(128),
	JOINABLE int,
	DATAID varchar(255),
	ROPGRPCELL INT null
);
CREATE TABLE Measurementobjbhsupport (
	TYPEID varchar(255),
	OBJBHSUPPORT varchar(32)
);
CREATE TABLE Measurementtable (
	MTABLEID varchar(255),
	TABLELEVEL varchar(50),
	TYPEID varchar(255),
	BASETABLENAME varchar(255),
	DEFAULT_TEMPLATE varchar(50),
	PARTITIONPLAN varchar(128)
);
CREATE TABLE Measurementcolumn (
	MTABLEID varchar(255),
	DATANAME varchar(128),
	COLNUMBER numeric(9, 0),
	DATATYPE varchar(50),
	DATASIZE int,
	DATASCALE int,
	UNIQUEVALUE numeric(9, 0),
	NULLABLE int,
	INDEXES varchar(20),
	DESCRIPTION varchar(32000),
	DATAID varchar(255),
	RELEASEID varchar(50),
	UNIQUEKEY int,
	INCLUDESQL int,
	COLTYPE varchar(16),
	FOLLOWJOHN int
);
CREATE TABLE Measurementcounter (
	TYPEID varchar(255),
	DATANAME varchar(128),
	DESCRIPTION varchar(32000),
	TIMEAGGREGATION varchar(50),
	GROUPAGGREGATION varchar(50),
	COUNTAGGREGATION varchar(32000),
	COLNUMBER numeric(9, 0),
	DATATYPE varchar(50),
	DATASIZE int,
	DATASCALE int,
	INCLUDESQL int,
	UNIVOBJECT varchar(128),
	UNIVCLASS varchar(35),
	COUNTERTYPE varchar(16),
	COUNTERPROCESS varchar(16),
	DATAID varchar(255),
	FOLLOWJOHN int
);
CREATE TABLE Measurementvector (
	TYPEID varchar(255),
	DATANAME varchar(128),
	VENDORRELEASE varchar(16),
	VINDEX numeric(30, 6),
	VFROM varchar(255),
	VTO varchar(255),
	MEASURE varchar(255),
	QUANTITY numeric(30, 6)
);
CREATE TABLE Busyhour (
	VERSIONID varchar(128),
	BHLEVEL varchar(255),
	BHTYPE varchar(32),
	BHCRITERIA varchar(32000),
	WHERECLAUSE varchar(32000),
	DESCRIPTION varchar(32000),
	TARGETVERSIONID varchar(128),
	BHOBJECT varchar(32),
	BHELEMENT int,
	ENABLE int,
	AGGREGATIONTYPE varchar(128),
	OFFSET int,
	WINDOWSIZE int,
	LOOKBACK int,
	P_THRESHOLD int,
	N_THRESHOLD int,
	CLAUSE varchar(32000),
	PLACEHOLDERTYPE varchar(128),
	GROUPING varchar(32),
	REACTIVATEVIEWS int
);
CREATE TABLE Busyhourmapping (
	VERSIONID varchar(128),
	BHLEVEL varchar(255),
	BHTYPE varchar(32),
	TARGETVERSIONID varchar(128),
	BHOBJECT varchar(32),
	TYPEID varchar(255),
	BHTARGETTYPE varchar(128),
	BHTARGETLEVEL varchar(128),
	ENABLE int
);
CREATE TABLE Busyhourplaceholders (
	VERSIONID varchar(128),
	BHLEVEL varchar(255),
	PRODUCTPLACEHOLDERS int,
	CUSTOMPLACEHOLDERS int
);
CREATE TABLE Universeclass (
	VERSIONID varchar(128),
	CLASSNAME varchar(128),
	UNIVERSEEXTENSION varchar(12),
	DESCRIPTION varchar(32000),
	PARENT varchar(128),
	OBJ_BH_REL int,
	ELEM_BH_REL int,
	INHERITANCE int,
	ORDERNRO numeric(30, 6)
);
CREATE TABLE Verificationobject (
	VERSIONID varchar(128),
	MEASTYPE varchar(128),
	MEASLEVEL varchar(32),
	OBJECTCLASS varchar(32),
	OBJECTNAME varchar(32)
);
CREATE TABLE Verificationcondition (
	VERSIONID varchar(128),
	FACTTABLE varchar(2560),
	VERLEVEL varchar(32),
	CONDITIONCLASS varchar(32),
	VERCONDITION varchar(128),
	PROMPTNAME1 varchar(255),
	PROMPTVALUE1 varchar(128),
	PROMPTNAME2 varchar(255),
	PROMPTVALUE2 varchar(128),
	OBJECTCONDITION varchar(255),
	PROMPTNAME3 varchar(255),
	PROMPTVALUE3 varchar(128)
);
CREATE TABLE Transformer (
	TRANSFORMERID varchar(255),
	VERSIONID varchar(128),
	DESCRIPTION varchar(32000),
	TYPE varchar(50)
);
CREATE TABLE Transformation (
	TRANSFORMERID varchar(255),
	ORDERNO numeric(9, 0),
	TYPE varchar(128),
	SOURCE varchar(128),
	TARGET varchar(128),
	CONFIG varchar(32000),
	DESCRIPTION varchar(32000)
);
CREATE TABLE Referencetable (
	TYPEID varchar(255),
	VERSIONID varchar(128),
	TYPENAME varchar(255),
	OBJECTID varchar(255),
	OBJECTNAME varchar(255),
	OBJECTVERSION varchar(50),
	OBJECTTYPE varchar(255),
	DESCRIPTION varchar(32000),
	STATUS numeric(9, 0),
	UPDATE_POLICY numeric(9, 0),
	TABLE_TYPE varchar(12),
	DATAFORMATSUPPORT int,
	BASEDEF int
);
CREATE TABLE Referencecolumn (
	TYPEID varchar(255),
	DATANAME varchar(128),
	COLNUMBER numeric(9, 0),
	DATATYPE varchar(50),
	DATASIZE int,
	DATASCALE int,
	UNIQUEVALUE numeric(9, 0),
	NULLABLE int,
	INDEXES varchar(20),
	UNIQUEKEY int,
	INCLUDESQL int,
	INCLUDEUPD int,
	COLTYPE varchar(16),
	DESCRIPTION varchar(32000),
	UNIVERSECLASS varchar(35),
	UNIVERSEOBJECT varchar(128),
	UNIVERSECONDITION varchar(255),
	DATAID varchar(255),
	BASEDEF int
);
CREATE TABLE Dataformat (
	DATAFORMATID varchar(100),
	TYPEID varchar(255),
	VERSIONID varchar(128),
	OBJECTTYPE varchar(255),
	FOLDERNAME varchar(128),
	DATAFORMATTYPE varchar(50)
);
CREATE TABLE Defaulttags (
	TAGID varchar(128),
	DATAFORMATID varchar(100),
	DESCRIPTION varchar(200)
);
CREATE TABLE Dataitem (
	DATAFORMATID varchar(100),
	DATANAME varchar(128),
	COLNUMBER numeric(9, 0),
	DATAID varchar(255),
	PROCESS_INSTRUCTION varchar(128),
	DATATYPE varchar(50),
	DATASIZE int,
	DATASCALE int
);
CREATE TABLE Externalstatement (
	VERSIONID varchar(128),
	STATEMENTNAME varchar(255),
	EXECUTIONORDER numeric(9, 0),
	DBCONNECTION varchar(20),
	STATEMENT varchar(32000),
	BASEDEF int
);
CREATE TABLE Universename (
	VERSIONID varchar(128),
	UNIVERSENAME varchar(30),
	UNIVERSEEXTENSION varchar(16),
	ORDERNRO numeric(30, 6),
	UNIVERSEEXTENSIONNAME varchar(35)
);
CREATE TABLE Aggregation (
	AGGREGATION varchar(255),
	VERSIONID varchar(128),
	AGGREGATIONSET varchar(100),
	AGGREGATIONGROUP varchar(100),
	REAGGREGATIONSET varchar(100),
	REAGGREGATIONGROUP varchar(100),
	GROUPORDER int,
	AGGREGATIONORDER int,
	AGGREGATIONTYPE varchar(50),
	AGGREGATIONSCOPE varchar(50)
);
CREATE TABLE Aggregationrule (
	AGGREGATION varchar(255),
	VERSIONID varchar(128),
	RULEID int,
	TARGET_TYPE varchar(50),
	TARGET_LEVEL varchar(50),
	TARGET_TABLE varchar(255),
	TARGET_MTABLEID varchar(255),
	SOURCE_TYPE varchar(50),
	SOURCE_LEVEL varchar(50),
	SOURCE_TABLE varchar(255),
	SOURCE_MTABLEID varchar(255),
	RULETYPE varchar(50),
	AGGREGATIONSCOPE varchar(50),
	BHTYPE varchar(50),
	ENABLE int
);
CREATE TABLE Tpactivation (
	TECHPACK_NAME varchar(30),
	STATUS varchar(10),
	VERSIONID varchar(128),
	TYPE varchar(10),
	MODIFIED int
);
CREATE TABLE DWHColumn (
	STORAGEID varchar(255),
	DATANAME varchar(128),
	COLNUMBER numeric(9, 0),
	DATATYPE varchar(50),
	DATASIZE int,
	DATASCALE int,
	UNIQUEVALUE numeric(9, 0),
	NULLABLE int,
	INDEXES varchar(20),
	UNIQUEKEY int,
	STATUS varchar(10),
	INCLUDESQL int
);
CREATE TABLE DWHPartition (
	STORAGEID varchar(255),
	TABLENAME varchar(255),
	STARTTIME timestamp,
	ENDTIME timestamp,
	STATUS varchar(10),
	LOADORDER int
);
CREATE TABLE DWHTechPacks (
	TECHPACK_NAME varchar(30),
	VERSIONID varchar(128),
	CREATIONDATE timestamp
);
CREATE TABLE DWHType (
	TECHPACK_NAME varchar(30),
	TYPENAME varchar(255),
	TABLELEVEL varchar(50),
	STORAGEID varchar(255),
	PARTITIONSIZE numeric(9, 0),
	PARTITIONCOUNT numeric(9, 0),
	STATUS varchar(50),
	TYPE varchar(50),
	OWNER varchar(50),
	VIEWTEMPLATE varchar(255),
	CREATETEMPLATE varchar(255),
	NEXTPARTITIONTIME timestamp,
	BASETABLENAME varchar(125),
	DATADATECOLUMN varchar(128),
	PUBLICVIEWTEMPLATE varchar(255),
	PARTITIONPLAN varchar(128)
);
CREATE TABLE Supportedvendorrelease (
	VERSIONID varchar(128),
	VENDORRELEASE varchar(16)
);
CREATE TABLE ExternalStatement (
	VERSIONID varchar(128),
	STATEMENTNAME varchar(255),
	EXECUTIONORDER numeric(9, 0),
	DBCONNECTION varchar(20),
	STATEMENT varchar(32000),
	BASEDEF int
);
CREATE TABLE Techpackdependency (
	VERSIONID varchar(128),
	TECHPACKNAME varchar(30),
	VERSION varchar(32)
);
CREATE TABLE Universetable (
	VERSIONID varchar(128),
	TABLENAME varchar(255),
	UNIVERSEEXTENSION varchar(12),
	OWNER varchar(255),
	ALIAS varchar(255),
	OBJ_BH_REL int,
	ELEM_BH_REL int,
	INHERITANCE int,
	ORDERNRO numeric(30, 6)
);
CREATE TABLE Universejoin (
	VERSIONID varchar(128),
	SOURCETABLE varchar(32000),
	SOURCELEVEL varchar(255),
	SOURCECOLUMN varchar(255),
	TARGETTABLE varchar(32000),
	TARGETLEVEL varchar(255),
	TARGETCOLUMN varchar(255),
	EXPRESSION varchar(255),
	CARDINALITY varchar(255),
	CONTEXT varchar(32000),
	EXCLUDEDCONTEXTS varchar(32000),
	TMPCOUNTER int,
	ORDERNRO numeric(30, 6)
);
CREATE TABLE Universeobject (
	VERSIONID varchar(128),
	CLASSNAME varchar(128),
	UNIVERSEEXTENSION varchar(12),
	OBJECTNAME varchar(128),
	DESCRIPTION varchar(32000),
	OBJECTTYPE varchar(16),
	QUALIFICATION varchar(16),
	AGGREGATION varchar(16),
	OBJSELECT varchar(32000),
	OBJWHERE varchar(32000),
	PROMPTHIERARCHY varchar(255),
	OBJ_BH_REL int,
	ELEM_BH_REL int,
	INHERITANCE int,
	ORDERNRO numeric(30, 6)
);
CREATE TABLE Universecondition (
	VERSIONID varchar(128),
	CLASSNAME varchar(128),
	UNIVERSEEXTENSION varchar(12),
	UNIVERSECONDITION varchar(128),
	DESCRIPTION varchar(32000),
	CONDWHERE varchar(32000),
	AUTOGENERATE int,
	CONDOBJCLASS varchar(128),
	CONDOBJECT varchar(128),
	PROMPTTEXT varchar(255),
	MULTISELECTION int,
	FREETEXT int,
	OBJ_BH_REL int,
	ELEM_BH_REL int,
	INHERITANCE int,
	ORDERNRO numeric(30, 6)
);
CREATE TABLE SYS.SYSTABLE (
	table_id varchar(32),
	table_type varchar(32),
	table_name varchar(64)
);
CREATE TABLE SYS.SYSCOLUMNS (
	cname varchar(128),
	creator varchar(32),
	tname varchar(64),
	coltype varchar(128),
	length integer,
	syslength smallint,
	colno integer,
	nulls char(1)
);
CREATE TABLE SYS.SYSINDEX (
	index_name varchar(64),
	index_owner varchar(32),
	index_type varchar(4)
);
CREATE TABLE SYS.SYSINDEXES (
	iname varchar(64),
	tname varchar(64),
	colnames varchar(4)
);

CREATE TABLE DC_E_TEST_SECURITYHANDLING_RAW_01(MOID numeric(16, 2));
CREATE TABLE DC_E_TEST_SECURITYHANDLING_RAW_02(MOID numeric(16, 2));

