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
CREATE TABLE Measurementtable (
	MTABLEID varchar(255),
	TABLELEVEL varchar(50),
	TYPEID varchar(255),
	BASETABLENAME varchar(255),
	DEFAULT_TEMPLATE varchar(50),
	PARTITIONPLAN varchar(128)
);
CREATE TABLE Measurementobjbhsupport (
	TYPEID varchar(255),
	OBJBHSUPPORT varchar(32)
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
CREATE TABLE Techpackdependency (
	VERSIONID varchar(128),
	TECHPACKNAME varchar(30),
	VERSION varchar(32)
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
CREATE TABLE Transformer (
	TRANSFORMERID varchar(255),
	VERSIONID varchar(128),
	DESCRIPTION varchar(32000),
	TYPE varchar(50)
);
CREATE TABLE Dataformat (
	DATAFORMATID varchar(100),
	TYPEID varchar(255),
	VERSIONID varchar(128),
	OBJECTTYPE varchar(255),
	FOLDERNAME varchar(128),
	DATAFORMATTYPE varchar(50)
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
CREATE TABLE Universename (
	VERSIONID varchar(128),
	UNIVERSENAME varchar(30),
	UNIVERSEEXTENSION varchar(16),
	ORDERNRO numeric(30, 6),
	UNIVERSEEXTENSIONNAME varchar(35)
);
CREATE TABLE Grouptypes (
	GROUPTYPE varchar(64),
	VERSIONID varchar(128),
	DATANAME varchar(128),
	DATATYPE varchar(50),
	DATASIZE int,
	DATASCALE int,
	NULLABLE smallint
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
CREATE TABLE Typeactivation (
	TECHPACK_NAME varchar(30),
	STATUS varchar(10),
	TYPENAME varchar(255),
	TABLELEVEL varchar(50),
	STORAGETIME numeric(15, 0),
	TYPE varchar(12),
	PARTITIONPLAN varchar(128)
);
