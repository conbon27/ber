
-- View Queries

select *
from ##tempraw;
select *
from ##tempevent;
select *
from ##tempsample;
select *
from ##tempmeasure;

-- Raw Data Example

DROP TABLE IF EXISTS ##tempraw;

DECLARE @BEGINDATE VARCHAR(20);
SET @BEGINDATE = 'Jan 19 2010';
-- Change this to your desired beginning date

SELECT ber.*,
	ROUND(COALESCE(geo_coordinates.Latitude, 0.0), 4) AS Latitude,
	ROUND(COALESCE(geo_coordinates.Longitude, 0.0), 4) AS Longitude
INTO ##tempraw
FROM [master].[dbo].[cleaned_ber_data] AS ber
	LEFT JOIN geo_coordinates ON ber.CountyName = geo_coordinates.CountyName
WHERE CONVERT(DATETIME, ber.DateOfAssessment, 100) >= CONVERT(DATETIME, @BEGINDATE, 100);


--############### Event Query #####################

drop table if exists ##tempevent
select 'BER-'+RTRIM(samplecode) as EventID
	, ParentEventID = NULL
	, DatasetID = '12345xyz'
	, DatasetName = '@DATASETNAME' 
	, DateOfAssessment as [DateTime]
	, DateTimeAccuracy = case when datediff(ss,cast(cast(DateOfAssessment as Date) as DateTime),DateOfAssessment) = 0 then 'Date Only' else 'Date & Time' end
	, DateTimeAttribute = 'Point'
	, TimeZoneOffset = 00.00
	, DateTimeISO = convert(varchar(33),DateOfAssessment,126)+'+00:00'
	, DateOfAssessment as StartDateTime 
	, DateOfAssessment as EndDateTime
	, Latitude
	, Longitude
	, LatLongAttribute = 'Point'
	, LocationLabel = CountyName
	, 'POINT (' + cast(Latitude as varchar) + ' ' + cast(Longitude as varchar)  + ')' as FootprintWKT
	, geometry::STGeomFromText('POINT (' + cast(Latitude as varchar) + ' ' + cast(Longitude as varchar)  + ')',0) as Geometry
	, coordinateuncertaintyinmeters=1.11
	, extractTime = getdate()
	, modifiedonDateTime = getdate()
into ##tempevent
from
	(
		select distinct
		samplecode, Latitude, Longitude, DateOfAssessment, CountyName
	from
		##tempraw
		)a

--############### Sample Query #####################

drop table if exists ##tempsample
select distinct 'BER-'+RTRIM(samplecode) as EventID,
	SampleID = 'BER-'+RTRIM(samplecode) + '-'+cast(TypeofRating as varchar)
	, ParentSampleID =NULL
	, extractonDateTime = getdate()
	, modifiedOnDatetime = getdate()
into ##tempsample
from ##tempraw sc

--############### Measurement Query #####################

DROP TABLE IF EXISTS ##tempmeasure;
CREATE TABLE ##tempmeasure
(
    SampleID VARCHAR(100),
    MeasurementID VARCHAR(100),
    ObservedProperty VARCHAR(100),
    Parameter INT,
    ParameterValue FLOAT,
    extractOnDateTime DATETIME,
    modifiedOnDateTime DATETIME
);

-- SQL for columns
DECLARE @columns NVARCHAR(MAX);
SET @columns = '';

SELECT @columns += 
    CASE
        WHEN @columns = '' THEN ''
        ELSE ', '
    END +
    'MAX(CASE WHEN ref.ParameterName = ''' + ParameterName + 
    ''' THEN c.' + QUOTENAME(ParameterName) + ' ELSE NULL END) AS [' + ParameterName + ']'
FROM referenceLists;

-- dynamic SQL
DECLARE @sql NVARCHAR(MAX);
SET @sql = 
    'INSERT INTO ##tempmeasure (SampleID, MeasurementID, ObservedProperty, Parameter, ParameterValue, extractOnDateTime, modifiedOnDateTime)
    SELECT 
        ''BER-'' + CAST(c.samplecode AS VARCHAR) + ''-'' + CAST(c.TypeofRating AS VARCHAR),
        ''BER-'' + CAST(c.samplecode AS VARCHAR) + ''-'' + CAST(c.TypeofRating AS VARCHAR) + ''-'' + RIGHT(''000'' + CAST(ROW_NUMBER() OVER (PARTITION BY c.samplecode, c.TypeofRating ORDER BY (SELECT NULL)) AS VARCHAR), 3),
        ref.ParameterName,
        ref.ParameterID,
        ' + @columns + ',
        GETDATE() AS extractOnDateTime,
        GETDATE() AS modifiedOnDateTime
    FROM cleaned_ber_data c
    CROSS JOIN referenceLists ref
    WHERE c.DateOfAssessment >= ''Jan 19 2010''
    GROUP BY c.samplecode, c.TypeofRating, ref.ParameterName, ref.ParameterID;';

EXEC sp_executesql @sql;


--###########  other query #################--
-- put anything else you want that doesn't explicitely fit in the other 4 tables here - i.e. superfluous value adding detail
drop table if exists ##tempother

select 'BER-'+RTRIM(samplecode) as ID, TableName = 'Events', OtherLabel, OtherValue, extractOndateTime = getdate()	, modifiedOnDateTime = getdate()
into ##tempother
from (
	 select distinct SampleCode, cast(WeekNo as varchar) as WeekNo, cast(convert(varchar(10),WeekDateFrom,103) as varchar) as WeekDateFrom, cast(LocationID as varchar) as LocationID, cast(RegionName as varchar) as RegionName, cast(ParentAreaID as varchar) as ParentAreaID, cast(ParentAreaCode as varchar) as ParentAreaCode, cast(ParentAreaName as varchar) as ParentAreaName, cast(LocationCode as varchar) as LocationCode
	from ##tempraw
	 
     ) p
UNPIVOT
		(OtherValue for OtherLabel in ())) as unpvt
UNION

select
	'BER-'+RTRIM(samplecode) + '-'+cast(ResultID as varchar) as ID,
	TableName = 'Blah',
	OtherLabel = 'BlahLabel',
	OtherValue = cast(Blah as varchar),
	extractOndateTime = getdate(),
	modifiedOnDateTime = getdate()
from ##tempraw