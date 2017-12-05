if object_ID('tempdb..#address') is not null
	drop table #address
create table #address
(
	[AddressID] [varchar](20) NOT NULL,
	[State] [varchar](50) NULL,
	[HashValue] [binary](16) NULL,
	[CreatedAt] [smalldatetime] NOT NULL,
	[UpdatedAt] [smalldatetime] NOT NULL,
	[Address1] [varchar](50) NULL,
	[City] [varchar](50) NULL,
	[PostalCode] [varchar](20) NULL,
	[PhoneNumber] [varchar](60) NULL,
	[AltPhoneNumber] [varchar](60) NULL,
	[EmailAddress] [varchar](255) NULL
)

insert into #address
	SELECT id as [AddressID]
		,billingstate as [State]
		,null as [HashValue]
		,createddate as [CreatedAt]
		,lastmodifieddate as [UpdatedAt]
		,billingstreet as [Address1]
		,billingcity as [City]
		,billingpostalcode as [PostalCode]
		,null as [PhoneNumber] -- If below 3 can be found.
		,null as [AltPhoneNumber] -- If below 3 can be found.
		,null as [EmailAddress] -- If below 3 can be found.
	from [CLS].[cls_migration].[public].[sf_account]
	where delete_flag = 'N' and isdeleted = 'false'

update #address set HashValue = HASHBYTES('MD5',ISNULL(RTRIM(AddressID),'')+ISNULL(RTRIM(State),'')+ISNULL(RTRIM(Address1), '')+ISNULL(RTRIM(City), '')+ISNULL(RTRIM(PostalCode), '')+ISNULL(RTRIM(PhoneNumber), '')+ISNULL(RTRIM(AltPhoneNumber), '')+ISNULL(RTRIM(EmailAddress), ''))

DECLARE @ActionResult TABLE (MergeAction VARCHAR(20))

MERGE INTO LAPro.Address WITH (HOLDLOCK) AS trg    -- clusterd on ID, non-clustered on LoanID
USING (SELECT AddressID
			,Address1
			,City
            ,State
			,PostalCode
			,PhoneNumber
			,AltPhoneNumber
			,EmailAddress
            ,HashValue
			,UpdatedAt
			,CreatedAt
        FROM #address
        ) AS src ON (src.AddressID=trg.AddressID)

	WHEN MATCHED AND trg.HashValue <> src.HashValue
	THEN UPDATE SET trg.Address1    = src.Address1
				,trg.City		  = src.City
				,trg.State       = src.State
				,trg.PostalCode  = src.PostalCode
				,trg.PhoneNumber = src.PhoneNumber
				,trg.AltPhoneNumber = src.AltPhoneNumber
				,trg.EmailAddress= src.EmailAddress
                ,trg.HashValue   = src.HashValue
                ,trg.UpdatedAt   = src.UpdatedAt

	WHEN NOT MATCHED BY TARGET   -- in src, not trg
	THEN INSERT ( AddressID
			,Address1
			,City
            ,[State]
			,PostalCode
			,PhoneNumber
			,AltPhoneNumber
			,EmailAddress
            ,HashValue
            ,CreatedAt
            ,UpdatedAt
            )
    VALUES (src.AddressID
		    ,src.Address1
			,src.City
            ,src.[State]
			,src.PostalCode
			,src.PhoneNumber
			,src.AltPhoneNumber
			,src.EmailAddress
            ,src.HashValue
            ,CreatedAt
            ,UpdatedAt
            )

/* -- JIN This will become an issue as we have 2 sources... need to figure out way to delete without impacting the other side.
WHEN NOT MATCHED BY SOURCE   -- in trg, not src
	THEN DELETE
*/
OUTPUT $action INTO @ActionResult;

SELECT InsertCnt  = IsNULL(SUM(CASE WHEN MergeAction ='INSERT' THEN 1 ELSE 0 END),0)  -- inserts & updates fall under this "INSERT" action
            -- ,@UpdateCnt  = IsNULL(SUM(CASE WHEN MergeAction ='UPDATE' THEN 1 ELSE 0 END),0)  -- these aren't true updates
            ,DeleteCnt  = IsNULL(SUM(CASE WHEN MergeAction ='DELETE' THEN 1 ELSE 0 END),0)  -- deletes & updates fall under this "DELETE" action
        FROM @ActionResult

select * from LAPro.Address 
--truncate table LAPro.Address