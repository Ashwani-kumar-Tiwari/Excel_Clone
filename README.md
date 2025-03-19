-- Declare variables for date calculations
DECLARE Datacut_Quarter STRING = '2024 02';
DECLARE Year STRING;
DECLARE Quarter STRING;
DECLARE Month STRING;

-- Extract year and quarter
SET Year = LEFT(Datacut_Quarter, 4);
SET Quarter = RIGHT(Datacut_Quarter, 2);

-- Determine month based on quarter
SET Month = CASE Quarter
    WHEN '01' THEN '03'
    WHEN '02' THEN '06'
    WHEN '03' THEN '09'
    WHEN '04' THEN '12'
    ELSE '00'
END;

-- Calculate Start of Quarter (SOQ) and End of Quarter (EOQ)
DECLARE SOQ DATE = DATEADD(QUARTER, DATEDIFF(QUARTER, DATEFROMPARTS(0, 1, 1), DATEFROMPARTS(Year::INT, Month::INT, 1)), DATEFROMPARTS(0, 1, 1));
DECLARE EOQ DATE = DATEADD(DAY, -1, DATEADD(QUARTER, DATEDIFF(QUARTER, DATEFROMPARTS(0, 1, 1), DATEFROMPARTS(Year::INT, Month::INT, 1)) + 1, DATEFROMPARTS(0, 1, 1)));

-- Main query to fetch active risks
SELECT DISTINCT
    @EOQ AS "As At Date",
    CONCAT(@Year, @Quarter) AS "As At",
    'Live' AS "Data Stream",
    @Quarter AS "Quarter",
    @Year AS "Year",

    -- Risk and policy details
    CONCAT(R.Risk_Reference, R.Sub_Risk_Identifier) AS "Risk Reference (Dec)",
    JC.Section_Reference_Company AS "Binder Section Reference",
    CONCAT(R.Risk_Reference, R.Sub_Risk_Identifier) AS "Section Reference",
    R.Risk_Reference AS "Policy Reference",
    JC.Contract_Unique_Market_Reference AS "Unique Market Reference",

    -- Dates and cancellation details
    R.Risk_Inception_Date AS "Inception Date",
    R.Risk_Expiry_Date AS "Expiry Date",
    R.Cancellation_Date AS "Cancellation Date",
    R.Cancellation_Reason AS "Cancellation Reason",

    -- Aggregated references and risk codes
    CONCAT(LEFT(JC.Section_Reference_Company, 6), RIGHT(JC.Section_Reference_Company, 4)) AS "Agg Ref",
    CONCAT(
        LEFT(JC.Section_Reference_Company, 6),
        RIGHT(JC.Section_Reference_Company, 4),
        R.Assigned_Risk_Code_Reference
    ) AS "Agg Ref Risk Code",
    CONCAT(JC.Contract_Unique_Market_Reference, R.Assigned_Risk_Code_Reference) AS "UMR Risk Code",
    R.Assigned_Risk_Code_Reference AS "Risk Code",
    R.Assigned_Risk_Code_Description AS "Risk Description",

    -- Additional details
    R.Coverholder_Name AS "Coverholder Name",
    R.Risk_Country AS "Risk Country",
    R.Risk_Currency AS "Risk Currency",
    
    -- Data source and financials
    SUM(R.Gross_Premium_100_Original) AS "Gross Written Premium"

FROM TRANSACTIONAL.RISK R

-- Join with Core.Contract to get contract details
INNER JOIN (
    SELECT DISTINCT
        Contract.Tide_ContractId,
        Contract.Tide_SectionId,
        Contract.DWH_DivisionName,
        Contract.Contract_Unique_Market_Reference,
        Contract.Section_Reference_Company,
        Contract.Contract_Year_of_Account,
        ROW_NUMBER() OVER (
            PARTITION BY Contract.Tide_SectionId
            ORDER BY Contract.DWH_InsertDateTime DESC
        ) AS RowNum
    FROM Core.Contract Contract
) JC ON JC.Tide_SectionId = R.Tide_SectionId AND JC.RowNum = 1

-- Join with Core.Bordereau to get bordereau details
INNER JOIN (
    SELECT DISTINCT
        Bordereau.Tide_BordereauId,
        Bordereau.Tide_ReportChannelId,
        Bordereau.Tide_ContractId,
        Bordereau.Bordereau_Status,
        ROW_NUMBER() OVER (
            PARTITION BY Bordereau.Tide_BordereauId
            ORDER BY Bordereau.DWH_InsertDateTime DESC
        ) AS RowNum
    FROM Core.Bordereau Bordereau
) JB ON JB.Tide_BordereauId = R.Tide_BordereauId AND JB.RowNum = 1

WHERE 
    JB.Bordereau_Status != 'deleted' -- Exclude deleted bordereaux
    AND R.Risk_Inception_Date <= @EOQ -- Inception date must be before or on EOQ
    AND R.Risk_Expiry_Date > @SOQ -- Expiry date must be after SOQ
    AND JC.Section_Insurer_Lead_YN = 'Yes' -- Insurer lead must be confirmed

GROUP BY 
    CONCAT(R.Risk_Reference, R.Sub_Risk_Identifier),
    JC.Section_Reference_Company,
    JC.Contract_Unique_Market_Reference,
    
-- Group by all other fields used in SELECT statement to ensure proper aggregation.
