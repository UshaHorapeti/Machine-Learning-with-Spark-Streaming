#Load External DLL and Add Types
$assemblyPath = ".\Microsoft.AnalysisServices.AdomdClient.dll"
$adomd_obj = Add-Type -Path $assemblyPath -PassThru
$Connection_class = $adomd_obj | Where-Object {$_.FullName -eq 'Microsoft.AnalysisServices.AdomdClient.AdomdConnection'}
$Adapter_class = $adomd_obj | Where-Object {$_.FullName -eq 'Microsoft.AnalysisServices.AdomdClient.AdomdDataAdapter'}

$PowerBILogin = "ISCBI001@bd.com"
$PowerBIPassword = "52SQ6I>*P+*63*Z"
$PowerBIEndpoint = "powerbi://api.powerbi.com/v1.0/myorg/E2E%20Supply%20Chain%20Excellence"
$reportName = "Demand Reconciliation"

$RunDate = Get-Date
$SaveDate = $RunDate.ToString("yyyyMMdd")

$FileName = $SaveDate + "_Bpcbysku.csv"
$ExportPath = "F:\DP Waterfall\" + $FileName

Write-Output "Exporting File: $FileName , ReportDate = $SaveDate"

$Query = @"
DEFINE
    VAR __DS0FilterTable = 
        FILTER(
            KEEPFILTERS(VALUES('Calendar'[Month Start])),
            AND(
                'Calendar'[Month Start] >= DATE(2025, 2, 10),
                'Calendar'[Month Start] <  DATE(2026, 2, 10)
            )
        )

    VAR __DS0FilterTable2 = 
        TREATAS({"Units"}, 'Display Toggle'[Display Toggle])

    VAR __DS0FilterTable3 = 
        TREATAS({"None"}, 'Units Toggle'[Units Toggle])

    VAR __DS0FilterTable4 = 
        TREATAS({"BD Interventional"}, 'BPC All'[Segment])

    VAR __DS0FilterTable5 = 
        TREATAS({"Japan Performance",
            "Taiwan"}, 'BPC All'[Country])

    VAR __DS0FilterTable6 = 
        TREATAS({"Business Unit"}, 'BPC Actuals Categories'[Field])

    VAR __DS0FilterTable7 = 
        FILTER(
            KEEPFILTERS(VALUES('Calendar'[Fiscal Year])),
            NOT(ISBLANK('Calendar'[Fiscal Year]))
        )

    VAR __DS0FilterTable8 = 
        FILTER(
            KEEPFILTERS(VALUES('BPC All'[BU])),
            NOT(
                'BPC All'[BU] IN {"BDI",
                    "BDM",
                    "BLS",
                    "CCA",
                    "CMA",
                    "DOP",
                    "NBU",
                    "RSS",
                    "UNK",
                    "VMR"}
            )
        )

    VAR __DS0FilterTable9 = 
        FILTER(
            KEEPFILTERS(VALUES('Main'[ReltioBU])),
            NOT(
                'Main'[ReltioBU] IN {BLANK(),
                    "BDI",
                    "BDM",
                    "BLS",
                    "CCA",
                    "CMA",
                    "DC",
                    "DOP",
                    "NBU",
                    "RSS",
                    "UNK",
                    "VMR"}
            )
        )

    VAR __DS0Core = 
        SUMMARIZECOLUMNS(
            ROLLUPADDISSUBTOTAL(
                ROLLUPGROUP(
                    'BPC All'[BU],
                    'BPC All'[Material ID],
                    'BPC All'[Country],
                    'Calendar'[Month Year],
                    'Calendar'[Month Sort]
                ), "IsGrandTotalRowTotal"
            ),
            __DS0FilterTable,
            __DS0FilterTable2,
            __DS0FilterTable3,
            __DS0FilterTable4,
            __DS0FilterTable5,
            __DS0FilterTable6,
            __DS0FilterTable7,
            __DS0FilterTable8,
            __DS0FilterTable9,
            "BPC_Actuals", 'Measures Table'[BPC Actuals],
            "v_BPC_Actuals_FormatString", IGNORE('Measures Table'[_BPC Actuals FormatString])
        )

    VAR __Output =
        SELECTCOLUMNS(
            __DS0Core,
            'BPC All'[BU],
            'BPC All'[Material ID],
            'BPC All'[Country],
            'Calendar'[Month Year],
            "BPC Actuals", [BPC_Actuals]
        )

EVALUATE
    __Output

ORDER BY
    'BPC All'[BU],
    'BPC All'[Material ID],
    'BPC All'[Country],
    'Calendar'[Month Year]
"@

$connstring = "Provider=MSOLAP.8;Data Source="+ $PowerBIEndpoint + ";Initial Catalog=" + $reportName +";User ID="+ $PowerBILogin +";Password="+ $PowerBIPassword
#Write-Output $connstring
Write-Output "Query: " $query
$Connection = New-Object $Connection_class
$Results = New-Object System.Data.DataTable
$Connection.ConnectionString = $connstring 
 
$Connection.Open()
 
$Adapter = New-Object $Adapter_class $Query ,$Connection
$Adapter.Fill($Results)

$Results | export-csv -Path $ExportPath  -NoTypeInformation
#$Results | Format-Table

$Connection.Dispose()
$Connection.Close()