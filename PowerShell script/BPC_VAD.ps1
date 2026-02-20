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

$FileName = $SaveDate + "Bpcvad.csv"
$ExportPath = "F:\DP Waterfall\" + $FileName

Write-Output "Exporting File: $FileName , ReportDate = $SaveDate"

$Query = @"
DEFINE

    VAR __DS0FilterTable =
        FILTER(
            KEEPFILTERS(VALUES('Calendar'[Month Start])),
            'Calendar'[Month Start] >= DATE(2025, 2, 14) &&
            'Calendar'[Month Start] <  DATE(2026, 2, 14)
        )

    VAR __DS0FilterTable2 =
        TREATAS({"None"}, 'Units Toggle'[Units Toggle])

    VAR __DS0FilterTable3 =
        TREATAS(
            {
                "Vascular Access Technologies",
                "Advanced Access Devices"
            },
            'BPC All'[PH1Desc]
        )

    VAR __DS0FilterTable4 =
        FILTER(
            KEEPFILTERS(VALUES('BPC All'[BU])),
            NOT(
                'BPC All'[BU] IN {
                    "BDM","BLS","CCA","CMA",
                    "DOP","NBU","RSS","UNK","BDI"
                }
            )
        )

    VAR __DS0FilterTable5 =
        FILTER(
            KEEPFILTERS(VALUES('Main'[ReltioBU])),
            NOT(
                'Main'[ReltioBU] IN {
                    BLANK(),"BDI","BDM","BLS","CCA",
                    "CMA","DC","DOP","NBU","RSS","UNK","VMR"
                }
            )
        )

    VAR __DS0Core =
        SUMMARIZECOLUMNS(
            'Calendar'[Month Year],
            'Calendar'[Month Sort],
            'BPC All'[Country],
            'BPC All'[Material ID],
            'BPC All'[PH1Desc],

            __DS0FilterTable,
            __DS0FilterTable2,
            __DS0FilterTable3,
            __DS0FilterTable4,
            __DS0FilterTable5,

            "BPC Only - Actual Units",
                CALCULATE([BPC Only - Actual Units])
        )

EVALUATE
    __DS0Core

ORDER BY
    'Calendar'[Month Sort],
    'Calendar'[Month Year],
    'BPC All'[Country],
    'BPC All'[Material ID],
    'BPC All'[PH1Desc]
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