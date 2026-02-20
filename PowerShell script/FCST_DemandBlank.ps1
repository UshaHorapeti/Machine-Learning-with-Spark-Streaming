#Load External DLL and Add Types
$assemblyPath = "C:\DP Waterfall Automation\src\Microsoft.AnalysisServices.AdomdClient.dll"
$adomd_obj = Add-Type -Path $assemblyPath -PassThru
$Connection_class = $adomd_obj | Where-Object {$_.FullName -eq 'Microsoft.AnalysisServices.AdomdClient.AdomdConnection'}
$Adapter_class = $adomd_obj | Where-Object {$_.FullName -eq 'Microsoft.AnalysisServices.AdomdClient.AdomdDataAdapter'}

$PowerBILogin = "ISCBI001@bd.com"
$PowerBIPassword = "52SQ6I>*P+*63*Z"
$PowerBIEndpoint = "powerbi://api.powerbi.com/v1.0/myorg/E2E%20Supply%20Chain%20Excellence"
$reportName = "Demand Reconciliation"

$RunDate = Get-Date
$SaveDate = $RunDate.ToString("yyyyMMdd")

$FileName = $SaveDate + "_FcstDB.csv"
$ExportPath = "F:\DP Waterfall\" + $FileName

Write-Output "Exporting File: $FileName , ReportDate = $SaveDate"

$Query = @"
DEFINE
    VAR vEnd   = YEAR ( TODAY() ) * 100 + MONTH ( TODAY() )

EVALUATE
VAR BaseFiltered =
    CALCULATETABLE (
        'SIOP',
        KEEPFILTERS ( ISBLANK ( 'SIOP'[Country] ) || 'SIOP'[Country] = "" ),
        KEEPFILTERS ( 'SIOP'[Cycle] = "Current" ),
        KEEPFILTERS ( 'SIOP'[ReltioBU] IN { "BDB","DS","MDS","MMS","PI","PS","SM","SUR","UCC" } ),
        KEEPFILTERS ( FILTER ( 'Calendar', 'Calendar'[Month Sort] >= vEnd ) )
    )


VAR ResultTable =
    ADDCOLUMNS (
        SUMMARIZE (
            BaseFiltered,
            'SIOP'[Planning System],
            'SIOP'[Material ID],
            'SIOP'[Sales Organization],
            'SIOP'[ReltioBU],
            'SIOP'[Region],
            'SIOP'[Sub Region],
            'SIOP'[Country],
            'SIOP'[Mapped Country],
            'Calendar'[Month Year],
            'Calendar'[Month Sort]
        ),
        "SIOP f/Planning", CALCULATE ( SUM ( 'SIOP'[SIOP Consensus] ) )
    )


RETURN
FILTER(
        ADDCOLUMNS ( ResultTable,
            "Snapshot", DATE ( YEAR ( TODAY() ), MONTH ( TODAY() ), 1 )
        ),
        [SIOP f/Planning] <> 0
    )

ORDER BY 'Calendar'[Month Sort]
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