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

$FileName = $SaveDate + "_FcstUnknown.csv"
$ExportPath = "F:\DP Waterfall\" + $FileName

Write-Output "Exporting File: $FileName , ReportDate = $SaveDate"

$Query = @"

DEFINE
    VAR LatestSnapshot =
        CALCULATE(
            MAX('SIOP'[Snapshot Date]),
            ALL('SIOP'[Snapshot Date])
        )

    VAR CurrentMonthStart =
        DATE(YEAR(TODAY()), MONTH(TODAY()), 1)

    VAR __DS0FilterTable =
        TREATAS({ LatestSnapshot }, 'SIOP'[Snapshot Date])

    VAR __DS0FilterTable2 =
        FILTER(
            KEEPFILTERS(VALUES('SIOP'[SIOP Consensus])),
            AND('SIOP'[SIOP Consensus] <> 0, NOT(ISBLANK('SIOP'[SIOP Consensus])))
        )

    VAR __DS0FilterTable3 =
        TREATAS(
            CALCULATETABLE(
                VALUES('Calendar'[Month Year]),
                FILTER(
                    ALL('Calendar'),
                    'Calendar'[Month Start] >= CurrentMonthStart
                )
            ),
            'Calendar'[Month Year]
        )

    VAR __DS0FilterTable4 =
        TREATAS(
            { 2025, 2026, 2027, 2028, 2029 },
            'Calendar'[Fiscal Year]
        )

    VAR __DS0FilterTable5 =
        TREATAS({ "Unknown" }, 'SIOP'[Segment])

    VAR __DS0Core =
        SELECTCOLUMNS(
            KEEPFILTERS(
                FILTER(
                    KEEPFILTERS(
                        SUMMARIZECOLUMNS(
                            'SIOP'[Country],
                            'SIOP'[Material ID Harmonized],
                            'SIOP'[Sales Organization],
                            'SIOP'[Planning System],
                            'SIOP'[Snapshot Date],
                            'SIOP'[ReltioBU],
                            'SIOP'[SIOP Consensus],
                            'Calendar'[Month Year],
                            'Calendar'[Month Sort],
                            __DS0FilterTable,
                            __DS0FilterTable2,
                            __DS0FilterTable3,
                            __DS0FilterTable4,
                            __DS0FilterTable5,
                            "CountRowsSIOP", COUNTROWS('SIOP')
                        )
                    ),
                    OR(
                        OR(
                            OR(
                                OR(
                                    OR(
                                        OR(
                                            OR(
                                                OR(
                                                    NOT(ISBLANK('SIOP'[Country])),
                                                    NOT(ISBLANK('SIOP'[Material ID Harmonized]))
                                                ),
                                                NOT(ISBLANK('SIOP'[Sales Organization]))
                                            ),
                                            NOT(ISBLANK('SIOP'[Planning System]))
                                        ),
                                        NOT(ISBLANK('SIOP'[Snapshot Date]))
                                    ),
                                    NOT(ISBLANK('SIOP'[ReltioBU]))
                                ),
                                NOT(ISBLANK('SIOP'[SIOP Consensus]))
                            ),
                            NOT(ISBLANK('Calendar'[Month Year]))
                        ),
                        NOT(ISBLANK('Calendar'[Month Sort]))
                    )
                )
            ),
            "'SIOP'[Country]", 'SIOP'[Country],
            "'SIOP'[Material ID Harmonized]", 'SIOP'[Material ID Harmonized],
            "'SIOP'[Sales Organization]", 'SIOP'[Sales Organization],
            "'SIOP'[Planning System]", 'SIOP'[Planning System],
            "'SIOP'[Snapshot Date]", 'SIOP'[Snapshot Date],
            "'SIOP'[ReltioBU]", 'SIOP'[ReltioBU],
            "'SIOP'[SIOP Consensus]", 'SIOP'[SIOP Consensus],
            "'Calendar'[Month Year]", 'Calendar'[Month Year],
            "'Calendar'[Month Sort]", 'Calendar'[Month Sort]
        )

EVALUATE
    __DS0Core

ORDER BY
    'SIOP'[SIOP Consensus] DESC,
    'SIOP'[Country],
    'SIOP'[Material ID Harmonized],
    'SIOP'[Sales Organization],
    'SIOP'[Planning System],
    'SIOP'[Snapshot Date],
    'SIOP'[ReltioBU],
    'Calendar'[Month Sort],
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