param(
    [Parameter(Position=0,mandatory=$true)][string][ValidateSet('explicit','all','negate')] $mode , #= 'explicit'
    [switch] $dump_warehouse,
    [switch] $dump_master,
    [switch] $v5_report_configuration,
    [switch] $v5_report_fiscal_year,
    [switch] $v5_about_users,
    [switch] $v5_special,
    [switch] $archive
 
)


$INI_DB_config_path = 'D:\Eye2Scan\Web\connections.config'
$INI_CCM_FOLDERNAME = 'Ccm'
$INI_DB_FISCAL_PREFIX = 'e2sF'
$INI_FISCAL_YEARS_LIST = @(2021,2022,2023)
$INI_CORRECT_REPOSITORY = $false 
$INI_OUTPUT_PATH ='D:\eye2scan\SHARED'
$INI_OUTPUT_REPORT_ALL = '{0}\report_ccm_all_input_{1}.csv' 
$INI_OUTPUT_REPORT_LAST = '{0}\report_ccm_last_status_{1}.csv' 
$INI_OUTPUT_RAW_IMPORTED_TABLES = '{0}\report_imported_raw_tables_{1}.csv' 
$INI_OUTPUT_REPORT_CONFIG = '{0}\report_import_configuration.csv' 
$INI_OUTPUT_REPORT_USERS = '{0}\report_users_configuration.csv' 
$INI_OUTPUT_ERROR_MESSAGES = '{0}\report_error_message.csv'  
$INI_OUTPUT_DUMP_RAW = '{0}\dump_e2s_{1}.csv'
$INI_OUTPUT_FLAG ='{0}\refresh_on_going' -f $INI_OUTPUT_PATH
$INI_SQL_QUERIES = [System.IO.Path]::GetDirectoryName($MyInvocation.MyCommand.Path)
$INI_COMPRESSION_LEVEL = 'Fastest'  # Optimal

$RUNTIME_CHOICE=@{
    'report_fiscal_year_v5'         = $v5_report_fiscal_year
    "query report configuration"    = $v5_report_configuration
    "retrieve error logs"           = $report_configuration
    "About users"                   = $v5_about_users
    "dump [e2sF2022].[0042].V_BSEG" = $v5_special
    "dump [e2sF2022].[0042].T_FI00" = $v5_special
    "dump [e2sF2022].[dbo]_MM03 (CDPOS)" = $v5_special
    'dump_e2sMaster'                = $dump_master
    'dump_e2sWarehouse'             = $dump_warehouse
    'archive'                       =  $archive
}


function action-or-skip ([string] $query ){
    if ( $mode -eq 'all' ){
        return $true 
    }
    if ( $mode -eq 'negate' ) {
        if ( $RUNTIME_CHOICE.Contains($query) ){
            return -not($RUNTIME_CHOICE[$query])
        }
        else{
            return $true
        }
    }
    if ( $RUNTIME_CHOICE.Contains($query) ){
        return $RUNTIME_CHOICE[$query]
    }
    else{
        return $false 
    }
}

function archive-or-remove( [string]$path ){
    if ( ($RUNTIME_CHOICE['archive']) -and ( Test-Path $path ) ){
        $tmp= get-item $path 
        $ext=$tmp.LastWriteTime.ToString('yyyyMMddHHmmss')
        $dst =$tmp.DirectoryName+'\'+$tmp.BaseName+'-'+$ext+$tmp.Extension
        Move-Item -Verbose -Path $path -Destination $dst     
    }
}

function read-file-query ([string] $filename) {
    return Get-Content (join-path  $INI_SQL_QUERIES  $filename )
}

function establish-connexion(){
    try{
        #pas de xpath, on se contente de convertir le xml en object powershell et ensuite on parcours à la mano & en dur.
        [xml]$config_xml = Get-Content -path $INI_DB_config_path
        $connection_string = $config_xml.connectionStrings.add.Attributes['connectionString'].value
    }catch{
        $Error = $_.Exception.Message 
        $msg="Can not retrieve connexion file and connection_string, giving up"
        Write-Output $msg
        Write-Error $msg 
        Write-Error $Error 
        exit 1 
    }


    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    try{
        $SqlConnection.ConnectionString = $connection_string 
        $SqlConnection.Open();
    }catch{
        $Error = $_.Exception.Message 
        $msg="Can not retrieve open database, giving up"
        Write-Output $msg
        Write-Error $msg 
        Write-Error $Error 
        exit 1
    }
    return $SqlConnection 
}

function execute-sqlselectquery1 ([System.Data.SqlClient.SqlConnection]$SqlConnection,[string] $SqlStatement, [hashtable]$sqlparameters, [string] $path ){
    $ErrorActionPreference = "Stop"
    
    archive-or-remove $path 

    $sqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $sqlCmd.Connection = $sqlConnection
    $sqlCmd.CommandText = $SqlStatement
   
    foreach ($h in $sqlparameters.GetEnumerator()) {
        $SqlCmd.parameters.AddWithValue( "@"+$h.Name, $h.value) | out-null 
    }

    $sqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $sqlAdapter.SelectCommand = $sqlCmd
    $data = New-Object System.Data.DataSet
    try
    {
        $sqlAdapter.Fill($data) 
        $data.Tables[0] | ConvertTo-Csv  -NoTypeInformation | Out-File -FilePath "$path" -Encoding default
    }
    catch
    {
        $Error = $_.Exception.Message
        Write-Error $Error
        Write-Error -Verbose "Error executing SQL on database [$Database] on server [$SqlServer]. Statement: `r`n$SqlStatement"
    }
    finally {
       # $sqlAdapter.Dispose()
    }
    #
    #if ($dataTable) { return ,$dataTable } else { return $null }
}

function execute-sqlselectquery2( [System.Data.SqlClient.SqlCommand]$SqlCmd, [string]$sqlText , [hashtable]$sqlparameters ){
    foreach ($h in $sqlparameters.GetEnumerator()) {
        $SqlCmd.parameters.AddWithValue( "@"+$h.Name, $h.value) | out-null 
    }

    if ( [String]::IsNullOrEmpty($sqlText) -eq $false ){
        $SqlCmd.CommandText = $sqlText
    }

    $dataReader =  $SqlCmd.ExecuteReader()
    $resulttype=$dataReader.GetType().Name
    $result= @{}

    if ( $resulttype -eq 'SqlDataReader' ) {
        $columns_name = New-Object Collections.Generic.List[string]
        
        $line=0
        for($i=0;$i -lt $dataReader.VisibleFieldCount;$i++){
            $columns_name.add($dataReader.GetName($i) ) | out-null 
        }
        while( $dataReader.Read() ){
            $row = New-object psobject 
            foreach($col in $columns_name){
                add-member -InputObject $row -MemberType NoteProperty -Name $col -Value $dataReader[$col]|out-null 
            }
            $result.add($line,$row)|out-null
            $line++
        }#while

    }else{
        Write-Error -Category InvalidType 'looks like a bug, sorry'
    }

    $dataReader.close() 
    return $result  

}

function export ([hashtable] $t, $file ){

    archive-or-remove $file 

    if ( $t -eq $null -or $t.Count -eq 0 ){
        Write-Output "   -> no results"
        "" | Out-File $file -Encoding default
        return
    }
    $t[0] | ConvertTo-Csv -NoTypeInformation -Delimiter ";" |  Select-Object -First 1 | Out-File $file -Encoding default 
    $t.GetEnumerator() | ForEach-Object {
        $_.Value | ConvertTo-Csv -NoTypeInformation -Delimiter ";"   | Select-Object -Skip 1    | Out-File -Append $file -Encoding default 
    } 
    
}
 
function fill-temporarytable([hashtable] $tableslist,[System.Data.SqlClient.SqlConnection]$SqlConnection){
    if ( $tableslist -eq $null -or $tableslist.count -eq 0 ) {
        Write-Warning "No table to stage"
        return 
    }

    $sqlTemplate = read-file-query 'query_insert_into_temptable.sql'

    #Write-Output $tableslist

    $tableslist.GetEnumerator() | ForEach-Object {
        $active_table = $_.value 

        $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
        $SqlCmd.Connection = $SqlConnection

        $sql = $sqlTemplate -f $active_table.FULLNAME
        $sqlCmd.CommandText = $sql
        $sqlCmd.Parameters.AddWithValue('@SCHEMANAME',$active_table.SCHEMANAME ) | Out-Null
        $sqlCmd.Parameters.AddWithValue('@FULLNAME'  ,$active_table.FULLNAME )   | Out-Null
        $sqlCmd.Parameters.AddWithValue('@LABEL'     ,$active_table.CPY_LABEL )  | Out-Null


        try {
            $r=$sqlCmd.ExecuteNonQuery()
            Write-Output "staged $($active_table.FULLNAME) with $r records over $($active_table.ROWS_COUNT) found."
        }catch {
            $Error = $_.Exception.Message
            Write-Error $Error
            write-error "issue during $($active_table.FULLNAME) setup"
        }
    } #each table
}

function perform-onequery-report([System.Data.SqlClient.SqlConnection]$SqlConnection,[string]$title,[string]$sql,[hashtable]$param,[string]$path){
    Write-Output $title 
    Write-Output "   ->$path"
    try {
        execute-sqlselectquery1 $SqlConnection $sql $param $path 
#       $all_results = execute-sqlselectquery2 $SqlCmd $sql $param 
#		export $all_results $path 
    }catch {
        $Error = $_.Exception.Message
        Write-Error $Error
    }
}

function report_fiscal_year ( $current_year,[System.Data.SqlClient.SqlConnection]$SqlConnection ){

    #create report table
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection

    $SqlCmd.CommandText = 'SET ANSI_NULLS ON'
    $SqlCmd.ExecuteNonQuery() | out-null 

    $SqlCmd.CommandText = 'SET QUOTED_IDENTIFIER ON'
    $SqlCmd.ExecuteNonQuery() | out-null 
    
    $sqlCmd.CommandText = @"
CREATE TABLE [#MONITOR_CCS_STATUS](
	[Schema] [varchar](10) NULL,
	[Tablename] [varchar](96) NOT NULL,
	[compagny] [varchar](100) NULL,
	[CCS_ID] [int] NOT NULL,
	[CCS_KEY] [varchar](2550) NULL,
	[CCS_CCM_CODE] [varchar](10) NULL,
	[CCS_CCM_NAME] [varchar](200) NULL,
	[CCS_USR_LOGIN] [varchar](100) NULL,
	[CCS_USR_NAME] [varchar](200) NULL,
	[CCS_DATE] [datetime] NOT NULL,
	[CCS_STATUS] [int] NOT NULL,
	[CCS_COMMENT] [varchar](max) NULL,
	[CCS_FILE] [nvarchar](max) NULL,
	CONSTRAINT PK_TABLEID PRIMARY KEY (Tablename,ccs_id)		
)
"@

    $SqlCmd.ExecuteNonQuery() | out-null 


    Write-Output "---- Working on $current_year -----"

    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection
    $tmp = read-file-query 'query_list_tables.sql'
    $sql = $tmp -f $INI_DB_FISCAL_PREFIX,$current_year
    
    $tableslist = execute-sqlselectquery2 $SqlCmd $sql @{'UTYPE'='U';'PATTERN'='T_%_CCS'}
	Write-Output "found $($tableslist.count) relevant tables"
    fill-temporarytable $tableslist $SqlConnection 


    #all actions 
    $title= "query all ccm actions {0}" -f $current_year
    $sql = read-file-query 'query_ccm_all_actions.sql' 
    $path = $INI_OUTPUT_REPORT_ALL -f $INI_OUTPUT_PATH,$current_year 
	$param=@{}
	perform-onequery-report $SqlConnection $title $sql $param $path 


    #latest status
    $title= "query ccm last status {0}" -f $current_year
    $sql = read-file-query 'query_ccm_last_status.sql'
    $path=$INI_OUTPUT_REPORT_LAST -f $INI_OUTPUT_PATH,$current_year 
 	$param = @{}	
	perform-onequery-report $SqlConnection $title $sql $param $path 


   # other file 
    $title="query raw imported data {0}" -f $current_year
    $tmp = read-file-query 'query_raw_imported_data.sql'
    $sql =$tmp -f $INI_DB_FISCAL_PREFIX,$current_year
	$path = $INI_OUTPUT_RAW_IMPORTED_TABLES	-f $INI_OUTPUT_PATH,$current_year 
 	$param = @{}
	perform-onequery-report $SqlConnection $title $sql $param $path 

    Write-Output "---- End of work on $current_year -----"



    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection
    $sqlCmd.CommandText = @"
DELETE FROM [#MONITOR_CCS_STATUS]
"@
    $SqlCmd.ExecuteNonQuery() | out-null 

    $sqlCmd.CommandText = @"
DROP TABLE [#MONITOR_CCS_STATUS]
"@
    $SqlCmd.ExecuteNonQuery() | out-null 

}

function dump_e2sWarehouse([System.Data.SqlClient.SqlConnection]$SqlConnection) {
    dump_e2sDatabase $SqlConnection 'query_e2sWarehouse_all_tables.sql' 'e2sWarehouse_all_tables'
}

function dump_e2sMaster([System.Data.SqlClient.SqlConnection]$SqlConnection) {
    dump_e2sDatabase $SqlConnection 'query_e2sMaster_some_tables.sql' 'e2sMaster_some_tables'
}

function dump_e2sDatabase([System.Data.SqlClient.SqlConnection]$SqlConnection, $sql_script,[string] $zipname = '' ) {
    #create report table
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection

    $SqlCmd.CommandText = 'SET ANSI_NULLS ON'
    $SqlCmd.ExecuteNonQuery() | out-null 

    $SqlCmd.CommandText = 'SET QUOTED_IDENTIFIER ON'
    $SqlCmd.ExecuteNonQuery() | out-null 
    
    $sql = read-file-query $sql_script
    $sqlTemplate = read-file-query 'query_select_start.sql'

    $intermediate = execute-sqlselectquery2 $SqlCmd $sql @{}

    Write-Output "Found $($intermediate.count) items"
    if ( [string]::IsNullOrEmpty($zipname) ){
        Write-Output "Dumped item will be zipped "
        $do_zip=$false
    }else{
        Write-Output "Dumped item will not be zipped "
        $zip_fqln = $INI_OUTPUT_DUMP_RAW -f $INI_OUTPUT_PATH,$zipname
        $zip_fqln = $zip_fqln + '.zip'
        $do_zip=$true
        archive-or-remove $zip_fqln 
        if ( Test-Path $zip_fqln ){
            Remove-Item -Verbose $zip_fqln
        }
    }

    $intermediate.GetEnumerator() | ForEach-Object {
        $active_table = $_.value
        
        $title="Dumping table {0} " -f $active_table.express
        
        $sql = $sqlTemplate -f $active_table.express
        #$sqlCmd.CommandText = $sql

        $tmp = $active_table.DatabaseName+'-'+$active_table.SchemaName+'-'+$active_table.TableName
        $path = $INI_OUTPUT_DUMP_RAW -f $INI_OUTPUT_PATH,$tmp 

        Write-Output $title 
        execute-sqlselectquery1 $SqlConnection $sql @{} $path

        #manage zip file immediatily
        if ( $do_zip ) {
            move-tozip  $path $zip_fqln 
        }
    }#getEnumerator
}

function move-tozip ([string] $path ,[string] $zip_fqln ){
    if ( Test-Path -Path $zip_fqln -PathType leaf )  {
        Write-Output "Add $path to $zip_fqln"
        Compress-Archive -Path $path -DestinationPath $zip_fqln -Update -CompressionLevel $INI_COMPRESSION_LEVEL
    }else{
        Write-Output "Create $path to $zip_fqln"
        Compress-Archive -Path $path  -DestinationPath $zip_fqln -CompressionLevel  $INI_COMPRESSION_LEVEL
    }
    Remove-Item -LiteralPath $path -Recurse 
}

function main (){
    $sqlconnection = establish-connexion

	#retrieve-fiscalyear $sqlconnection
	Write-Output "Current activated year: $INI_FISCAL_YEARS_LIST"

    #put a flag
    'go' | Out-File $INI_OUTPUT_FLAG -Force

    #V5 queries 
    $INI_FISCAL_YEARS_LIST |ForEach-Object {
        if ( action-or-skip 'report_fiscal_year_v5' ) {
            report_fiscal_year $_  $SqlConnection
        }
    }

    # other file 
    $title ="query report configuration"
    if (action-or-skip $title){
        $path=$INI_OUTPUT_REPORT_CONFIG  -f $INI_OUTPUT_PATH
 	    $param = @{} 
        $sql = read-file-query "query_report_configuration.sql"
  	    perform-onequery-report $SqlConnection $title $sql $param $path 
    }

    # other file 
    $title="retrieve error logs"
    if  (action-or-skip $title){
        $sql = read-file-query "query_retrieve_error_logs.sql"
	    $path = $INI_OUTPUT_ERROR_MESSAGES  -f $INI_OUTPUT_PATH
 	    $param = @{}
    	perform-onequery-report $SqlConnection $title $sql $param $path 
    }

    # other file 
    $title="About users"
    if  (action-or-skip $title){
	    $path = $INI_OUTPUT_REPORT_USERS -f $INI_OUTPUT_PATH
 	    $param = @{}
        $sql = read-file-query 'query_about_users.sql'
	    perform-onequery-report $SqlConnection $title $sql $param $path 
    }


    # other file 
    $title ="dump [e2sF2022].[0042].V_BSEG"
    if  (action-or-skip $title){
        $sql = "select * from [e2sF2022].[0042].V_BSEG"
        $path=$INI_OUTPUT_DUMP_RAW   -f $INI_OUTPUT_PATH, "2022-0042-V_BSEG"
 	    $param = @{}
    	perform-onequery-report $SqlConnection $title $sql $param $path 
    }


   # other file 
    $title ="dump [e2sF2022].[0042].T_FI00"
    if  (action-or-skip $title){
        $sql =  "select * from [e2sF2022].[0042].T_FI00" 
        $path=$INI_OUTPUT_DUMP_RAW   -f $INI_OUTPUT_PATH,"2022-0042-T_FI00"
 	    $param = @{}
    	perform-onequery-report $SqlConnection $title $sql $param $path 
    }


    $title ="dump [e2sF2022].[dbo]_MM03 (CDPOS)"
    if  (action-or-skip $title){
        $sql =  "select * from [e2sF2022].dbo.MM03 where CDPOS_TABNAME=@value " 
        $path=$INI_OUTPUT_DUMP_RAW   -f $INI_OUTPUT_PATH,"2022- CDPOS issue"
 	    $param = @{'value'='LFBK'}
	    perform-onequery-report $SqlConnection $title $sql $param $path 
    }

    #V6 queries
    $title = 'dump_e2sWarehouse'
    if  (action-or-skip $title){
        dump_e2sWarehouse $SqlConnection
    }

    $title = 'dump_e2sMaster'
    if  (action-or-skip $title){
        dump_e2sMaster $SqlConnection
    }

	Write-Output "Ending script"

    $sqlconnection.close() | Out-Null


    Remove-Item -Path $INI_OUTPUT_FLAG 


    Write-Output "see you soon"
}

main


exit 0 
