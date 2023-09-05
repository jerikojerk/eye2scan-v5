$INI_DB_config_path = 'D:\Eye2Scan\Web\connections.config'
$INI_CCM_FOLDERNAME = 'Ccm'
$INI_DB_FISCAL_PREFIX = 'e2sF'
$INI_FISCAL_YEARS_LIST = @(2021,2022,2023)
$INI_CORRECT_REPOSITORY = $false 
$INI_OUTPUT_PATH ='D:\eye2scan\BMX_EYE2SCAN_PROD'
$INI_OUTPUT_REPORT_ALL = '{0}\report_ccm_all_input_{1}.csv' 
$INI_OUTPUT_REPORT_LAST = '{0}\report_ccm_last_status_{1}.csv' 
$INI_OUTPUT_RAW_IMPORTED_TABLES = '{0}\report_imported_raw_tables_{1}.csv' 
$INI_OUTPUT_REPORT_CONFIG = '{0}\report_import_configuration.csv' 
$INI_OUTPUT_REPORT_USERS = '{0}\report_users_configuration.csv' 
$INI_OUTPUT_ERROR_MESSAGES = '{0}\report_error_message.csv'  
$INI_OUTPUT_WHAREHOUSE_RAW = '{0}\dump_e2s_{1}.csv'  
$INI_OUTPUT_FLAG ='{0}\refresh_on_going' -f $INI_OUTPUT_PATH

$INI_SQL_QUERIES = [System.IO.Path]::GetDirectoryName($MyInvocation.MyCommand.Path)


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


#https://stackoverflow.com/questions/30375519/executereader-in-powershell-script
function execute-sqlselectquery1 ([System.Data.SqlClient.SqlConnection]$SqlConnection,[string] $SqlStatement, [hashtable]$sqlparameters, [string] $path )
{
    $ErrorActionPreference = "Stop"
    
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


function execute-sqlselectquery2( [System.Data.SqlClient.SqlCommand]$SqlCmd, [string]$sqlText , [hashtable]$sqlparameters ) {

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
        
        if ( $active_table.FULLNAME -like '*T_FI02_CCS*' ){
            Write-Output "bug?"
        }

        #write-host -ForegroundColor Yellow $sql  
        #write-host -ForegroundColor Yellow $sqlCmd
        try {
            $r=$sqlCmd.ExecuteNonQuery()
            Write-Output "staged $($active_table.FULLNAME) with $r records over $($active_table.ROWS_COUNT) found."
        }catch {
            $Error = $_.Exception.Message
            Write-Error $Error
            write-error "issue during $($active_table.FULLNAME) setup"
        }
    } #each table
    # remove-variable $sqlCmd
}

function perform-onequery-report([System.Data.SqlClient.SqlConnection]$SqlConnection,[string]$title,[string]$sql,[hashtable]$param,[string]$path){

    Write-Output $title 
    Write-Output "   ->$path"
    try {
        execute-sqlselectquery1 $SqlConnection $sql $param $path 
#        $all_results = execute-sqlselectquery2 $SqlCmd $sql $param 
#		export $all_results $path 
    }catch {
        $Error = $_.Exception.Message
        Write-Error $Error
    }
}

function report_fiscal_year ( $current_year,[System.Data.SqlClient.SqlConnection]$SqlConnection ){

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


}


function dump_e2sWarehouse([System.Data.SqlClient.SqlConnection]$SqlConnection) {
    #create report table
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection

    $SqlCmd.CommandText = 'SET ANSI_NULLS ON'
    $SqlCmd.ExecuteNonQuery() | out-null 

    $SqlCmd.CommandText = 'SET QUOTED_IDENTIFIER ON'
    $SqlCmd.ExecuteNonQuery() | out-null 
    
    $sql = read-file-query "query_e2sWarehouse_all_tables.sql"
    $sqlTemplate = read-file-query 'query_select_start.sql'

    $intermediate = execute-sqlselectquery2 $SqlCmd $sql @{}


    $intermediate.GetEnumerator() | ForEach-Object {
        $active_table = $_.value
        
        $title="Dumping table {0} " -f $active_table.express
        
        $sql = $sqlTemplate -f $active_table.express
        #$sqlCmd.CommandText = $sql

        $tmp = $active_table.DatabaseName+'-'+$active_table.SchemaName+'-'+$active_table.TableName
        $path = $INI_OUTPUT_WHAREHOUSE_RAW -f $INI_OUTPUT_PATH,$tmp 
        
        Write-Output $title 
        execute-sqlselectquery1 $SqlConnection $sql @{} $path

    }#getEnumerator

}


function main (){
    $sqlconnection = establish-connexion

	#retrieve-fiscalyear $sqlconnection
	Write-Output "Current activated year: $INI_FISCAL_YEARS_LIST"

    #put a flag
    'go' | Out-File $INI_OUTPUT_FLAG -Force

    #create report table
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection

    $SqlCmd.CommandText = 'SET ANSI_NULLS ON'
    $SqlCmd.ExecuteNonQuery() | out-null 

    $SqlCmd.CommandText = 'SET QUOTED_IDENTIFIER ON'
    $SqlCmd.ExecuteNonQuery() | out-null 
    
    $sqlCmd.CommandText = @"
CREATE TABLE [e2sGeneral].[Log].[#MONITOR_CCS_STATUS](
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


    $INI_FISCAL_YEARS_LIST |ForEach-Object {
#        report_fiscal_year $_  $SqlConnection
    }


<#

    # other file 
    $title ="query report configuration"
    $sql = read-file-query "query_report_configuration.sql"
    $path=$INI_OUTPUT_REPORT_CONFIG  -f $INI_OUTPUT_PATH
 	$param = @{}
	perform-onequery-report $SqlConnection $title $sql $param $path 


    # other file 
    $title="retrieve error logs"
    $sql = read-file-query "query_retrieve_error_logs.sql"
	$path = $INI_OUTPUT_ERROR_MESSAGES  -f $INI_OUTPUT_PATH
 	$param = @{}
	perform-onequery-report $SqlConnection $title $sql $param $path 


    # other file 
    $title="About users"
	$path = $INI_OUTPUT_REPORT_USERS -f $INI_OUTPUT_PATH
 	$param = @{}
    $sql = read-file-query 'query_about_users.sql'
	perform-onequery-report $SqlConnection $title $sql $param $path 


*#>

    # other file 
    $title ="dump [e2sF2022].[0042].V_BSEG"
    $sql = "select * from [e2sF2022].[0042].V_BSEG"
    $path=$INI_OUTPUT_WHAREHOUSE_RAW   -f $INI_OUTPUT_PATH, "2022-0042-V_BSEG"
 	$param = @{}
#	perform-onequery-report $SqlConnection $title $sql $param $path 


   # other file 
    $title ="dump [e2sF2022].[0042].T_FI00"
    $sql =  "select * from [e2sF2022].[0042].T_FI00" 
    $path=$INI_OUTPUT_WHAREHOUSE_RAW   -f $INI_OUTPUT_PATH,"2022-0042-T_FI00"
 	$param = @{}
#	perform-onequery-report $SqlConnection $title $sql $param $path 


#    dump_e2sWarehouse $SqlConnection



	Write-Output "Ending script"

    $sqlconnection.close() | Out-Null


    Remove-Item -Path $INI_OUTPUT_FLAG 


    Write-Output "see you soon"
}

main


exit 0 