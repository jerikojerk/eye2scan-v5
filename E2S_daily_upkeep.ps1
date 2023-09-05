#author Jerikokerk  @github.

$DB_retention_months=13 #months
$DB_config_path = 'D:\Eye2Scan\Web\connections.config'

$IIS_LogPath="C:\inetpub\logs\LogFiles\W3SVC2"
$IIS_LogAge = 100 #days
$E2S_log_root = 'D:\Eye2Scan\Log\eye2ScanWeb'
$E2S_too_early_to_compress = 8 #days
$E2S_too_old_to_stay = 366  #days

$Upkeep_retention = 26 #days
$Upkeep_log = 'D:\Eye2Scan\Log\E2S_daily_upkeep.{0}.log' -f (Get-Date -Format 'dd')

Get-Item -Path $Upkeep_log  | Where{$_.LastWriteTime -le (Get-Date).AddDays(-$Upkeep_retention)} | Remove-Item -Verbose

if ( Test-Path -Path $Upkeep_log ) {
    Start-Transcript $Upkeep_log -Append
 }
 else{
    Start-Transcript $Upkeep_log 
 }

function show-init {
    Write-Output "log directory $Upkeep_log"
    Write-Output "DB_retention_months= $DB_retention_months"
    Write-output "DB_config_path= $DB_config_path"
    Write-output "IIS_LogPath= $IIS_LogPath"
    Write-output "IIS_LogAge=  $IIS_LogAge"
    Write-output "E2S_log_root=$E2S_log_root"
    Write-output "E2S_too_early_to_compress=$E2S_too_early_to_compress"
    Write-output "E2S_too_old_to_stay=$E2S_too_old_to_stay"
}

function exec-sql([System.Data.SqlClient.SqlCommand]$SqlCommandOject,[string]$title,[string]$sqlText){
    try{
	    $SqlCommandOject.CommandText = $sqlText
	    $row=$SqlCommandOject.ExecuteNonQuery()
        Write-Output "$title : $row row(s) affected"
    }
    catch{
        $Error = $_.Exception.Message 
        $row=0
        Write-Output "$title : failed"
        Write-Error $Error     
    }
#    return $row 
}

function trim-dbdata { 
    try{
        #pas de xpath, on se contente de convertir le xml en object powershell et ensuite on parcours à la mano & en dur.
        [xml]$config_xml = Get-Content -path $DB_config_path
        $connection_string = $config_xml.connectionStrings.add.Attributes['connectionString'].value
    }catch{
        $Error = $_.Exception.Message 
        $msg="Can not retreive connexion file and connection_string, giving up"
        Write-Output $msg
        Write-Error $msg 
        Write-Error $Error 
        return 
    }


    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    try{
        $SqlConnection.ConnectionString = $connection_string 
        $SqlConnection.Open();
    }catch{
        $Error = $_.Exception.Message 
        $msg="Can not retreive open database, giving up"
        Write-Output $msg
        Write-Error $msg 
        Write-Error $Error 
        return
    }

    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.Connection = $SqlConnection

 
    $title="Trim archived email - [e2sGeneral].[Email].[BOX] "
    $text = 'DELETE FROM [e2sGeneral].[Email].[BOX] WHERE [EBX_CREATION] < DATEADD(MONTH, -'+$DB_retention_months+', CURRENT_TIMESTAMP) '
    exec-sql $SqlCmd $title $text 

    $title="Trim log trace - [e2sGeneral].[Log].[BUILD]      "
    $sql =  'DELETE FROM [e2sGeneral].[Log].[BUILD] WHERE [LOG_DATE] < DATEADD(MONTH,-'+$DB_retention_months+', CURRENT_TIMESTAMP) '
    exec-sql $SqlCmd $title $text 

    $title="Trim log site - [e2sGeneral].[Log].[LOG_SITE]    "
    $sql = 'DELETE FROM [e2sGeneral].[Log].[LOG_SITE] WHERE [LOG_DATE] < DATEADD(MONTH,-'+$DB_retention_months+', CURRENT_TIMESTAMP) '
    exec-sql $SqlCmd $title $text 

    $title="Trim log task - [e2sGeneral].[dbo].[TASK]        "
    $sql = 'DELETE FROM [e2sGeneral].[dbo].[TASK] WHERE [TSK_CREATE] < DATEADD(MONTH,-'+$DB_retention_months+', CURRENT_TIMESTAMP) '
    exec-sql $SqlCmd $title $text 


    $title="Trim historical - [e2sGeneral].[dbo].[HISTORICAL] "
    $sql = 'DELETE FROM [e2sGeneral].[dbo].[HISTORICAL] WHERE [LOG_DATE] < DATEADD(MONTH,-'+2*$DB_retention_months+', CURRENT_TIMESTAMP) '
    exec-sql $SqlCmd $title $text 

    $SqlConnection.Close()
}



function purge-iislog{
    $Filter = "*.log"
    $oldfiles = Get-Childitem -path:$IIS_LogPath -File -Filter:$Filter -Recurse | Where-Object {($_.LastWriteTime -lt (Get-Date).AddDays(-$IIS_LogAge))}
    $LogCount = ($oldfiles | Measure-Object).Count
    Write-Output "IIS $LogCount files to remove"
    if ( $LogCount -gt 0 ){
        $oldfiles | Remove-Item -Force -Verbose
    }}



function compress-e2slog{
    $msg="warning.directory are zipped and zip are purged.txt"
    $msg = Join-Path -Path $E2S_log_root -ChildPath $msg 
    write-output "see upkeep task" > $msg 

    $refdate=(Get-Date).AddDays(-$E2S_too_early_to_compress) 
    $dirs = Get-ChildItem $E2S_log_root -Directory | Where-Object {($_.LastAccessTime -lt $refdate)} 
    $count = ($dirs|Measure-Object).count 
    Write-Output "Found $count log directory to archive"

    $dirs | ForEach-Object {
        $zip_name = $_.BaseName.substring(0,6) + '.zip'
        $zip_fqln = Join-Path -Path $E2S_log_root -ChildPath $zip_name 
        $zipped = $_.FullName + '\*'
        if ( Test-Path -Path $zip_fqln -PathType leaf ) {
            Write-Output "Add $zipped to $zip_fqln"
            Compress-Archive -Path $zipped -DestinationPath $zip_fqln -Update -CompressionLevel Optimal -Verbose
        }else{
            Write-Output "Create $zipped to $zip_fqln"
            Compress-Archive -Path $zipped  -DestinationPath $zip_fqln -CompressionLevel Optimal -Verbose
        }

        Remove-Item -LiteralPath $_.FullName  -Recurse -Verbose

    }

    $Filter = "*.zip"
    $refdate=(Get-Date).AddDays(-$E2S_too_old_to_stay)
    $oldfiles = Get-Childitem -path:$E2S_log_root -File -Filter:$Filter -Recurse | Where-Object {($_.LastWriteTime -lt $refdate)}
    $LogCount = ($oldfiles | Measure-Object).Count
    Write-Output "Compressed log files to remove $LogCount"
    if ( $LogCount -gt 0 ){
        $oldfiles | Remove-Item -Force -Verbose
    }

}


show-init 
compress-e2slog 
trim-dbdata  
#purge-iislog  #je n'ai pas le bon user pour ça.

D:\eye2scan\Misc\E2S_report_ccm.ps1

Stop-Transcript

return 0
