SELECT  m.[Schema],m.[Tablename],m.[compagny]
    ,m.[CCS_ID]
    ,m.[CCS_KEY]+' :' CCS_KEY
    ,m.[CCS_CCM_CODE]
    ,m.[CCS_CCM_NAME]
	,r.[CCM_MONITORING_NAME]
    ,m.[CCS_USR_LOGIN]
    ,m.[CCS_USR_NAME]
    ,FORMAT(m.[CCS_DATE], 'yyyy-MM-dd hh:mm:ss') as CCS_DATE
    ,m.[CCS_STATUS]
	,sl.CSA_STATUS 
    ,m.[CCS_COMMENT]
    ,m.[CCS_FILE]
FROM  [#MONITOR_CCS_STATUS] m 
left join [e2sGeneral].[Ccm].CCMRULE r on m.[schema]=r.CCM_XCPY and m.ccs_ccm_code = r.CCM_CODE and m.ccs_ccm_name = r.ccm_name 
left join [e2sGeneral].[Ccm].[STATUS] sl on m.CCS_STATUS = sl.CSA_ID 
where not ( m.ccs_usr_login = '' and m.ccs_status = 1 )
