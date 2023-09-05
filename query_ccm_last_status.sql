SELECT m.[Schema],m.[Tablename],m.[compagny]
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
	  ,m.CURRENT_RANK 
	  , HASHBYTES('SHA1', m.[Tablename]+':'+m.[CCS_KEY]+':'+m.[CCS_CCM_NAME]) as MYHASH
  FROM
   ( select
 	m0.[Schema],m0.[Tablename],m0.[compagny]
      ,m0.[CCS_ID]
      ,m0.[CCS_KEY]
      ,m0.[CCS_CCM_CODE]
      ,m0.[CCS_CCM_NAME]
	  ,m0.[CCS_USR_LOGIN]
      ,m0.[CCS_USR_NAME]
	  ,m0.[CCS_STATUS]
	  ,m0.[CCS_COMMENT]
	  ,m0.[CCS_DATE]
	  ,Rank() Over (partition by m0.[Tablename], m0.[CCS_KEY],m0.[CCS_CCM_CODE],m0.[CCS_CCM_NAME] Order by m0.[CCS_DATE] desc) CURRENT_RANK 
  from	[e2sGeneral].[Log].[#MONITOR_CCS_STATUS] m0  ) m 
	left join [e2sGeneral].Ccm.CCMRULE r on m.[schema]=r.CCM_XCPY and m.ccs_ccm_code = r.CCM_CODE and m.ccs_ccm_name = r.ccm_name 
	left join [e2sGeneral].[Ccm].[STATUS] sl on m.CCS_STATUS = sl.CSA_ID 
	where not ( ccs_usr_login = '' and ccs_status = 1 ) and m.CURRENT_RANK = 1
	