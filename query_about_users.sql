SELECT   [USR_ID]
      ,[USR_XPFL]
      ,[USR_LOGIN]
      ,[USR_SNAME]
      ,[USR_FNAME]
      ,'***' as USR_PWD
      ,[USR_PWDINIT]
      ,[USR_PWDDATE]
      ,[USR_ACTIVE]
      ,[USR_LANGAGE]
      ,[USR_VIRTUALPATH]
      ,[USR_PHYSICALPATH]
      ,[USR_ISUSER]
      ,[USR_ISADMIN]
      ,[USR_EMAIL]
      ,[USR_ISFUNCTIONAL]
	  ,[PFL_CODE] as "profil code"
      ,[PFL_LABEL] as "profil name"
	  ,pc.cpy_codes as "profil subsidiaries code"
	  ,pc.cpy_names as "profil subsidiaries name"
	  ,ccm.[controler on cpy]
	  ,ccm.[controler on rpt]
	  ,ccm.[supervisor on cpy]
	  ,ccm.[supervisor on rpt]
  FROM [e2sGeneral].[dbo].[USERREF] 
	inner join  [e2sGeneral].[dbo].[PROFIL] on [USR_XPFL]=[PFL_ID]
	inner join (
SELECT [PCP_XPFL],
	STRING_AGG([PCP_XCPY],', ') Within group (order by [PCP_XCPY]) cpy_codes,
	STRING_AGG(REPLACE(REPLACE(REPLACE([CPY_LABEL],'bMx','BMX'),'Biomerieux','BMX'),'bioMérieux','BMX'),', ') Within group (order by [PCP_XCPY]) cpy_names
  FROM [e2sGeneral].[dbo].[PFLCPY] inner join [e2sGeneral].[dbo].COMPANY on [PCP_XCPY]=[CPY_CODE]
  group by pcp_xpfl ) pc on pc.PCP_XPFL = [USR_XPFL]
	inner join (
select ccm1.[CUS_XUSR]
,STRING_AGG(case when ccm1.[CUS_XPFC] = 1 then ccm1.CCM_XCPY else null end,', ') "supervisor on cpy"
,STRING_AGG(case when ccm1.[CUS_XPFC] = 2 then ccm1.CCM_XCPY else null end,', ') "controler on cpy"
,STRING_AGG(case when ccm1.[CUS_XPFC] = 1 then ccm2.CCM_XRPT else null end,', ') "supervisor on rpt"
,STRING_AGG(case when ccm1.[CUS_XPFC] = 2 then ccm2.CCM_XRPT else null end,', ') "controler on rpt"
from (
SELECT distinct [CUS_XUSR]
      ,[CUS_XPFC]
	  ,cr.CCM_XCPY
  FROM [e2sGeneral].[Ccm].[USERS] u 
	inner join [e2sGeneral].[Ccm].CCMRULE cr on u.CUS_XCCM = cr.CCM_ID
	) ccm1 inner join (
SELECT distinct [CUS_XUSR]
      ,[CUS_XPFC]
	  ,cr.CCM_XRPT
  FROM [e2sGeneral].[Ccm].[USERS] u 
	inner join [e2sGeneral].[Ccm].CCMRULE cr on u.CUS_XCCM = cr.CCM_ID	
	) ccm2  on ccm1.CUS_XUSR=ccm2.CUS_XUSR and ccm1.cus_xpfc=ccm2.CUS_XPFC  
group by ccm1.cus_xusr,ccm1.CUS_XPFC
) ccm on ccm.CUS_XUSR = [USR_ID]
