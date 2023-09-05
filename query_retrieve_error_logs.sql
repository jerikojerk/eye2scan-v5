SELECT TOP (1000) B.[LOG_ID]
      ,b.[LOG_DATE]
      ,b.[LOG_MODULE]
      ,b.[LOG_MESSAGE]
      ,b.[LOG_TYPE]
	  ,h.[HTR_ID]
	  ,h.HTR_WARNING
	  ,h.HTR_MESSAGE
	  ,h.HTR_DATE_START
	  ,h.HTR_DATE_END
	  ,h.HTR_DATE_IMPORT
	  ,h.HTR_TIME_IMPORT
	  ,h.HTR_IDSYST_TARGET
	  ,h.HTR_MANDANT_TARGET
  FROM [e2sGeneral].[Log].[BUILD] b left join [e2sGeneral].dbo.HISTORICAL h on b.[FK_HTR_ID] = h.HTR_ID
  where log_type <> 'INFO' and LOG_DATE > CURRENT_TIMESTAMP - 30  
  order by log_id desc
  