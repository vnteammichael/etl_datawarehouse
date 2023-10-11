SELECT
	D.full_date,
	S.social_name,
	P.platform_name,
	L.level_range,
	F.churn_type,
	F.churn_users,
	F.total_daily_active_users
FROM
	fact_churn_measure F
	INNER JOIN dim_date D ON (F.date_id = D.id)
	LEFT JOIN dim_social S ON (F.social_id = S.id)
	LEFT JOIN dim_platform P ON (F.platform_id = P.id)
	LEFT JOIN dim_level_range L ON (F.level_range_id = L.id)
