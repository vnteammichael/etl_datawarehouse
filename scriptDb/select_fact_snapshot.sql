SELECT
	D.full_date,
	A.metric_name,
	S.social_name,
	P.platform_name,
	F.metric_value as metric_value,
	C1.dimension_name dimension_name_1,
	C2.dimension_name dimension_name_2,
	C3.dimension_name dimension_name_3
FROM
	fact_snapshot F
	INNER JOIN dim_date D ON (F.date_id = D.id)
	LEFT JOIN dim_social S ON (F.social_id = S.id)
	LEFT JOIN dim_platform P ON (F.platform_id = P.id)
	LEFT JOIN dim_metric A ON (F.metric_id = A.id)
	LEFT JOIN dim_dimension C1 ON (F.dimension_1_id = C1.id)
	LEFT JOIN dim_dimension C2 ON (F.dimension_2_id = C2.id)
	LEFT JOIN dim_dimension C3 ON (F.dimension_3_id = C3.id)