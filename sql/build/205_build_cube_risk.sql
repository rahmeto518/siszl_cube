INSERT INTO cube.risk_final
SELECT
    f_region_smo_cd,
    f_profile_cd,
    f_dn_group_cd,
    f_y,
    f_q,
    f_smo,
    f_mo,
    risk_id,
    countV,
    countGtAverageV,
    mCountV,
    wCountV,
    m1839V,
    toUInt8(if(m_max = 0, m1839_p,
        if(m1839_p = m_max, 100 - m4065_p - mOver65_p, m1839_p)
    )) AS m1839_percent,
    m4065V,
    toUInt8(if(m_max = 0, m4065_p,
        if(m1839_p != m_max AND m4065_p = m_max, 100 - m1839_p - mOver65_p, m4065_p)
    )) AS m4065_percent,
    mOver65V,
    toUInt8(if(m_max = 0, mOver65_p,
        if(m1839_p != m_max AND m4065_p != m_max AND mOver65_p = m_max, 100 - m1839_p - m4065_p, mOver65_p)
    )) AS mOver65_percent,
    w1839V,
    toUInt8(if(w_max = 0, w1839_p,
        if(w1839_p = w_max, 100 - w4060_p - wOver60_p, w1839_p)
    )) AS w1839_percent,
    w4060V,
    toUInt8(if(w_max = 0, w4060_p,
        if(w1839_p != w_max AND w4060_p = w_max, 100 - w1839_p - wOver60_p, w4060_p)
    )) AS w4060_percent,
    wOver60V,
    toUInt8(if(w_max = 0, wOver60_p,
        if(w1839_p != w_max AND w4060_p != w_max AND wOver60_p = w_max, 100 - w1839_p - w4060_p, wOver60_p)
    )) AS wOver60_percent,
    mCountGtAverageV,
    wCountGtAverageV,
    m1839GtAverageV,
    toUInt8(if(m_gt_max = 0, m1839_gt_p,
        if(m1839_gt_p = m_gt_max, 100 - m4065_gt_p - mOver65_gt_p, m1839_gt_p)
    )) AS m1839GtAverage_percent,
    m4065GtAverageV,
    toUInt8(if(m_gt_max = 0, m4065_gt_p,
        if(m1839_gt_p != m_gt_max AND m4065_gt_p = m_gt_max, 100 - m1839_gt_p - mOver65_gt_p, m4065_gt_p)
    )) AS m4065GtAverage_percent,
    mOver65GtAverageV,
    toUInt8(if(m_gt_max = 0, mOver65_gt_p,
        if(m1839_gt_p != m_gt_max AND m4065_gt_p != m_gt_max AND mOver65_gt_p = m_gt_max, 100 - m1839_gt_p - m4065_gt_p, mOver65_gt_p)
    )) AS mOver65GtAverage_percent,
    w1839GtAverageV,
    toUInt8(if(w_gt_max = 0, w1839_gt_p,
        if(w1839_gt_p = w_gt_max, 100 - w4060_gt_p - wOver60_gt_p, w1839_gt_p)
    )) AS w1839GtAverage_percent,
    w4060GtAverageV,
    toUInt8(if(w_gt_max = 0, w4060_gt_p,
        if(w1839_gt_p != w_gt_max AND w4060_gt_p = w_gt_max, 100 - w1839_gt_p - wOver60_gt_p, w4060_gt_p)
    )) AS w4060GtAverage_percent,
    wOver60GtAverageV,
    toUInt8(if(w_gt_max = 0, wOver60_gt_p,
        if(w1839_gt_p != w_gt_max AND w4060_gt_p != w_gt_max AND wOver60_gt_p = w_gt_max, 100 - w1839_gt_p - w4060_gt_p, wOver60_gt_p)
    )) AS wOver60GtAverage_percent,
    lowV,
    toUInt8(if(lah_max = 0, low_p,
        if(low_p = lah_max, 100 - average_p - high_p, low_p)
    )) AS low_percent,
    averageV,
    toUInt8(if(lah_max = 0, average_p,
        if(low_p != lah_max AND average_p = lah_max, 100 - low_p - high_p, average_p)
    )) AS average_percent,
    highV,
    toUInt8(if(lah_max = 0, high_p,
        if(low_p != lah_max AND average_p != lah_max AND high_p = lah_max, 100 - low_p - average_p, high_p)
    )) AS high_percent
FROM
(
    SELECT
        *,
        greatest(m1839_p, m4065_p, mOver65_p) AS m_max,
        greatest(w1839_p, w4060_p, wOver60_p) AS w_max,
        greatest(m1839_gt_p, m4065_gt_p, mOver65_gt_p) AS m_gt_max,
        greatest(w1839_gt_p, w4060_gt_p, wOver60_gt_p) AS w_gt_max,
        greatest(low_p, average_p, high_p) AS lah_max
    FROM
    (
        SELECT
            *,
            toInt32(ifNull(round(m1839V * 100 / nullIf(mCountV, 0)), 0)) AS m1839_p,
			toInt32(ifNull(round(m4065V * 100 / nullIf(mCountV, 0)), 0)) AS m4065_p,
			toInt32(ifNull(round(mOver65V * 100 / nullIf(mCountV, 0)), 0)) AS mOver65_p,
			toInt32(ifNull(round(w1839V * 100 / nullIf(wCountV, 0)), 0)) AS w1839_p,
			toInt32(ifNull(round(w4060V * 100 / nullIf(wCountV, 0)), 0)) AS w4060_p,
			toInt32(ifNull(round(wOver60V * 100 / nullIf(wCountV, 0)), 0)) AS wOver60_p,
			toInt32(ifNull(round(m1839GtAverageV * 100 / nullIf(mCountGtAverageV, 0)), 0)) AS m1839_gt_p,
			toInt32(ifNull(round(m4065GtAverageV * 100 / nullIf(mCountGtAverageV, 0)), 0)) AS m4065_gt_p,
			toInt32(ifNull(round(mOver65GtAverageV * 100 / nullIf(mCountGtAverageV, 0)), 0)) AS mOver65_gt_p,
			toInt32(ifNull(round(w1839GtAverageV * 100 / nullIf(wCountGtAverageV, 0)), 0)) AS w1839_gt_p,
			toInt32(ifNull(round(w4060GtAverageV * 100 / nullIf(wCountGtAverageV, 0)), 0)) AS w4060_gt_p,
			toInt32(ifNull(round(wOver60GtAverageV * 100 / nullIf(wCountGtAverageV, 0)), 0)) AS wOver60_gt_p,
			toInt32(ifNull(round(lowV * 100 / nullIf(countV, 0)), 0)) AS low_p,
			toInt32(ifNull(round(averageV * 100 / nullIf(countV, 0)), 0)) AS average_p,
			toInt32(ifNull(round(highV * 100 / nullIf(countV, 0)), 0)) AS high_p
        FROM
        (
            SELECT
                f.f_region_smo_cd,
                f.f_profile_cd,
                f.f_dn_group_cd,
                f.f_y,
                f.f_q,
                f.f_smo,
                f.f_mo,
                x.risk_id,
                count() AS countV,
                countIf(x.risk_level_id > 3) AS countGtAverageV,
                countIf(x.pol_cd = 1) AS mCountV,
                countIf(x.pol_cd = 2) AS wCountV,
                countIf(x.pol_cd = 1 AND x.age_on_period_end BETWEEN 18 AND 39) AS m1839V,
                countIf(x.pol_cd = 1 AND x.age_on_period_end BETWEEN 40 AND 65) AS m4065V,
                countIf(x.pol_cd = 1 AND x.age_on_period_end >= 66) AS mOver65V,
                countIf(x.pol_cd = 2 AND x.age_on_period_end BETWEEN 18 AND 39) AS w1839V,
                countIf(x.pol_cd = 2 AND x.age_on_period_end BETWEEN 40 AND 60) AS w4060V,
                countIf(x.pol_cd = 2 AND x.age_on_period_end >= 61) AS wOver60V,
                countIf(x.pol_cd = 1 AND x.risk_level_id > 3) AS mCountGtAverageV,
                countIf(x.pol_cd = 2 AND x.risk_level_id > 3) AS wCountGtAverageV,
                countIf(x.pol_cd = 1 AND x.risk_level_id > 3 AND x.age_on_period_end BETWEEN 18 AND 39) AS m1839GtAverageV,
                countIf(x.pol_cd = 1 AND x.risk_level_id > 3 AND x.age_on_period_end BETWEEN 40 AND 65) AS m4065GtAverageV,
                countIf(x.pol_cd = 1 AND x.risk_level_id > 3 AND x.age_on_period_end >= 66) AS mOver65GtAverageV,
                countIf(x.pol_cd = 2 AND x.risk_level_id > 3 AND x.age_on_period_end BETWEEN 18 AND 39) AS w1839GtAverageV,
                countIf(x.pol_cd = 2 AND x.risk_level_id > 3 AND x.age_on_period_end BETWEEN 40 AND 60) AS w4060GtAverageV,
                countIf(x.pol_cd = 2 AND x.risk_level_id > 3 AND x.age_on_period_end >= 61) AS wOver60GtAverageV,
                countIf(x.risk_level_id < 3) AS lowV,
                countIf(x.risk_level_id = 3) AS averageV,
                countIf(x.risk_level_id > 3) AS highV
            FROM tmp.dim_risk_filters AS f
            INNER JOIN
            (
                SELECT
                    f_y,
                    f_q,
                    if(bitTest(mask, 0), '0', if(ifNull(region_smo_cd, '') = '', '0', region_smo_cd)) AS f_region_smo_cd,
                    if(bitTest(mask, 1), '0', if(ifNull(profile_cd, '') = '', '0', profile_cd)) AS f_profile_cd,
                    if(bitTest(mask, 2), '0', if(ifNull(group_cd, '') = '', '0', group_cd)) AS f_dn_group_cd,
                    if(bitTest(mask, 3), '0', if(ifNull(smo_cd, '') = '', '0', smo_cd)) AS f_smo,
                    if(bitTest(mask, 4), '0', if(ifNull(mo_cd, '') = '', '0', mo_cd)) AS f_mo,
                    risk_id,
                    risk_level_id,
                    pol_cd,
                    age_on_period_end
                FROM tmp.risk_precalc
                ARRAY JOIN range(32) AS mask
            ) AS x
                ON  x.f_region_smo_cd = f.f_region_smo_cd
                AND x.f_profile_cd = f.f_profile_cd
                AND x.f_dn_group_cd = f.f_dn_group_cd
                AND x.f_y = f.f_y
                AND x.f_q = f.f_q
                AND x.f_smo = f.f_smo
                AND x.f_mo = f.f_mo
            GROUP BY
                f.f_region_smo_cd,
                f.f_profile_cd,
                f.f_dn_group_cd,
                f.f_y,
                f.f_q,
                f.f_smo,
                f.f_mo,
                x.risk_id
        )
    )
);
