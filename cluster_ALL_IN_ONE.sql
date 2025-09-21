---------------------------------------------------------------- create clusters on changeset level ---------------------------------------------------
-- takes about 3min
DROP TABLE IF EXISTS changesets_bbox_UPR_250421_cluster;
CREATE TABLE changesets_bbox_UPR_250421_cluster
AS
WITH 
cluster_params AS (
	SELECT
		4::integer AS minpoints
),
user_params AS (
	SELECT
		10::integer AS user_min_changes, -- minimum changes/changesets by a user to be included in calculation
		0.5::float AS user_inside_percentage, -- minimum inside percentage by a user to be included in calculation
		1::integer AS top_country_coverage -- used to filter out users who map only in one country
),
users AS (
	SELECT 
		u.uid, u.username, u.total_changes, u.total_changesets, u.percent_inside_changesets,
		CASE
			WHEN (u.stddev_knn_dist / u.avg_knn_dist) >= 3.5 THEN GREATEST(u.median_knn_dist, 200)
			WHEN (u.stddev_knn_dist / u.avg_knn_dist) > 2 THEN (u.avg_knn_dist - (0.15 * u.stddev_knn_dist))
			ELSE u.avg_knn_dist
		END AS eps_distance,
		up.user_min_changes, up.user_inside_percentage,
		kv.value
	FROM users_bbox_upperrhine_250421 u, user_params up
	LEFT JOIN LATERAL (
	    SELECT key, value::numeric
	    FROM jsonb_each_text(u.country_coverage_percentage)
		WHERE key IS NOT NULL
	    ORDER BY value::numeric DESC
	    LIMIT 1
	) kv ON TRUE
	WHERE total_changesets >= up.user_min_changes AND percent_inside_changesets >= up.user_inside_percentage AND kv.value < up.top_country_coverage AND max_knn_dist > 0
	--LIMIT 10
),
changesets AS (
	SELECT
		c.changeset_id, c.uid, c.username, c.num_changes, c.created_at, c.geom_pt::geometry AS geom_pt_3857, c.geom_pt_4326, c.country_names, dist_to_border_m,
		u.eps_distance
	FROM changesets_250421 c
	JOIN users u ON c.uid = u.uid
	WHERE c.uid IS NOT NULL
),
clusters AS (
	SELECT
		c.changeset_id,
 		c.uid,
    	c.username,
   		(ST_ClusterDBSCAN(geom_pt_3857, eps := c.eps_distance, minpoints := cp.minpoints)
		OVER (PARTITION BY uid)) AS cluster_id,
		c.num_changes,
		c.created_at,
		c.country_names,
		c.dist_to_border_m,
    	c.geom_pt_3857,
		geom_pt_4326,
		-- joined parameters
			c.eps_distance,
			cp.minpoints,
			up.user_min_changes,
			up.user_inside_percentage
	FROM changesets c
	JOIN cluster_params cp ON TRUE
	JOIN user_params up ON TRUE
)
SELECT
	changeset_id,
	uid,
	username,
	cluster_id,
	num_changes,
	created_at,
	country_names,
	dist_to_border_m,
	geom_pt_3857,
	geom_pt_4326,
	eps_distance AS user_eps_dist_m,
	minpoints AS user_minpoints
FROM clusters;
	CREATE INDEX changesets_bbox_UPR_250421_cluster2_uid_IDX ON changesets_bbox_UPR_250421_cluster("uid");
	CREATE INDEX changesets_bbox_UPR_250421_cluster2_username_IDX ON changesets_bbox_UPR_250421_cluster("username");
	CREATE INDEX changesets_bbox_UPR_250421_cluster2_geom_3857_IDX ON changesets_bbox_UPR_250421_cluster USING gist(geom_pt_3857);
	CREATE INDEX changesets_bbox_UPR_250421_cluster2_geom_4326_IDX ON changesets_bbox_UPR_250421_cluster USING gist(geom_pt_4326);
-- PRIMARY KEY
ALTER TABLE changesets_bbox_UPR_250421_cluster ADD PRIMARY KEY (changeset_id);
-- Change Owner to mika. Otherwise QGIS can't load layer
ALTER TABLE changesets_bbox_UPR_250421_cluster OWNER TO mika;

ALTER TABLE users_bbox_upperrhine_250421
	DROP COLUMN IF EXISTS eps_dist_m,
	ADD COLUMN eps_dist_m NUMERIC;
UPDATE users_bbox_upperrhine_250421 u
	SET eps_dist_m = c.user_eps_dist_m
	FROM changesets_bbox_upr_250421_cluster c
	WHERE c.uid = u.uid;

---------------------------------------------------------------- rank clusters and aggregate country_names => home nation ---------------------------------------------------
-- 14s | calculate basic_info per cluster
DROP TABLE IF EXISTS basic_info;
CREATE TEMP TABLE basic_info AS WITH
-- normalisation values
cluster_stats AS (
	SELECT 
		uid,
		cluster_id,
		COUNT(*) AS cluster_size,
		STDDEV(EXTRACT(EPOCH FROM created_at)) AS stddev_seconds,
		ST_Area(ST_ConcaveHull(ST_Collect(geom_pt_3857),0.1)) AS cluster_area
	FROM changesets_bbox_upr_250421_cluster
	WHERE cluster_id IS NOT NULL
	GROUP BY uid, cluster_id
),
user_minmax AS (
	SELECT
		uid,
		MAX(cluster_size)::numeric AS max_size,
		MIN(cluster_size)::numeric AS min_size,
		MAX(stddev_seconds)::numeric AS max_stddev,
		MIN(stddev_seconds)::numeric AS min_stddev
	FROM cluster_stats
	GROUP BY uid
),
normalised AS (
	SELECT 
	cs.uid,
	cs.cluster_id,
	cs.cluster_size,
	cs.cluster_area,
	cs.stddev_seconds,
	-- normalisation
	(cs.cluster_size - um.min_size) / NULLIF(um.max_size - um.min_size, 0) AS norm_size,
	(cs.stddev_seconds - um.min_stddev) / NULLIF(um.max_stddev - um.min_stddev, 0) AS norm_stddev
	FROM cluster_stats cs
	JOIN user_minmax um ON cs.uid = um.uid
),

scored AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY uid
			ORDER BY ((norm_size * 0.6) + (norm_stddev * 0.4)) DESC
		) AS cluster_score
	FROM normalised
),

ranked AS (
	SELECT
		uid,
		cluster_id,
		cluster_size,
	-- ranking include area of top cluster
		ROW_NUMBER() OVER (
			PARTITION BY uid
			ORDER BY 
				CASE WHEN cluster_score <= 3 THEN cluster_area ELSE NULL END
				DESC NULLS LAST
		) AS cluster_rank
	FROM scored
),

-- other info for clusters
prelim_info AS (
	SELECT 
		c.uid,
		c.username,
		c.cluster_id,
		r.cluster_size,
		r.cluster_rank,
		SUM(num_changes) AS num_changes,
		MIN(created_at) AS first_change,
		MAX(created_at) AS last_change,
		date_trunc('second', to_timestamp(AVG(EXTRACT(EPOCH FROM created_at))))::timestamp WITHOUT TIME ZONE AS avg_timestamp,
		STDDEV(EXTRACT(EPOCH FROM created_at)) AS stddev_seconds,
		ST_ConcaveHull(ST_Collect(geom_pt_3857),0.1) AS geom_hull
	FROM changesets_bbox_upr_250421_cluster c
	JOIN ranked r ON r.uid = c.uid AND r.cluster_id = c.cluster_id
	WHERE c.cluster_id IS NOT NULL
	GROUP BY c.uid, c.username, c.cluster_id, r.cluster_size, r.cluster_rank
)
SELECT
	*,
	CASE
		WHEN stddev_seconds < 60 THEN ROUND(stddev_seconds)::text || ' sec'
		WHEN stddev_seconds < 3600 THEN ROUND(stddev_seconds / 60)::text || ' min'
		WHEN stddev_seconds < 86400 THEN ROUND(stddev_seconds / 3600)::text || ' hrs'
		WHEN stddev_seconds < 604800 THEN ROUND(stddev_seconds / 86400)::text || ' days'
		WHEN stddev_seconds < 2592000 THEN ROUND(stddev_seconds / 604800)::text || ' weeks'
		ELSE ROUND(stddev_seconds / 2592000)::text || ' months'
	END AS stddev_readable
FROM prelim_info;
	CREATE INDEX basic_info_uid_IDX ON basic_info("uid");
	CREATE INDEX basic_info_username_IDX ON basic_info("username");
	CREATE INDEX basic_info_cluster_id_IDX ON basic_info (cluster_id);

-- aggregate country names
DROP TABLE IF EXISTS upr_clusters;
CREATE TABLE upr_clusters 
AS
WITH
clusters AS(
	SELECT
		cl.*,
		lat_name
	FROM changesets_bbox_upr_250421_cluster cl
	JOIN LATERAL unnest(country_names) AS lat_name ON TRUE
	JOIN basic_info bi ON bi.uid = cl.uid AND bi.cluster_id = cl.cluster_id
),

count_per_country_name AS (
	SELECT
		uid,
		username,
		cluster_id,
		lat_name,
		COUNT(*) AS country_count
	FROM clusters
	GROUP BY uid, username, cluster_id, lat_name
),

count_per_cluster AS (
	SELECT
		uid,
		username,
		cluster_id,
		SUM(country_count)::float AS total_count
	FROM count_per_country_name
	GROUP BY uid, username, cluster_id
),

percentages AS (
	SELECT
		c1.uid,
		c1.username,
		c1.cluster_id,
		c1.lat_name,
		c1.country_count,
		(c1.country_count / c2.total_count)::numeric(4,2) AS coverage_ratio
	FROM count_per_country_name c1
	JOIN count_per_cluster c2
	ON c1.uid = c2.uid AND c1.cluster_id = c2.cluster_id
),

cluster_agg AS (
	SELECT
		uid,
		username,
		cluster_id,
		jsonb_object_agg(lat_name, country_count) AS country_coverage_count,
		jsonb_object_agg(lat_name, coverage_ratio) AS country_coverage_percentage
	FROM percentages
	GROUP BY uid, username, cluster_id
),

join_to_cluster AS(
	SELECT 
		ca.uid,
		ca.username,
		ca.cluster_id,
		bi.cluster_rank,
		bi.cluster_size,
		bi.num_changes,
		ca.country_coverage_count,
		ca.country_coverage_percentage,
		bi.first_change,
		bi.last_change,
		bi.avg_timestamp,
		bi.stddev_seconds,
		bi.stddev_readable,
		ST_Transform(bi.geom_hull, 4326) AS geom_hull_4326,
		COALESCE (
			(ST_MaximumInscribedCircle(ST_Transform(bi.geom_hull, 4326))).center,
			ST_Centroid(ST_Transform(bi.geom_hull, 4326))
		) AS geom_hull_center_4326
	FROM cluster_agg ca
	JOIN basic_info bi ON bi.uid = ca.uid AND bi.cluster_id = ca.cluster_id
)

-- select and filter for the ratio
SELECT *
FROM join_to_cluster
,LATERAL jsonb_each(country_coverage_percentage) AS cp(home_nation, home_nation_ratio)
WHERE 
	cp.home_nation_ratio::float > 0.5; 
	-- AND cp.home_nation_ratio::float < 0.6
--ORDER BY uid, num_changes, cp.ratio DESC
	ALTER TABLE upr_clusters OWNER TO mika;
	CREATE INDEX upr_clusters_uid_IDX ON upr_clusters("uid");
	CREATE INDEX upr_clusters_username_IDX ON upr_clusters("username");

-- update changesets cluster
ALTER TABLE changesets_bbox_upr_250421_cluster
DROP COLUMN IF EXISTS cluster_rank,
ADD COLUMN cluster_rank NUMERIC(6,0);

UPDATE changesets_bbox_upr_250421_cluster ch
SET cluster_rank = cl.cluster_rank
FROM upr_clusters cl
WHERE cl.uid = ch.uid AND cl.cluster_id = ch.cluster_id;

-- update users
ALTER TABLE users_bbox_upperrhine_250421
DROP COLUMN IF EXISTS home_region,
DROP COLUMN IF EXISTS home_region_ratio,
DROP COLUMN IF EXISTS home_region_geom,
DROP COLUMN IF EXISTS home_region_center,
ADD COLUMN home_region TEXT,
ADD COLUMN home_region_ratio NUMERIC(3,2),
ADD COLUMN home_region_geom GEOMETRY,
ADD COLUMN home_region_center GEOMETRY;

UPDATE users_bbox_upperrhine_250421 u
SET
	home_region = cl.home_nation,
	home_region_ratio = cl.home_nation_ratio::NUMERIC(3,2),
	home_region_geom = cl.geom_hull_4326,
	home_region_center = cl.geom_hull_center_4326
FROM upr_clusters cl
WHERE u.uid = cl.uid AND cl.cluster_rank = 1; -- cluster_rank=1 is the home cluster

------------------------------------------------------ calculate distance to home cluster -----------------------------------------------------------------------------------------------
-- 6min for all 
DROP TABLE IF EXISTS clusters_filtered;
CREATE TEMP TABLE clusters_filtered 
AS
SELECT
	uid,
	username,
	cluster_rank,
	geom_hull_center_4326
FROM upr_clusters
WHERE cluster_rank = 1;
	CREATE INDEX clusters_filtered_uid_IDX ON clusters_filtered("uid");
	CREATE INDEX clusters_filtered_username_IDX ON clusters_filtered("username");
	CREATE INDEX clusters_filtered_geom_hull_4326_IDX ON clusters_filtered USING gist(geom_hull_center_4326);

DROP TABLE IF EXISTS changeset_dist_to_home;
CREATE TABLE changeset_dist_to_home
AS
WITH
distance AS (
	SELECT
		ch.changeset_id,
		ch.cluster_id,
		ch.cluster_rank,
		ch.uid,
		ST_Distance(cf.geom_hull_center_4326, ch.geom_pt_4326, false) AS dist_to_home -- better to calculate to home_region_center?
	FROM changesets_bbox_upr_250421_cluster ch
	JOIN clusters_filtered cf ON ch.uid = cf.uid
	--WHERE ch.cluster_rank IS DISTINCT FROM 1 -- includes NULL, otherwise dist to home is not calculated for all changesets outside home cluster
)

SELECT
	changeset_id,
	cluster_id,
	cluster_rank,
	uid,
	ROUND(dist_to_home)::numeric(10,0) AS dist_to_home
FROM distance;
	CREATE INDEX changeset_dist_to_home_changeset_id_IDX ON changeset_dist_to_home(changeset_id);
	CREATE INDEX changeset_dist_to_home_uid_IDX ON changeset_dist_to_home(uid);

ALTER TABLE changesets_bbox_upr_250421_cluster
ADD COLUMN dist_to_home_m NUMERIC(10,0);

UPDATE changesets_bbox_upr_250421_cluster cl
SET dist_to_home_m = dh.dist_to_home
FROM changeset_dist_to_home dh
WHERE cl.changeset_id = dh.changeset_id;

-- set dist_to_home_m to NULL for changesets 
UPDATE changesets_bbox_upr_250421_cluster
SET dist_to_home_m = NULL 

------------------------------------------------------ calculate distance between home geom and border ----------------------------------------------------------------------------
-- 4min
DROP TABLE IF EXISTS border_lines_4326;
CREATE TEMP TABLE border_lines_4326 AS (
	SELECT
		fid,
		ST_Transform(geom, 4326)::geography AS geom_4326	
	FROM border_lines
	WHERE upr_bbox = 1
);
	CREATE INDEX border_lines_4326_geom_4326_idx ON border_lines_4326 USING GIST(geom_4326);

DROP TABLE IF EXISTS home_to_border;
CREATE TEMP TABLE home_to_border AS 
SELECT
	u.uid,
	u.home_region_center,
	ST_Distance(u.home_region_center, bl.geom_4326) AS homeregion_to_border_dist
FROM users_bbox_upperrhine_250421 u
JOIN LATERAL (
	SELECT b.geom_4326
	FROM border_lines_4326 b
	ORDER BY u.home_region_center <-> b.geom_4326
	LIMIT 1
) bl ON TRUE;
CREATE INDEX home_to_border_uid_IDX ON home_to_border(uid);


ALTER TABLE users_bbox_upperrhine_250421
DROP COLUMN IF EXISTS homeregion_to_border_dist,
ADD COLUMN homeregion_to_border_dist numeric(16,2);

UPDATE users_bbox_upperrhine_250421 u
SET homeregion_to_border_dist = hb.homeregion_to_border_dist
FROM home_to_border hb
WHERE u.uid = hb.uid;