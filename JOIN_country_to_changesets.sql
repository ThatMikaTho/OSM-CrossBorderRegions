-- temp changeset table 
-- 5-7min for 11 countries
DROP TABLE IF EXISTS eligible_changesets;
CREATE TEMP TABLE eligible_changesets AS
SELECT c.changeset_id, c.geom_pt_4326::geometry
FROM changesets_250421 c
JOIN users_bbox_upperrhine_250421 u ON c.uid = u.uid
WHERE u.total_changesets >= 10 AND u.percent_inside_changesets >= 0.5;
	CREATE INDEX changeset_idx ON eligible_changesets(changeset_id);
	CREATE INDEX geom_idx ON eligible_changesets USING gist(geom_pt_4326);

-- temp select countries table
-- 4s
DROP TABLE IF EXISTS selected_countries;
CREATE TEMP TABLE selected_countries AS
SELECT
	fid,
	name_en,
	ST_Subdivide("geom_4326"::geometry, 100) AS geom_div_4326
FROM countries_admin_lvl_2_selected; -- has only Germany and its direct neighbours
	CREATE INDEX geom_div_4326_idx ON selected_countries USING gist(geom_div_4326);

-- temp joined table
-- 1-3min | returns roughly 4mil changesets
DROP TABLE IF EXISTS changesets_countries;
CREATE TEMP TABLE changesets_countries AS
WITH matched_countries AS (
	SELECT
		c.changeset_id,
		c.geom_pt_4326,
		ARRAY_AGG(DISTINCT sc.name_en) AS country_names,
		COUNT(*) AS matched_countries_count
	FROM eligible_changesets c
	LEFT JOIN selected_countries sc
		ON ST_Intersects(sc.geom_div_4326, c.geom_pt_4326)
		AND c.geom_pt_4326 && sc.geom_div_4326	
	GROUP BY changeset_id, geom_pt_4326
	--LIMIT 100
)
SELECT DISTINCT ON (changeset_id)
	*
FROM matched_countries;
	
ALTER TABLE changesets_countries ADD PRIMARY KEY (changeset_id);
UPDATE changesets_countries
SET country_names = ARRAY['outside']
WHERE country_names = '{NULL}';

--WHERE NOT country_names ?| array['Germany', 'France', 'Switzerland']
--WHERE ARRAY_LENGTH(country_names,1) > 1


-- rejoin to changeset TABLE
-- 20min
ALTER TABLE changesets_250421
--DROP COLUMN country_names
ADD COLUMN country_names VARCHAR[];

UPDATE changesets_250421 c
SET country_names = cc.country_names
FROM changesets_countries cc
WHERE c.changeset_id = cc.changeset_id;

-- 4.5h to set all other values to {'outside'}
-- UPDATE changesets_250421
-- SET country_names = ARRAY['outside']
-- WHERE country_names IS NULL;
