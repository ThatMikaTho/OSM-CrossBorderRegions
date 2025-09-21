-- 13h 
-- manche 1000er Batches brauchen 3h andere nur 10min. Evtl. batch-Größe verkleinern oder die initiale Sortierung so machen, dass die batches ungefähr gleich lange brauchen
DROP TABLE IF EXISTS users_bbox_upperrhine_250421_knn;
CREATE TABLE users_bbox_upperrhine_250421_knn(
    uid bigint PRIMARY KEY,
    username text,
    min_knn_dist real,
	max_knn_dist real,
	median_knn_dist real,
	avg_knn_dist real,
	stddev_knn_dist real,
    user_min_changes integer,
    user_inside_percentage real
);

-- Batch process
DO $$
DECLARE
	min_changes INT := 10;
	inside_percentage REAL := 0.5;
    batch_size INT := 10;
    offset_step INT := 0;
    user_batch RECORD;
    user_list BIGINT[];
BEGIN
    LOOP
        -- batch
        SELECT ARRAY(
            SELECT uid
            FROM users_bbox_upperrhine_250421
            WHERE total_changesets >= min_changes AND percent_inside_changesets >= inside_percentage
            ORDER BY uid
            OFFSET offset_step LIMIT batch_size
        ) INTO user_list;

        EXIT WHEN array_length(user_list, 1) IS NULL;

        RAISE NOTICE 'Processing users from offset % to %', offset_step, offset_step + batch_size - 1;
		RAISE NOTICE 'Started batch at %', clock_timestamp()::timestamp(0);

        -- Insert results for this batch
        INSERT INTO users_bbox_upperrhine_250421_knn
        WITH
		nearest_neighbour AS(
			SELECT
				c1.uid,
				c1.username,
				c1.changeset_id,
				ST_Distance(c1.geom_pt_4326, nn.geom_pt_4326, false)::real AS knn_dist
			FROM changesets_250421 c1
			JOIN users_bbox_upperrhine_250421 u ON c1.uid = u.uid
			JOIN LATERAL (
				SELECT c2.changeset_id, c2.geom_pt_4326
				FROM changesets_250421 c2
				WHERE
				c2.uid = c1.uid AND
				c2.changeset_id <> c1.changeset_id AND
				-- Use bounding box filter to reduce candidates
				c2.geom_pt_4326 && ST_Expand(c1.geom_pt_4326::geometry, 0.2)::geography AND
				-- Accurate distance check in meters
				ST_DWithin(c1.geom_pt_4326, c2.geom_pt_4326, 50000, false)
				ORDER BY c1.geom_pt_4326 <-> c2.geom_pt_4326
				LIMIT 3 -- set k value
			) AS nn ON true
			WHERE c1.uid = ANY(user_list)
		),
		
		avg_knn AS (
			SELECT
				uid,
				username,
				MIN(knn_dist) AS min_knn_dist,
				MAX(knn_dist) AS max_knn_dist,
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY knn_dist) AS median_knn_dist,
				AVG(knn_dist) AS avg_knn_dist,
				STDDEV_POP(knn_dist) AS stddev_knn_dist
			FROM nearest_neighbour
			GROUP BY uid, username
		)
        SELECT
        	ann.uid,
            ann.username,
			ann.min_knn_dist,
			ann.max_knn_dist,
			ann.median_knn_dist,
			ann.avg_knn_dist,
			ann.stddev_knn_dist
        FROM avg_knn ann
		ON CONFLICT (uid) DO NOTHING;
        -- Move to next batch
        offset_step := offset_step + batch_size;
		
		RAISE NOTICE 'Finished batch at %', clock_timestamp()::timestamp(0);
   	END LOOP;

    RAISE NOTICE 'Finished all batches.';
END $$;

--------------------------------------------------------- JOIN KNN TO USER TABLE ---------------------------------------------------------------------------
/*
ALTER TABLE users_bbox_upperrhine_250421
ADD COLUMN min_knn_dist real,
ADD COLUMN max_knn_dist real,
ADD COLUMN median_knn_dist real,
ADD COLUMN avg_knn_dist real,
ADD COLUMN stddev_knn_dist real,
ADD COLUMN user_min_changes integer,
ADD COLUMN user_inside_percentage real;
*/

UPDATE users_bbox_upperrhine_250421 u1
SET
	min_knn_dist = u2.min_knn_dist,
	max_knn_dist = u2.max_knn_dist,
	median_knn_dist = u2.median_knn_dist,
	avg_knn_dist = u2.avg_knn_dist,
	stddev_knn_dist = u2.stddev_knn_dist,
	user_min_changes = u2.min_changes,
	user_inside_percentage = u2.inside_percentage
FROM users_bbox_upperrhine_250421_knn u2
WHERE u1.uid = u2.uid;
