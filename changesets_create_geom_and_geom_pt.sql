ALTER TABLE public.changesets_date
ADD geom geometry(Polygon,3857)
ADD geom_pt geometry;

-- CREATE bbox
	UPDATE public.changesets_date
	SET geom = ST_Transform(
		ST_MakeEnvelope("min_lon", "min_lat", "max_lon", "max_lat", 4326),
		3857
	)
	-- makes sure only valid geometries are processed
	WHERE "min_lon" BETWEEN -180 AND 180
	  AND "max_lon" BETWEEN -180 AND 180
	  AND "min_lat" BETWEEN -90 AND 90
	  AND "max_lat" BETWEEN -90 AND 90;

-- CREATE centroid
	UPDATE changesets_date
	SET geom_pt = ST_Centroid(geom);

-- CREATE INDEX
CREATE INDEX changesets_date_geom_IDX ON public.changesets_date USING gist(geom);
CREATE INDEX changesets_date_geom_pt_IDX ON public.changesets_date USING gist(geom_pt);

-- check on table
SELECT *
FROM public.changesets_date
ORDER BY num_changes DESC 
LIMIT 100

-- check INDEX creation
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'changesets_date';