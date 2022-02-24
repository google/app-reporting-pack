# Get meta information on each media element
SELECT
    media_file.id AS media_id,
    media_file.name AS name,
    media_file.image.full_size_image_url AS url,
    media_file.source_url AS source_url,
    media_file.media_bundle.url AS bundle_url,
    media_file.video.youtube_video_id AS video_id
FROM media_file
