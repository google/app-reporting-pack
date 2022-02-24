# Get meta information on each video (title & duration)
SELECT
    video.id AS video_id,
    video.title AS video_title,
    video.duration_millis AS video_duration
FROM video
