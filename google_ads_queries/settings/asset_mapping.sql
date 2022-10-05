-- Performs association between each asset_id and its corresponding meta
-- information (size for images and HTML5, actual text for headlines and
-- descriptions, etc.)

SELECT
    asset.id AS id,
    asset.name AS asset_name,
    asset.text_asset.text AS text,
    asset.image_asset.full_size.height_pixels AS height,
    asset.image_asset.full_size.width_pixels AS width,
    asset.image_asset.full_size.url AS url,
    asset.type AS type,
    asset.image_asset.mime_type AS mime_type,
    asset.youtube_video_asset.youtube_video_id AS youtube_video_id,
    asset.youtube_video_asset.youtube_video_title AS youtube_video_title
FROM asset
