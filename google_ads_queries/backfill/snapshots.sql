SELECT
  change_event.resource_name,
  change_event.change_date_time,
  change_event.change_resource_name,
  change_event.user_email,
  change_event.client_type,
  change_event.change_resource_type,
  change_event.old_resource,
  change_event.new_resource,
  change_event.resource_change_operation,
  change_event.changed_fields
FROM change_event
WHERE
    change_event.change_date_time <= "2021-08-30"
    AND change_event.change_date_time >=  "2021-08-21"
ORDER BY change_event.change_date_time DESC
LIMIT 10000
