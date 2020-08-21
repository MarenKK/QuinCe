-- Add core_sensor type
update variable_sensors set core=1 where sensor_type=28;

-- Add run_type
update sensor_types set internal_calibration=1 where id=31;