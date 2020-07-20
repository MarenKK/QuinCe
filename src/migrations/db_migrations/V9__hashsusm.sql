-- Update data_file table to include hashsum column
ALTER TABLE data_file ADD hashsum varchar(64) DEFAULT NULL;