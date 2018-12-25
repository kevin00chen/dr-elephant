/**
 * Stores the Yarn Application Original Info.
 */
CREATE TABLE IF NOT EXISTS `yarn_app_original` (
  `app_id` varchar(50) NOT NULL COMMENT 'The application id, e.g., application_1236543456321_1234567',
  `name` varchar(1024) NOT NULL COMMENT 'The application name',
  `queue_name` varchar(150) DEFAULT NULL COMMENT 'The queue the application was submitted to',
  `user` varchar(50) NOT NULL COMMENT 'The user who started the application',
  `state` varchar(20) NOT NULL,
  `final_status` varchar(20) NOT NULL,
  `application_type` varchar(20) NOT NULL COMMENT 'The Job Type e.g, SPARK, MAPREDUCE',
  `application_tags` varchar(100) DEFAULT NULL,
  `tracking_url` varchar(255) DEFAULT NULL COMMENT 'The web URL that can be used to track the application',
  `start_time` bigint(20) unsigned NOT NULL COMMENT 'The time in which application started',
  `finish_time` bigint(20) unsigned NOT NULL COMMENT 'The time in which application finished',
  `elapsed_time` bigint(20) unsigned DEFAULT NULL,
  `memory_seconds` bigint(20) unsigned DEFAULT '0' COMMENT 'The Memory resources used by the job in MB Seconds',
  `vcore_seconds` bigint(20) unsigned DEFAULT '0' COMMENT 'The CPU resources used by the job in core Seconds',
  `diagnostics` text,
  `cluster_name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`app_id`),
  KEY `yarn_app_original_i1` (`finish_time`),
  KEY `yarn_app_original_i2` (`user`,`finish_time`),
  KEY `yarn_app_original_i3` (`application_type`,`user`,`finish_time`),
  KEY `yarn_app_original_i7` (`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;