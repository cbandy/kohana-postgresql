<?php defined('SYSPATH') OR die('No direct access allowed.');

return array
(
	'postgresql' => array
	(
		'type'       => 'postgresql',
		'connection' => array(
			/**
			 * The following options are available for PostgreSQL:
			 *
			 * string   info        @link http://www.postgresql.org/docs/current/static/libpq-connect.html
			 * string   hostname    NULL to use default domain socket
			 * integer  port
			 * string   username
			 * string   password
			 * string   database
			 * boolean  persistent
			 * mixed    ssl         TRUE to require, FALSE to disable, or 'allow' to negotiate
			 */
			'hostname'   => 'localhost',
			'username'   => NULL,
			'password'   => NULL,
			'persistent' => FALSE,
			'database'   => 'kohana',
		),
		'primary_key'  => '',   // Column to return from INSERT queries, see #2188 and #2273
		'schema'       => '',
		'table_prefix' => '',
		'charset'      => 'utf8',
		'caching'      => FALSE,
		'profiling'    => TRUE,
	),
);
