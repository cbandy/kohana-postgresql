<?php defined('SYSPATH') or die('No direct script access.');
/**
 * PostgreSQL database connection.
 *
 * @package PostgreSQL
 */
class Kohana_Database_PostgreSQL extends Database {

	protected $_version;

	public function connect()
	{
		if ($this->_connection)
			return;

		extract($this->_config['connection']);

		if ( ! isset($info) OR ! $info)
		{
			// Build connection string
			$info = isset($hostname) ? "host='$hostname'" : '';
			$info .= isset($port) ? " port='$port'" : '';
			$info .= isset($username) ? " user='$username'" : '';
			$info .= isset($password) ? " password='$password'" : '';
			$info .= isset($database) ? " dbname='$database'" : '';

			if (isset($ssl))
			{
				if ($ssl === TRUE)
				{
					$info .= " sslmode='require'";
				}
				elseif ($ssl === FALSE)
				{
					$info .= " sslmode='disable'";
				}
				else
				{
					$info .= " sslmode='$ssl'";
				}
			}
		}

		// Clear the connection parameters for security
		unset($this->_config['connection']);

		try
		{
			$this->_connection = empty($persistent)
				? pg_connect($info, PGSQL_CONNECT_FORCE_NEW)
				: pg_pconnect($info, PGSQL_CONNECT_FORCE_NEW);
		}
		catch (ErrorException $e)
		{
			throw new Database_Exception($e->getMessage());
		}

		$this->_version = pg_parameter_status($this->_connection, 'server_version');

		if ( ! empty($this->_config['charset']))
		{
			$this->set_charset($this->_config['charset']);
		}

		if (empty($this->_config['schema']))
		{
			// Assume the default schema without changing the search path
			$this->_config['schema'] = 'public';
		}
		else
		{
			if ( ! pg_send_query($this->_connection, 'SET search_path = '.$this->_config['schema'].', pg_catalog'))
				throw new Database_Exception(pg_last_error($this->_connection));

			if ( ! $result = pg_get_result($this->_connection))
				throw new Database_Exception(pg_last_error($this->_connection));

			if (pg_result_status($result) !== PGSQL_COMMAND_OK)
				throw new Database_Exception(pg_result_error($result));
		}
	}

	public function disconnect()
	{
		if ( ! $status = ! is_resource($this->_connection))
		{
			if ($status = pg_close($this->_connection))
			{
				$this->_connection = NULL;
			}
		}

		return $status;
	}

	public function set_charset($charset)
	{
		$this->_connection or $this->connect();

		if (pg_set_client_encoding($this->_connection, $charset) !== 0)
			throw new Database_Exception(pg_last_error($this->_connection));
	}

	public function query($type, $sql, $as_object)
	{
		$this->_connection or $this->connect();

		if ( ! empty($this->_config['profiling']))
		{
			// Benchmark this query for the current instance
			$benchmark = Profiler::start("Database ({$this->_instance})", $sql);
		}

		try
		{
			if ( ! pg_send_query($this->_connection, $sql))
				throw new Database_Exception(':error [ :query ]',
					array(':error' => pg_last_error($this->_connection), ':query' => $sql));

			if ( ! $result = pg_get_result($this->_connection))
				throw new Database_Exception(':error [ :query ]',
					array(':error' => pg_last_error($this->_connection), ':query' => $sql));

			// Check the result for errors
			switch (pg_result_status($result))
			{
				case PGSQL_EMPTY_QUERY:
					$rows = 0;
				break;
				case PGSQL_COMMAND_OK:
					$rows = pg_affected_rows($result);
				break;
				case PGSQL_TUPLES_OK:
					$rows = pg_num_rows($result);
				break;
				case PGSQL_COPY_OUT:
				case PGSQL_COPY_IN:
					throw new Database_Exception('PostgreSQL COPY operations not supported [ :query ]',
						array(':query' => $sql));
				case PGSQL_BAD_RESPONSE:
				case PGSQL_NONFATAL_ERROR:
				case PGSQL_FATAL_ERROR:
					throw new Database_Exception(':error [ :query ]',
						array(':error' => pg_result_error($result), ':query' => $sql));
			}

			if (isset($benchmark))
			{
				Profiler::stop($benchmark);
			}

			$this->last_query = $sql;

			if ($type === Database::SELECT)
				return new Database_PostgreSQL_Result($result, $sql, $as_object, $rows);

			if ($type === Database::INSERT)
			{
				if ($insert_id = pg_send_query($this->_connection, 'SELECT LASTVAL()'))
				{
					if ($result = pg_get_result($this->_connection) AND pg_result_status($result) === PGSQL_TUPLES_OK)
					{
						$insert_id = pg_fetch_result($result, 0);
					}
				}

				return array($insert_id, $rows);
			}

			return $rows;
		}
		catch (Exception $e)
		{
			if (isset($benchmark))
			{
				Profiler::delete($benchmark);
			}

			throw $e;
		}
	}

	public function list_tables($like = NULL)
	{
		$this->_connection or $this->connect();

		$sql = 'SELECT table_name FROM information_schema.tables WHERE table_schema = '.$this->quote($this->schema());

		if (is_string($like))
		{
			$sql .= ' AND table_name LIKE '.$this->quote($like);
		}

		$result = array();
		foreach ($this->query(Database::SELECT, $sql, FALSE) as $row)
		{
			$result[] = $row['table_name'];
		}

		return $result;
	}

	public function list_columns($table, $like = NULL)
	{
		$this->_connection or $this->connect();

		$sql = 'SELECT column_name FROM information_schema.columns WHERE table_schema = '.$this->quote($this->schema()).' AND table_name = '.$this->quote($table);

		if (is_string($like))
		{
			$sql .= ' AND column_name LIKE '.$this->quote($like);
		}

		$sql .= ' ORDER BY ordinal_position';

		$result = array();
		foreach ($this->query(Database::SELECT, $sql, FALSE) as $row)
		{
			$result[] = $row['column_name'];
		}

		return $result;
	}

	public function schema()
	{
		return $this->_config['schema'];
	}

	public function quote($value)
	{
		// This SQL-92 format works in boolean and integer columns
		if ($value === TRUE)
			return "'1'";

		if ($value === FALSE)
			return "'0'";

		return parent::quote($value);
	}

	public function escape($value)
	{
		$this->_connection or $this->connect();

		$value = pg_escape_string($this->_connection, $value);

		return "'$value'";
	}
}
