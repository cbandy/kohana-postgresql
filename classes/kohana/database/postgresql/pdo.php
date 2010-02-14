<?php defined('SYSPATH') or die('No direct script access.');
/**
 * PostgreSQL database connection using the PDO driver.
 *
 * @package PostgreSQL
 */
class Kohana_Database_PostgreSQL_PDO extends Database_PostgreSQL
{
	public function connect()
	{
		if ($this->_connection)
			return;

		$info = $this->_connection_string();
		$options = array
		(
			// Force PDO to use exceptions for all errors
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_PERSISTENT => ! empty($this->_config['connection']['persistent']),
		);

		// Clear the connection parameters for security
		unset($this->_config['connection']);

		$this->_connection = new PDO('pgsql:'.$info, NULL, NULL, $options);
		$this->_version = $this->_connection->getAttribute(PDO::ATTR_SERVER_VERSION);

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
			$this->_connection->exec('SET search_path = '.$this->_config['schema'].', pg_catalog');
		}
	}

	public function disconnect()
	{
		// Destroy the PDO object
		$this->_connection = NULL;

		return TRUE;
	}

	public function escape($value)
	{
		$this->_connection or $this->connect();

		return $this->_connection->quote($value);
	}

	public function query($type, $sql, $as_object)
	{
		$this->_connection or $this->connect();

		if ( ! empty($this->_config['profiling']))
		{
			// Benchmark this query for the current instance
			$benchmark = Profiler::start("Database ({$this->_instance})", $sql);
		}

		if ($type === Database::INSERT AND $this->_config['primary_key'])
		{
			$sql .= ' RETURNING '.$this->quote_identifier($this->_config['primary_key']);
		}

		try
		{
			$result = $this->_connection->query($sql);

			if (isset($benchmark))
			{
				Profiler::stop($benchmark);
			}

			// Set the last query
			$this->last_query = $sql;

			if ($type === Database::SELECT)
			{
				if ( ! $as_object)
				{
					$result->setFetchMode(PDO::FETCH_ASSOC);
				}
				else
				{
					$result->setFetchMode(PDO::FETCH_CLASS, is_string($as_object) ? $as_object : 'stdClass');
				}

				// Scollable cursors not supported until 5.3.0; see http://bugs.php.net/44861
				return new Database_Result_Cached($result->fetchAll(), $sql, $as_object);
			}

			if ($type === Database::INSERT)
			{
				if ($this->_config['primary_key'])
				{
					// Fetch the first column of the last row
					$insert_id = end($result->fetchAll(PDO::FETCH_COLUMN, 0));
				}
				else
				{
					$insert_id = $this->_connection->query('SELECT LASTVAL()')->fetchColumn();
				}

				return array($insert_id, $result->rowCount());
			}

			return $result->rowCount();
		}
		catch (Exception $e)
		{
			if (isset($benchmark))
			{
				// This benchmark is worthless
				Profiler::delete($benchmark);
			}

			// Rethrow the exception
			throw $e;
		}
	}

	public function set_charset($charset)
	{
		$this->_connection or $this->connect();

		$this->_connection->exec('SET NAMES '.$this->quote($charset));
	}
}
