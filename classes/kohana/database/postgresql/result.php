<?php defined('SYSPATH') or die('No direct script access.');
/**
 * PostgreSQL database result.
 *
 * @package PostgreSQL
 */
class Kohana_Database_PostgreSQL_Result extends Database_Result {

	protected $_internal_row = 0;

	public function __construct($result, $sql, $as_object, $total_rows = NULL)
	{
		parent::__construct($result, $sql, $as_object);

		if ($total_rows !== NULL)
		{
			$this->_total_rows = $total_rows;
		}
		else
		{
			switch (pg_result_status($result))
			{
				case PGSQL_EMPTY_QUERY:
					$this->_total_rows = 0;
				break;
				case PGSQL_COMMAND_OK:
					$this->_total_rows = pg_affected_rows($result);
				break;
				case PGSQL_TUPLES_OK:
					$this->_total_rows = pg_num_rows($result);
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
		}
	}

	public function __destruct()
	{
		if (is_resource($this->_result))
		{
			pg_free_result($this->_result);
		}
	}

	public function as_array($key = NULL, $value = NULL)
	{
		if ( ! $this->_as_object AND $key === NULL AND $value === NULL)
			return pg_fetch_all($this->_result);

		return parent::as_array($key, $value);
	}

	/**
	 * SeekableIterator: seek
	 */
	public function seek($offset)
	{
		if ($this->offsetExists($offset) AND pg_result_seek($this->_result, $offset))
		{
			$this->_current_row = $this->_internal_row = $offset;

			return TRUE;
		}

		return FALSE;
	}

	/**
	 * Iterator: current
	 */
	public function current()
	{
		if ($this->_current_row !== $this->_internal_row AND ! $this->seek($this->_current_row))
			return FALSE;

		// Track the row that will be returned by pg_fetch_* after the below call to pg_fetch_*
		++$this->_internal_row;

		if ( ! $this->_as_object)
			return pg_fetch_assoc($this->_result);

		if (is_string($this->_as_object))
			return pg_fetch_object($this->_result, $this->_current_row, $this->_as_object);

		return pg_fetch_object($this->_result);
	}

}
