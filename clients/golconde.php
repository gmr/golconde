<?php
/**
 * Golconde Client Library
 *
 * Use this class for adding items into the top level Golconde distribution destinations
 */
 
class Golconde
{
  private $connected = false;
  
  /**
   * Costructor, passing in ActiveMQ Connection Information
   *
   * @param string host
   * @param int port
   */  
  function __construct($host, $port)
  {
  }
  
  /**
   * Connect to an ActiveMQ Server
   *
   * @param string host
   * @param int port
   */
  private function connect( )
  {
  }
  
  /**
   * Issue a golconde add command for the destation
   *
   * @param string Destination (ie Test)
   * @param array Key=>Value array of data to send
   */
  function add( $destination, $data )
  {
    if ( !$connected ) $this->connect( );
  }
 
  /**
   * Issue a golconde delete command for the destation
   *
   * @param string Destination (ie Test)
   * @param array Key=>Value array of data to send
   */
  function delete( $destination, $data )
  {
    if ( !$connected ) $this->connect( );
  }

  /**
   * Issue a golconde update command for the destation
   *
   * @param string Destination (ie Test)
   * @param array Key=>Value array of data to send
   */
  function update( $destination, $data )
  {
    if ( !$connected ) $this->connect( );
  }

  /**
   * Issue a golconde upsert command for the destation
   *
   * @param string Destination (ie Test)
   * @param array Key=>Value array of data to send
   */
  function upsert( $destination, $data )
  {
    if ( !$connected ) $this->connect( );
  } 
  
}