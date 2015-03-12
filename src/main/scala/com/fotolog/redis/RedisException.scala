package com.fotolog.redis

/**
 *
 *
 * Date: 11/22/13 11:07 AM
 * @author Sergey Khruschak (sergey.khruschak@gmail.com)
 */
class RedisException(msg: String) extends Exception(msg)

case class ScriptSyntaxException(msg: String) extends RedisException(msg)
case class UnsupportedResponseException(msg: String) extends RedisException(msg)