package io.replicant

import cats.syntax.option._
import io.circe.syntax._
import io.replicant.Storage._
import io.replicant.common.syntax.json._
import scalikejdbc._

trait Storage {
  def get(key: String): Value
  def put(key: String, hash: Int, value: Option[String], clock: Map[String, Int]): Unit
  def range(fromHash: Int, toHash: Int, count: Int): Seq[Entry]
}

object Storage {
  final case class Value(data: Option[String], clock: Map[String, Int])
  final case class Entry(key: String, value: String, hash: Int, clock: Map[String, Int])
}

class EmbeddedDbStorage(file: String) extends Storage {
  Class.forName("org.h2.Driver")

  ConnectionPool.add(
    name = file,
    url = s"jdbc:h2:$file",
    user = "user",
    password = "password",
    settings = ConnectionPoolSettings(initialSize = 1, maxSize = 1)
  )

  private implicit val session: DBSession = NamedAutoSession(file)

  sql"""
  CREATE TABLE IF NOT EXISTS data (
    key VARCHAR(1024),
    value TEXT,
    hash INT NOT NULL,
    clock TEXT NOT NULL,
    PRIMARY KEY (key)
  );

  CREATE INDEX IF NOT EXISTS data_hash_idx
  ON data (hash);
  """.execute().apply()

  override def get(key: String): Value =
    sql"""
    SELECT value, clock
    FROM data
    WHERE key = $key
    """
      .map(r => (r.stringOpt("value"), r.string("clock").unsafeAs[Map[String, Int]]))
      .single()
      .apply()
      .fold(Value(none, Map.empty))(Value.tupled)

  override def put(key: String, hash: Int, value: Option[String], clock: Map[String, Int]): Unit = {
    sql"""
      MERGE INTO data KEY (key)
      VALUES ($key, $value, $hash, ${clock.asJson.noSpaces})
      """.update().apply()
    ()
  }

  override def range(fromHash: Int, toHash: Int, count: Int): Seq[Entry] =
    sql"""
    SELECT key, value, hash, clock
    FROM data
    WHERE hash >= $fromHash AND hash < $toHash
    LIMIT $count
    """
      .map(r => Entry(r.string("key"), r.string("value"), r.int("hash"), r.string("clock").unsafeAs[Map[String, Int]]))
      .toList()
      .apply()
}
