package io.replicant

import kamon.Kamon
import scalikejdbc._

trait Storage {
  def put(key: String, value: String, meta: Meta): Unit
  def get(key: String): Option[(String, Meta)]
  def del(key: String): Unit
}

final case class Meta(version: Long, modificationTime: Long, deleted: Boolean)

class EmbeddedDbStorage(filename: String) extends Storage {
  Class.forName("org.h2.Driver")

  ConnectionPool.add(
    name = filename,
    url = s"jdbc:h2:$filename",
    user = "user",
    password = "password",
    settings = ConnectionPoolSettings(initialSize = 1, maxSize = 1)
  )

  private implicit val session: DBSession = NamedAutoSession(filename)

  sql"""
  CREATE TABLE IF NOT EXISTS data (
    key VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL,
    modification_time BIGINT NOT NULL,
    deleted BOOLEAN,
    PRIMARY KEY (key, version)
  )
  """.execute().apply()

  override def get(key: String): Option[(String, Meta)] = {
    Kamon.counter("storage.get").increment()
    sql"""
    SELECT value, version, modification_time, deleted FROM data
    WHERE key = $key AND version = (
      SELECT MAX(version) FROM data WHERE key = $key
    )
    """
      .map(r => (r.string("value"), Meta(r.long("version"), r.long("modification_time"), r.boolean("deleted"))))
      .single()
      .apply()
  }

  override def put(key: String, value: String, meta: Meta): Unit = {
    Kamon.counter("storage.put").increment()
    sql"""
    INSERT INTO data (key, value, version, modification_time, deleted)
    VALUES ($key, $value, ${meta.version}, ${meta.modificationTime}, ${meta.deleted})
    """.update().apply()
    ()
  }

  override def del(key: String): Unit = {
    Kamon.counter("storage.del").increment()
    sql"""
    DELETE FROM data
    WHERE key = $key
    """.update().apply()
    ()
  }
}
