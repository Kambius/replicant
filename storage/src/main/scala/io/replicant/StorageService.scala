package io.replicant

trait StorageService {
  def get(key: String): Option[String]
  def put(key: String, value: String): Unit
  def del(key: String): Unit
}

class BasicStorageService(storage: Storage) extends StorageService {
  override def get(key: String): Option[String] =
    storage.get(key).collect { case (v, m) if !m.deleted => v }

  override def put(key: String, value: String): Unit =
    storage.get(key) match {
      case None =>
        storage.put(key, value, Meta(1, System.nanoTime(), deleted = false))

      case Some((_, m)) =>
        storage.put(key, value, Meta(m.version + 1, System.nanoTime(), deleted = false))

    }

  override def del(key: String): Unit =
    storage.get(key) match {
      case None         => storage.put(key, null, Meta(1, System.nanoTime(), deleted = true))
      case Some((v, m)) => storage.put(key, v, Meta(m.version + 1, System.nanoTime(), deleted = true))
    }
}
