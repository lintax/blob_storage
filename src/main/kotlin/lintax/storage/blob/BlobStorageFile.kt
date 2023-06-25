package lintax.storage.blob

import java.io.EOFException
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.util.*
import java.util.concurrent.Semaphore

private const val MASK_DELETED = 0x01
private const val MASK_USER_DATA = 0xf0

private const val MAX_KEY_LEN = 2048
private const val VACUUM_BUF_SIZE = 8192

/**
random access file containing the following structure:
- mask    1 byte      marker of the record (bit 1 mean deleted)
- keyLen  2 bytes     length of the key
- key     N bytes     key
- dataLen 4 bytes     length of the data
- data    N bytes     data
 */
internal class InnerBlobStorage(
    /** file to use as database */
    private val dbFile: File,

    /** separator to use for splitting key into groups like (/some/path/to/key) to implement hierarchy */
    private val keyPartSeparator: String = "/",

    private val onError: ((msg: String?, e: Throwable?) -> Unit)?,

    private val minPercentToVacuumOnOpen: Int,
    private val minRecordsToConsiderVacuumingOnOpen: Int,

    private val dataProcessorOnSave: ((ByteArray) -> Pair<Int, ByteArray>)?,
    private val dataProcessorOnLoad: ((mask: Int, data: ByteArray) -> ByteArray)?,
) {

    private var f = RandomAccessFile(dbFile, "rw")

    @Volatile
    private var index: SortedMap<CacheKey, Int> = TreeMap()
    private val lock = Semaphore(1)
    private var deleted = 0
    private val utfDecoder = lazy { Charsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE) }

    // if file has invalid record - means that not fully written, maintain last byte of last valid record
    // to use when appending new data, instead of pushing to the end of the file
    private var fileSoftTruncatedPos: Long? = null

    /** opens database and returns whatever vacuum is needed */
    fun open(): VacuumParams? {
        lock.acquire()
        var currentOffset = 0
        try {
            deleted = 0
            val index = TreeMap<CacheKey, Int>()
            val keyBuf = ByteArray(MAX_KEY_LEN)
            while (true) {
                val pos = f.filePointer
                currentOffset = pos.toInt()
                fileSoftTruncatedPos = pos

                // read mask
                val mask = f.read()
                if (mask == -1) {
                    // EOF reached
                    break
                }

                // read key
                val keyLength = f.readShort().toInt()
                if (keyLength == -1) {
                    return VacuumParams(currentOffset)
                }
                val bytesRead = f.read(keyBuf, 0, keyLength)
                if (bytesRead != keyLength) {
                    return VacuumParams(currentOffset)
                }

                // read data (actually just skip it right now)
                val dataSize = f.readInt()
                if (dataSize == -1) {
                    return VacuumParams(currentOffset)
                }
                val bytesSkipped = f.skipBytes(dataSize)
                if (bytesSkipped != dataSize) {
                    return VacuumParams(currentOffset)
                }

                if (mask and MASK_DELETED == MASK_DELETED) {
                    deleted++
                    continue
                }

                val cacheKey = keyBuf.toCacheKey(keyLength)
                index[cacheKey] = currentOffset
                fileSoftTruncatedPos = null
            }
            this.index = index
        } catch (e: Throwable) {
            if (e is EOFException) {
                return VacuumParams(currentOffset)
            }
            onError?.invoke(e.message, e)
        } finally {
            lock.release()
        }

        val total = deleted + index.size
        if (total >= minRecordsToConsiderVacuumingOnOpen && deleted >= total * minPercentToVacuumOnOpen / 100) {
            return VacuumParams()
        }
        return null
    }

    /** runs vacuum operation copying content to a new file, renaming then it to the original one */
    fun vacuum(params: VacuumParams) {
        lock.acquire()
        try {
            val vacuumFile = File(dbFile.parentFile, dbFile.name + ".vacuum")
            if (vacuumFile.exists()) vacuumFile.delete()

            val copy = RandomAccessFile(vacuumFile, "rw")
            f.seek(0)

            val index = TreeMap<CacheKey, Int>()
            val buf = ByteArray(VACUUM_BUF_SIZE)
            while (true) {
                val offset = copy.filePointer.toInt()
                if (params.maxOffset != null && offset >= params.maxOffset) {
                    // we are restricted to this offset (the rest seems to be broken)
                    break
                }

                val mask = f.read()
                if (mask == -1) break
                val isDeleted = mask and MASK_DELETED == MASK_DELETED

                val keyLength = f.readShort().toInt()
                if (isDeleted) {
                    f.skipBytes(keyLength)
                } else {
                    f.read(buf, 0, keyLength)
                }

                val dataSize = f.readInt()
                if (isDeleted) {
                    f.skipBytes(dataSize)
                    continue
                }

                val cacheKey = buf.toCacheKey(keyLength)
                index[cacheKey] = offset

                copy.writeByte(mask)
                copy.writeShort(keyLength)
                copy.write(buf, 0, keyLength)
                copy.write(dataSize)
                var bytesWritten = 0
                while (bytesWritten < dataSize) {
                    val toRead = minOf(buf.size, dataSize - bytesWritten)
                    f.read(buf, 0, toRead)
                    copy.write(buf, 0, toRead)
                    bytesWritten += toRead
                }
            }

            copy.fd.sync()
            copy.close()
            f.close()

            dbFile.delete()
            vacuumFile.renameTo(dbFile)

            f = RandomAccessFile(dbFile, "rw")
            this.index = index
        } catch (e: Throwable) {
            onError?.invoke(e.message, e)
            return
        } finally {
            lock.release()
        }
    }

    fun get(key: String): ByteArray? {
        val cacheKey = key.toCacheKey()
        val pos = index[cacheKey] ?: return null
        lock.acquire()

        val mask: Int
        val buf: ByteArray

        try {
            f.seek(pos.toLong())
            mask = f.read()
            val keyLength = f.readShort().toInt() // key length
            f.skipBytes(keyLength) // key
            when (val dataSize = f.readInt()) {
                0 -> return null
                else -> {
                    buf = ByteArray(dataSize)
                    f.read(buf)
                }
            }
        } finally {
            lock.release()
        }
        return dataProcessorOnLoad?.invoke(mask and MASK_USER_DATA, buf) ?: buf
    }

    fun scan(keyAcceptor: (String) -> Boolean): List<String> {
        return index.keys.asSequence().map { it.toString() }.filter { keyAcceptor.invoke(it) }.toList()
    }

    fun scanGroup(group: String, keyAcceptor: (String) -> Boolean): List<String> {
        return index.subMap(group.toCacheKey(), (group + "a").toCacheKey()).asSequence().map { it.key.toString() }.filter { keyAcceptor.invoke(it) }.toList()
    }

    fun delete(key: String): Boolean {
        val cacheKey = key.toCacheKey()
        val pos = index[cacheKey] ?: return false
        lock.acquire()
        try {
            f.seek(pos.toLong())
            var mask = f.read()
            mask = mask or MASK_DELETED
            f.seek(pos.toLong())
            f.writeByte(mask)

            val newIndex = TreeMap(index).also { it.remove(cacheKey) }
            this.index = newIndex
            deleted++

            f.fd.sync()
            return true
        } finally {
            lock.release()
        }
    }

    fun write(key: String, data: ByteArray) {
        if (data.isEmpty()) {
            delete(key)
            return
        }

        val userChangedData = dataProcessorOnSave?.invoke(data)
        val maskToSave = (userChangedData?.first ?: 0) and MASK_USER_DATA

        val cacheKey = key.toCacheKey()
        lock.acquire()
        try {
            val pos = fileSoftTruncatedPos ?: f.length()
            f.seek(pos)

            f.writeByte(maskToSave)
            val keyBytes = cacheKey.toBytes()
            f.writeShort(keyBytes.size)
            f.write(keyBytes)

            val processed = userChangedData?.second ?: data

            f.writeInt(processed.size)
            f.write(processed)

            val newIndex = TreeMap(index)
            val oldItemPos = newIndex.put(cacheKey, pos.toInt())
            this.index = newIndex
            if (oldItemPos != null) {
                f.seek(oldItemPos.toLong())
                var mask = f.read()
                mask = mask or MASK_DELETED
                f.seek(oldItemPos.toLong())
                f.writeByte(mask)
                deleted++
            }

            fileSoftTruncatedPos = null
            f.fd.sync()
        } finally {
            lock.release()
        }
    }

    private inner class CacheKey(val path: Array<String>) : Comparable<CacheKey> {
        override fun equals(other: Any?): Boolean = this === other || toString() == other.toString()
        override fun hashCode(): Int = path.contentHashCode()
        override fun toString(): String = path.joinToString(separator = keyPartSeparator)
        override fun compareTo(other: CacheKey): Int = toString().compareTo(other.toString())
        fun toBytes(): ByteArray = toString().toByteArray()
    }

    private fun String.toCacheKey(): CacheKey {
        return CacheKey(split(keyPartSeparator).toTypedArray())
    }

    private fun ByteArray.toCacheKey(len: Int): CacheKey {
        val key = utfDecoder.value.decode(ByteBuffer.wrap(this, 0, len)).toString()
        return key.toCacheKey()
    }
}
