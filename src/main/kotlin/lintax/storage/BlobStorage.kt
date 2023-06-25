package lintax.storage

import lintax.storage.blob.InnerBlobStorage
import lintax.storage.blob.VacuumParams
import java.io.File
import kotlin.math.abs

class BlobStorage(
    private val root: File,

    /**
     * number of shards to split data to multiple files
     * the more sharding, the fewer collisions and faster the processing with more files created
     * */
    private val numOfShards: Int = 256,
    /**
     * function to select a shard for storing/loading the key
     * */
    private val shardingFunc: (key: String) -> Int = { key -> abs(key.hashCode()) % numOfShards },

    /**
     * error handler to log an issue/etc
     * */
    private val onError: ((msg: String?, e: Throwable?) -> Unit)? = null,

    /**
     * minimal deleted items percent to start vacuum on database open
     * */
    private val minPercentToVacuumOnOpen: Int = 85,
    private val minRecordsToConsiderVacuumingOnOpen: Int = 1009,

    /**
     * allows to process the data before saving (like compressing, or encrypting)
     * if the data was processed it's highly recommended to set bits for the reader
     * app to correctly process the data.
     * user is allowed to use bits 0xf0, meaning that 0x10, 0x20, 0x40 and 0x80 are
     * available for marking data
     *
     * @returns pair of mask and processed byte array
     * if you did not modify the data, then you should return a pair of 0 to original byte array
     */
    private val dataProcessorOnSave: ((ByteArray) -> Pair<Int, ByteArray>)? = null,

    /**
     * allows to process the data on loading (like decompressing or decrypting)
     * if the data was processed it's assumed that the flags for this operation
     * has been saved and returned with mask 0xf0 in this delegate
     *
     * @returns modified or original bytearray
     */
    private val dataProcessorOnLoad: ((mask: Int, data: ByteArray) -> ByteArray)? = null,
) {
    private val shards: Array<InnerBlobStorage>

    init {
        root.mkdirs()

        shards = (1..numOfShards).map { index ->
            val file = File(root, Integer.toHexString(index))
            InnerBlobStorage(
                file,
                onError = { msg, e ->
                    onError?.invoke("$index: $msg", e)
                },
                minPercentToVacuumOnOpen = minPercentToVacuumOnOpen,
                minRecordsToConsiderVacuumingOnOpen = minRecordsToConsiderVacuumingOnOpen,
                dataProcessorOnSave = dataProcessorOnSave,
                dataProcessorOnLoad = dataProcessorOnLoad,
            )
        }.toTypedArray()

        // open all shards, reading index
        val toVacuum = mutableListOf<Pair<InnerBlobStorage, VacuumParams>>()
        shards.forEach { shard ->
            val vacuumParams = shard.open()
            if (vacuumParams != null) toVacuum.add(shard to vacuumParams)
        }

        if (toVacuum.isNotEmpty()) {
            // vacuum shards with too much empty slots
            Runnable {
                toVacuum.forEach { (shard, params) ->
                    shard.vacuum(params)
                }
            }.runInThread("bstr-vac")
        }
    }

    /** get value */
    fun get(key: String): ByteArray? {
        val shard = shards[shardingFunc.invoke(key)]
        return shard.get(key)
    }

    /** scans DB for keys that are accepted */
    fun scan(keyAcceptor: (String) -> Boolean): List<String> {
        val out = ArrayList<String>()
        shards.forEach { out.addAll(it.scan(keyAcceptor)) }
        return out
    }

    /** scans DB for keys starting with the group prefix that are accepted (like "path/to" will match "path/to/key" */
    fun scanGroup(group: String, keyAcceptor: (String) -> Boolean): List<String> {
        val out = ArrayList<String>()
        shards.forEach { out.addAll(it.scanGroup(group, keyAcceptor)) }
        return out
    }

    /** deletes a single record */
    fun delete(key: String): Boolean {
        val shard = shards[shardingFunc.invoke(key)]
        return shard.delete(key)
    }

    /** stores the data */
    fun write(key: String, data: ByteArray) {
        val shard = shards[shardingFunc.invoke(key)]
        shard.write(key, data)
    }
}

private fun Runnable.runInThread(name: String) = Thread(this, name).start()
