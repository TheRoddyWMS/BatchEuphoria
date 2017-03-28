package de.dkfz.eilslabs.batcheuphoria.config

import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

@CompileStatic
class ResourceSet {
    private final String queue
    private ResourceSetSize size
    /**
     * The target memory value.
     */
    private BufferValue mem
    private BufferValue memMax

    private Integer cores
    private Integer coresMax
    private Integer nodes
    private Integer nodesMax
    private TimeUnit walltime

    /**
     * Hard disk storage used.
     */
    private BufferValue storage
    private BufferValue storageMax
    private String additionalNodeFlag

    ResourceSet(ResourceSetSize size, BufferValue mem, Integer cores, Integer nodes, TimeUnit walltime, BufferValue storage, String queue, String additionalNodeFlag) {
        this.size = size
        this.mem = mem
        this.cores = cores
        this.nodes = nodes
        this.walltime = walltime
        this.storage = storage
        this.queue = queue
        this.additionalNodeFlag = additionalNodeFlag
    }

    ResourceSetSize getSize() {
        return size
    }

    ResourceSet clone() {
        return new ResourceSet(size, mem, cores, nodes, walltime, storage, queue, additionalNodeFlag)
    }

    BufferValue getMem() {
        return mem
    }

    Integer getCores() {
        return cores
    }

    Integer getNodes() {
        return nodes
    }

    BufferValue getStorage() {
        return storage
    }

    boolean isMemSet() {
        return mem != null
    }

    boolean isCoresSet() {
        return cores != null
    }

    boolean isNodesSet() {
        return nodes != null
    }

    boolean isStorageSet() {
        return storage != null
    }

    TimeUnit getWalltime() {
        return walltime
    }

    boolean isWalltimeSet() {
        return walltime != null
    }

    boolean isQueueSet() {
        return !RoddyConversionHelperMethods.isNullOrEmpty(queue)
    }

    String getQueue() {
        return queue
    }

    boolean isAdditionalNodeFlagSet() {
        return !RoddyConversionHelperMethods.isNullOrEmpty(additionalNodeFlag)
    }

    String getAdditionalNodeFlag() {
        return additionalNodeFlag
    }
}
