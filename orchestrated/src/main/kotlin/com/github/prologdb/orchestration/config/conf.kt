package com.github.prologdb.orchestration.config

import com.github.prologdb.orchestration.config.validation.FilePermission
import com.github.prologdb.orchestration.config.validation.FileType
import com.github.prologdb.orchestration.config.validation.ValidatedPath
import java.nio.file.Path
import java.nio.file.Paths
import javax.validation.Valid
import javax.validation.constraints.Max
import javax.validation.constraints.Min
import javax.validation.constraints.NotNull

data class ServerConf(
    @get:Valid
    val network: NetworkIConf = NetworkIConf(),

    @get:ValidatedPath(
        type = FileType.DIRECTORY,
        permissions = [FilePermission.READ, FilePermission.WRITE]
    )
    @get:NotNull(message = "You must specify a data directory")
    val dataDirectory: Path? = Paths.get(".").toAbsolutePath()
)

data class NetworkIConf(
    @get:Min(1)
    @get:Max(65535)
    val port: Int = 30001,

    /**
     * The maximum number of concurrent connections to allow.
     * Set null to not restrict the number of connections.
     */
    @get:Min(0)
    val maxConnections: Int? = null
)