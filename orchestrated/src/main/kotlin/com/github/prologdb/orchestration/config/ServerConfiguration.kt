package com.github.prologdb.orchestration.config

import com.github.prologdb.orchestration.config.validation.FilePermission
import com.github.prologdb.orchestration.config.validation.FileType
import com.github.prologdb.orchestration.config.validation.ValidatedPath
import java.nio.file.Path
import javax.validation.constraints.NotNull
import javax.validation.constraints.Size

data class ServerConfiguration(
    val network: NetworkInterfaceConfiguration = NetworkInterfaceConfiguration(),

    @get:ValidatedPath(
        type = FileType.DIRECTORY,
        permissions = [FilePermission.READ, FilePermission.WRITE]
    )
    @get:NotNull
    val dataDirectory: Path?
)

data class NetworkInterfaceConfiguration(
    @get:Size(min = 1, max = 65535)
    val port: Int = 30001
)