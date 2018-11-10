package com.github.prologdb.orchestration.config

import com.github.prologdb.orchestration.config.validation.FilePermission
import com.github.prologdb.orchestration.config.validation.FileType
import com.github.prologdb.orchestration.config.validation.ValidatedPath
import java.nio.file.Path
import javax.validation.constraints.Size

data class ServerConfiguration(
    @get:Size(min = 1, max = 65535)
    val port: Int = 30001,

    @get:ValidatedPath(
        type = FileType.DIRECTORY,
        permissions = [FilePermission.READ, FilePermission.WRITE]
    )
    val dataDirectory: Path
)