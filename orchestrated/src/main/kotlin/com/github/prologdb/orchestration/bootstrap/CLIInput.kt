package com.github.prologdb.orchestration.bootstrap

import com.github.prologdb.orchestration.config.*
import com.github.prologdb.orchestration.config.validation.FilePermission
import com.github.prologdb.orchestration.config.validation.FileType
import com.github.prologdb.orchestration.config.validation.ValidatedPath
import java.nio.file.Path
import java.nio.file.Paths

internal fun CLIInput.buildConfig(): ServerConf {
    val configText = configFile?.let { it.toFile().readText(Charsets.UTF_8) } ?: "{}"
    val configurationFromFile = configText.parseAsConfig<ServerConf>()

    return configurationFromFile.plusOverrides(overrides)
}

internal data class CLIInput(
    /** the config file; if omitted: use defaults */
    @get:ValidatedPath(
        type = FileType.FILE,
        permissions = [FilePermission.READ]
    )
    val configFile: Path?,

    /** overrides for the config file */
    val overrides: Map<ObjectPath<ServerConf>, Any>
)

internal fun parseCLI(args: Array<String>): CLIInput {
    if (args.isEmpty()) return CLIInput(null, emptyMap())
    val configString = args[0].trim()

    val configFile = if (configString == "-") null else Paths.get(configString)

    if (args.size <= 1) {
        return CLIInput(configFile, emptyMap())
    }

    val overrides = mutableMapOf<String, String>()
    for (arg in args[1..args.lastIndex]) {
        val equalsSignPos = arg.indexOf('=')
        val key: String
        val valueAsString: String
        if (equalsSignPos == -1) {
            key = arg
            valueAsString = "true"
        } else {
            key = arg.substring(0, equalsSignPos)
            valueAsString = arg.substring(equalsSignPos + 1)
        }

        overrides[key] = valueAsString
    }

    return CLIInput(configFile, overrides.parseAsOverrides())
}

private operator fun <T> Array<T>.get(range: IntRange): Iterable<T> {
    assert(range.start >= 0)
    assert(range.endInclusive <= lastIndex)

    return range.map(this::get)
}