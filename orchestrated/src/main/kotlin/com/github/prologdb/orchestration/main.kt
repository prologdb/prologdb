package com.github.prologdb.orchestration

import com.github.prologdb.orchestration.config.*
import com.github.prologdb.orchestration.config.validation.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths

private fun printUsage() {
    println("""
        Usage:
        prologdb-server [<path to config file>|-] [<<config parameter name>=<override value>>...]

        Examples:

        prologdb-server - port=42621

        prologdb-server /path/to/alternative/configuration.yml
    """.trimIndent())
}

private val log = LoggerFactory.getLogger("cli")

fun main(args: Array<String>) {


    val input = try {
        refuseInvalid(parseCLI(args))
    }
    catch (ex: ObjectPathException) {
        log.error("Failed to parse overrides: ${ex.message}", ex)
        System.exit(1)
        return
    }
    catch (ex: OverrideException) {
        log.error("Failed to parse overrides: ${ex.message}", ex)
        System.exit(1)
        return
    }
    catch (ex: ValidationException) {
        log.logViolationsError(ex, "Invalid command line input")

        printUsage()

        System.exit(1)
        return
    }

    val configuration = try {
        refuseInvalid(input.buildConfig())
    }
    catch (ex: IOException) {
        log.error("Failed to read configuration: ${ex.message}", ex)
        System.exit(-1)
        return
    }
    catch (ex: ValidationException) {
        log.logViolationsError(ex, "Configuration (after applying overrides) is not valid")
        System.exit(-1)
        return
    }
}

private fun CLIInput.buildConfig(): ServerConf {
    val configText = configFile?.let { it.toFile().readText(Charsets.UTF_8) } ?: "{}"
    val configurationFromFile = configText.parseAsConfig<ServerConf>()

    return configurationFromFile.plusOverrides(overrides)
}

private data class CLIInput(
    /** the config file; if omitted: use defaults */
    @get:ValidatedPath(
        type = FileType.FILE,
        permissions = [FilePermission.READ]
    )
    val configFile: Path?,

    /** overrides for the config file */
    val overrides: Map<ObjectPath<ServerConf>, Any>
)


private fun parseCLI(args: Array<String>): CLIInput {
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