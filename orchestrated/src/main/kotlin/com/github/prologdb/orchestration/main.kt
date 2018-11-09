package com.github.prologdb.orchestration

import com.github.prologdb.orchestration.config.validation.*
import java.nio.file.Path
import java.nio.file.Paths

fun main(args: Array<String>) {
    val input = try {
        refuseInvalid(parseCLI(args))
    }
    catch (ex: ValidationException) {
        System.err.println("Invalid input:")
        ex.violations.forEach {
            System.err.println("${it.propertyPath.asYAMLPath}: ${it.message}")
        }
        System.exit(1)
    }

    TODO()
}

private fun parseCLI(args: Array<String>): CLIInput {
    if (args.size == 0) return CLIInput(null, emptyMap())
    val configString = args[0].trim()

    val configFile = if (configString == "-") null else Paths.get(configString)

    if (args.size <= 1) {
        return CLIInput(configFile, emptyMap())
    }

    val overrides = mutableMapOf<String, String>()
    for (arg in args[1..args.lastIndex]) {
        val equalsSignPos = arg.indexOf('=')
        if (equalsSignPos == -1) {
            overrides[arg] = "true"
        } else {
            overrides[arg.substring(0, equalsSignPos)] = arg.substring(equalsSignPos + 1)
        }
    }

    return CLIInput(configFile, overrides)
}

private data class CLIInput(
    /** the config file; if omitted: use defaults */
    @ValidatedPath(
        type = FileType.FILE,
        permissions = [FilePermission.READ]
    )
    val configFile: Path?,

    /** overrides for the config file */
    val overrides: Map<String, String>
)

private operator fun <T> Array<T>.get(range: IntRange): Iterable<T> {
    assert(range.start >= 0)
    assert(range.endInclusive <= lastIndex)

    return range.map(this::get)
}

private class CLISyntaxException(message: String): RuntimeException(message)