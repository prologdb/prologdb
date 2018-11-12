package com.github.prologdb.orchestration.introspect

import com.github.prologdb.net.negotiation.SemanticVersion
import java.nio.charset.Charset
import java.util.*

@get:JvmName("serverVersion")
val SERVER_VERSION: SemanticVersion by lazy {
    getMavenVersionFromFilteredResource()
        ?: getMavenVersionFromMetaInformation()
        ?: deriveVersionFromCurrentGitCommit()
        ?: throw IllegalStateException("Cannot determine current version")
}

/**
 * Attempts to parse the given string as a semantic version.
 * @return The parsed structure or null if the string does not
 *         follow the syntax.
 */
fun String.parseAsSemanticVersion(): SemanticVersion? {
    val result = SEMVER_PATTERN.matchEntire(this) ?: return null

    val builder = SemanticVersion.newBuilder()
        .setMajor(result.groupValues[1].toInt())
        .setMinor(result.groupValues[2].toInt())
        .setPatch(result.groupValues[3].toInt())

    val allLabels = result.groups[4]?.value?.substring(1)?.split('-')
    allLabels?.let { builder.addAllPreReleaseLabels(allLabels) }

    val buildInfo = result.groups[8]?.value
    val buildNumber = buildInfo?.toLongOrNull() // if not numeric, ignore
    buildNumber?.let { builder.setBuildNumber(it) }

    return builder.build()
}
private val SEMVER_PATTERN = Regex("^(\\d+)\\.(\\d+)\\.(\\d+)((-((?![\\-+]).+?))*?)(\\+(.+))?$")

/**
 * Attempts to obtain the maven version from filtered
 * resources.
 * @return the parsed version or null if the version could not be parsed
 */
private fun getMavenVersionFromFilteredResource(): SemanticVersion? {
    val text = object{}.javaClass.getResource("/meta/server_mvn_version").readText(Charsets.UTF_8)
    return text.parseAsSemanticVersion()
}

/**
 * Attempts to read the META-INF/maven/{groupId}/{artifactId}/pom.properties; if successful,
 * parses the version entry.
 */
private fun getMavenVersionFromMetaInformation(): SemanticVersion? {
    return METAINF_POM_PROPERTIES?.getProperty("version")?.parseAsSemanticVersion()
}

private fun deriveVersionFromCurrentGitCommit(): SemanticVersion? = getCurrentGitCommit()?.let {
    SemanticVersion.newBuilder()
        .setMajor(0)
        .setMinor(0)
        .setPatch(0)
        .addPreReleaseLabels("git.rev.$it")
        .build()
}

/**
 * Attempts to get the current git commit.
 */
private fun getCurrentGitCommit(): String? {
    val uri = object{}.javaClass.getResource(".").toExternalForm()
    if (!uri.startsWith("file:")) return null

    val gitProc = Runtime.getRuntime().exec(arrayOf("git", "rev-parse", "HEAD"))
    val revision = gitProc.inputStream.reader(Charset.defaultCharset()).readText().trim()
    gitProc.waitFor()

    return if (revision.matches(Regex("^[a-fA-F0-9]{40,}$"))) {
        revision
    } else null
}

@get:JvmName("metaInformationPomProperties")
private val METAINF_POM_PROPERTIES: Properties? by lazy {
    object{}.javaClass.getResourceAsStream("/META-INF/maven/com.github.prologdb/prologdb-orchestrated/pom.properties")?.use {
        val props = Properties()
        props.load(it)
        props
    }
}

fun SemanticVersion.toFormatted(): String {
    val sb = StringBuilder()
    sb.append(major)
    sb.append('.')
    sb.append(minor)
    sb.append('.')
    sb.append(patch)

    for (preReleaseLabel in preReleaseLabelsList) {
        sb.append('-')
        sb.append(preReleaseLabel)
    }

    if (buildNumber != 0L) {
        sb.append('+')
        sb.append(buildNumber)
    }

    return sb.toString()
}