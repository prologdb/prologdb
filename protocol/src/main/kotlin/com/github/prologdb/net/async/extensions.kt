package com.github.prologdb.net.async

import com.github.prologdb.net.negotiation.SemanticVersion

fun SemanticVersion.format(): String {
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