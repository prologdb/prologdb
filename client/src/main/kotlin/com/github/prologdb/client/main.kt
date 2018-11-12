package com.github.prologdb.client

import com.tmarsteel.jcli.Environment
import com.tmarsteel.jcli.Input

fun main(args: Array<String>) {
    val input = Input(Environment.UNIX, args)
    val hostname = input.options()["host"]?.first() ?: "localhost"
    val port = input.options()["port"]?.first()?.toInt() ?: 30001


}