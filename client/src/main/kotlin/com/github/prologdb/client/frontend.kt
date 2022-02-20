package com.github.prologdb.client

import com.github.prologdb.async.LazySequence
import com.github.prologdb.net.util.prettyPrint
import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.unification.Unification

class CLIFrontend(private val connection: Connection) {

    private var mode = InstructionMode.QUERY

    fun run() {
        repl@while (true) {
            print("${mode.operator} ")
            var instruction = (readLine() ?: "") .trim()

            var singleMode = mode
            findMode@for (mode in InstructionMode.values()) {
                if (instruction == mode.operator) {
                    this.mode = mode
                    continue@repl
                }
                else if (instruction.startsWith(mode.operator)) {
                    singleMode = mode
                    instruction = instruction.substring(mode.operator.length).trim()
                    break@findMode
                }
            }

            val solutions = init(instruction, singleMode)
            showSolutions@do {
                val solution = try {
                    solutions.tryAdvance()
                }
                catch (ex: Throwable) {
                    println("Error: ${ex.message}")

                    if (ex is GenericQueryError) {
                        for ((key, value) in ex.additionalInformation) {
                            println("$key: $value")
                        }
                    } else if (ex is QueryError) {
                        for ((key, value) in ex.additionalInformation) {
                            println("$key: $value")
                        }
                    } else if (ex is PrologException) {
                        println(ex.prettyPrint())
                    } else {
                        ex.printStackTrace(System.out)
                    }

                    break@showSolutions
                }

                if (solution == null) {
                    println("false")
                    break@showSolutions
                }

                print(
                    solution.variableValues.values.joinToString(
                        separator = " ,\n",
                        transform = { "${it.first} = ${it.second}"}
                    )
                )
                if (solution.variableValues.isEmpty) {
                    print("true")
                }
                print(" ")

                while (true) {
                    val action = (readLine() ?: "").trim()
                    when (action) {
                        ";" -> { continue@showSolutions }
                        "." -> { break@showSolutions }
                        else -> { println("Unknown Action (; .)") }
                    }
                }
            } while (solution != null)

            solutions.close()
        }
    }

    private fun init(instruction: String, mode: InstructionMode): LazySequence<Unification> {
        return when (mode) {
            InstructionMode.QUERY     -> connection.startQuery(instruction)
            InstructionMode.DIRECTIVE -> connection.startDirective(instruction)
        }
    }
}

enum class InstructionMode(val operator: String) {
    QUERY("?-"), DIRECTIVE(":-")
}