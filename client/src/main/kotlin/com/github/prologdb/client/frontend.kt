package com.github.prologdb.client

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.forEachRemaining
import com.github.prologdb.async.mapRemaining
import com.github.prologdb.net.util.prettyPrint
import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.PrologNumber
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.util.DefaultOperatorRegistry
import com.github.prologdb.runtime.util.OperatorDefinition
import com.github.prologdb.runtime.util.OperatorRegistry
import com.github.prologdb.runtime.util.OperatorType

class CLIFrontend(private val connection: Connection) {
    fun run() {
        var mode = InstructionMode.QUERY
        var operators = ISOOpsOperatorRegistry

        repl@while (true) {
            print("${mode.operator} ")
            var instruction = (readLine() ?: "") .trim()

            var singleMode = mode
            findMode@for (availableMode in InstructionMode.values()) {
                if (instruction == availableMode.operator) {
                    mode = availableMode
                    continue@repl
                }
                else if (instruction.startsWith(availableMode.operator)) {
                    singleMode = availableMode
                    instruction = instruction.substring(availableMode.operator.length).trim()
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
                        transform = { "${it.first} = ${it.second?.toStringUsingOperatorNotations(operators)}"}
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
            if (singleMode == InstructionMode.DIRECTIVE) {
                try {
                    operators = fetchOperators()
                }
                catch (ex: GenericQueryError) {

                }
            }
        }
    }

    private fun init(instruction: String, mode: InstructionMode): LazySequence<Unification> {
        return when (mode) {
            InstructionMode.QUERY     -> connection.startQuery(instruction)
            InstructionMode.DIRECTIVE -> connection.startDirective(instruction)
        }
    }

    private fun fetchOperators(): OperatorRegistry {
        val operatorSolutions = init("current_op(Precedence, Associativity, Name)", InstructionMode.QUERY)
        val registry = DefaultOperatorRegistry()
        operatorSolutions
            .mapRemaining { solution -> OperatorDefinition(
                (solution.variableValues[Variable("Precedence")] as PrologNumber).toInteger().toShort(),
                OperatorType.valueOf((solution.variableValues[Variable("Associativity")] as Atom).name.uppercase()),
                (solution.variableValues[Variable("Name")] as Atom).name
            ) }
            .forEachRemaining(registry::defineOperator)

        return registry
    }
}

enum class InstructionMode(val operator: String) {
    QUERY("?-"), DIRECTIVE(":-")
}