package com.github.prologdb.orchestration.config.validation

import java.nio.file.Files
import java.nio.file.Path
import javax.validation.Constraint
import javax.validation.ConstraintValidator
import javax.validation.ConstraintValidatorContext
import javax.validation.Payload
import kotlin.annotation.AnnotationRetention.RUNTIME
import kotlin.annotation.AnnotationTarget.*
import kotlin.reflect.KClass

@Target(FUNCTION, PROPERTY_GETTER, PROPERTY_SETTER, FIELD, ANNOTATION_CLASS, CONSTRUCTOR, VALUE_PARAMETER, TYPE)
@Retention(RUNTIME)
@Constraint(validatedBy = [PathConstraintValidator::class])
annotation class ValidatedPath(
    val type: FileType = FileType.FILE,
    val permissions: Array<FilePermission> = arrayOf(FilePermission.READ),
    val message: String = "Path must point to an existing {type} with {permissions} permissions",

    val groups: Array<KClass<*>> = emptyArray(),
    val payload: Array<KClass<out Payload>> = emptyArray()
)

enum class FileType(val test: (Path) -> Boolean) {
    FILE({ Files.isRegularFile(it) }),
    DIRECTORY({ Files.isDirectory(it) })
}

enum class FilePermission(val test: (Path) -> Boolean) {
    READ(Files::isReadable),
    WRITE(Files::isWritable),
    EXECUTE(Files::isExecutable)
}

class PathConstraintValidator : ConstraintValidator<ValidatedPath, Path> {

    var current: ValidatedPath? = null

    override fun initialize(constraintAnnotation: ValidatedPath?) {
        current = constraintAnnotation
    }

    override fun isValid(value: Path?, context: ConstraintValidatorContext?): Boolean {
        val current = this.current ?: throw IllegalStateException("Not initialized")

        if (value == null) return true // to be rejected by @NotNull

        return current.type.test(value) && current.permissions.all { it.test(value) }
    }
}
