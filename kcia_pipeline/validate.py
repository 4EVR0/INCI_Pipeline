from models import ValidationResult


def validate(rows, stats, settings):
    if len(rows) == 0:
        return ValidationResult(False, "No rows collected")

    if settings.strict_count_check:
        if stats.total_expected != stats.total_collected:
            return ValidationResult(
                False,
                f"Count mismatch: expected {stats.total_expected}, got {stats.total_collected}",
            )

    return ValidationResult(True, "OK")