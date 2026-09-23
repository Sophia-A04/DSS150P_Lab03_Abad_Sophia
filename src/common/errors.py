import logging
from functools import wraps
from inspect import signature


logger = logging.getLogger("dss150p.pipeline")


class PipelineStageError(RuntimeError):
    """Raised when a pipeline stage fails because of a system/runtime error."""


def stage_error(stage_name: str):
    """Add stage/run context while preserving the original exception."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            run_id = None

            try:
                bound = signature(func).bind_partial(*args, **kwargs)
                run_id = bound.arguments.get("run_id")
            except (TypeError, ValueError):
                pass

            try:
                return func(*args, **kwargs)

            except PipelineStageError:
                raise

            except Exception as exc:
                context = f"stage={stage_name}"

                if run_id:
                    context += f", run_id={run_id}"

                logger.exception(
                    "Pipeline stage failed: %s",
                    context,
                )

                message = (
                    f"Pipeline stage '{stage_name}' failed"
                )

                if run_id:
                    message += f" (run_id={run_id})"

                message += f": {exc}"

                raise PipelineStageError(message) from exc

        return wrapper

    return decorator