"""Versioned backend contract constants and structured diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CONTRACT_EPOCH = 2
PIPELINE_SCHEMA = "windex.pipeline/1"
SEARCH_SOURCE_CONTRACT = "windex.search-source/1"
REGISTRY_CONTRACT = "windex.registry/3"

Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class ValidationIssue:
    path: str
    code: str
    severity: Severity
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "path": self.path,
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
        }


class ContractError(ValueError):
    """One or more stable, editor-addressable contract diagnostics."""

    def __init__(self, issues: list[ValidationIssue] | tuple[ValidationIssue, ...]):
        if not issues:
            raise ValueError("ContractError requires at least one issue")
        self.issues = tuple(issues)
        super().__init__("; ".join(issue.message for issue in self.issues))


def issue(path: str, code: str, message: str, *,
          severity: Severity = "error") -> ValidationIssue:
    return ValidationIssue(
        path=path,
        code=code,
        severity=severity,
        message=message,
    )
