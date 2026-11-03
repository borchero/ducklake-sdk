from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from .typedefs import QualifiedName

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ._native import PyMacro

MacroType = Literal["scalar", "table"]


class Macro:
    """A scalar or table macro stored in a DuckLake catalog."""

    _pymacro: PyMacro

    @property
    def name(self) -> QualifiedName:
        """The fully-qualified name of the macro."""
        return QualifiedName(*self._pymacro.name)

    @property
    def macro_type(self) -> MacroType:
        """Whether the macro returns a scalar value or a table."""
        return self._pymacro.macro_type  # ty: ignore[invalid-return-type]

    @property
    def implementations(self) -> list[MacroImplementation]:
        """All implementations (overloads) of the macro."""
        return [
            MacroImplementation(
                sql,
                [
                    MacroParameter(
                        name,
                        data_type=data_type,
                        default_value=default_value,
                        default_value_type=default_value_type,
                    )
                    for name, data_type, default_value, default_value_type in parameters
                ],
                dialect=dialect,
            )
            for dialect, sql, parameters in self._pymacro.implementations
        ]

    @classmethod
    def _from_pymacro(cls, pymacro: PyMacro) -> Macro:
        macro = cls.__new__(cls)
        macro._pymacro = pymacro
        return macro

    def __repr__(self) -> str:
        return (
            f"Macro(schema={self.name.schema!r}, name={self.name.name!r}, "
            f"macro_type={self.macro_type!r})"
        )


class MacroImplementation:
    """One SQL implementation (overload) of a macro."""

    def __init__(
        self,
        sql: str,
        parameters: Sequence[str | MacroParameter] = (),
        *,
        dialect: str = "duckdb",
    ) -> None:
        self.sql = sql
        self.parameters = [
            parameter if isinstance(parameter, MacroParameter) else MacroParameter(parameter)
            for parameter in parameters
        ]
        self.dialect = dialect

    def __eq__(self, other: object) -> bool:
        return isinstance(other, MacroImplementation) and vars(self) == vars(other)

    def __repr__(self) -> str:
        return f"MacroImplementation({self.sql!r}, {self.parameters!r}, dialect={self.dialect!r})"


class MacroParameter:
    """A parameter accepted by one macro implementation.

    Default values use DuckLake's serialized value format. For example, a varchar default is
    `"hello"` rather than the SQL expression `"'hello'"`.
    """

    def __init__(
        self,
        name: str,
        *,
        data_type: str = "unknown",
        default_value: str | None = None,
        default_value_type: str = "unknown",
    ) -> None:
        if default_value is not None and default_value_type == "unknown":
            raise ValueError("default_value_type is required when default_value is provided")
        if default_value is None and default_value_type != "unknown":
            raise ValueError("default_value_type requires default_value")
        self.name = name
        self.data_type = data_type
        self.default_value = default_value
        self.default_value_type = default_value_type

    def __eq__(self, other: object) -> bool:
        return isinstance(other, MacroParameter) and vars(self) == vars(other)

    def __repr__(self) -> str:
        options = []
        if self.data_type != "unknown":
            options.append(f"data_type={self.data_type!r}")
        if self.default_value is not None:
            options.append(f"default_value={self.default_value!r}")
            options.append(f"default_value_type={self.default_value_type!r}")
        suffix = f", {', '.join(options)}" if options else ""
        return f"MacroParameter({self.name!r}{suffix})"
