from typing import TYPE_CHECKING, Any, AnyStr, Union

if TYPE_CHECKING:
    from os import PathLike

# Must use Union for string forward refs - `str | "PathLike[str]"` fails at runtime
StrPath = Union[str, "PathLike[str]"]
BytesPath = Union[bytes, "PathLike[bytes]"]
GenericPath = Union[AnyStr, "PathLike[AnyStr]"]
StrOrBytesPath = Union[str, bytes, "PathLike[str]", "PathLike[bytes]"]

TargetType = list[str] | str
DictStrAny = dict[str, Any]
