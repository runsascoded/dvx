from typing import Any, AnyStr

StrPath = str | "PathLike[str]"
BytesPath = bytes | "PathLike[bytes]"
GenericPath = AnyStr | "PathLike[AnyStr]"
StrOrBytesPath = str | bytes | "PathLike[str]" | "PathLike[bytes]"

TargetType = list[str] | str
DictStrAny = dict[str, Any]
