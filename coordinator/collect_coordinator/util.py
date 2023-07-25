import logging

from bitmath import Byte, MiB
from cattrs import register_structure_hook, register_unstructure_hook


def setup_process() -> None:
    logging.basicConfig(level=logging.INFO)


# Register json structure/unstructure hooks
register_structure_hook(Byte, lambda v: MiB(v).best_prefix())  # type: ignore
register_unstructure_hook(Byte, lambda b: int(b.to_MiB().value))
