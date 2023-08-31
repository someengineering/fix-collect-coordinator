import logging
from argparse import Namespace

from bitmath import Byte, MiB
from cattrs import register_structure_hook, register_unstructure_hook


def setup_process(args: Namespace) -> None:
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger("arq.worker").setLevel(logging.CRITICAL)
    else:
        logging.basicConfig(level=logging.INFO)


# Register json structure/unstructure hooks
register_structure_hook(Byte, lambda v: MiB(v).best_prefix())  # type: ignore
register_unstructure_hook(Byte, lambda b: int(b.to_MiB().value))
