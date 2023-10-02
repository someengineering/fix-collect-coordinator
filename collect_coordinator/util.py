# fix-collect-coordinator
# Copyright (C) 2023  Some Engineering
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
from argparse import Namespace

from bitmath import Byte, MiB
from cattrs import register_structure_hook, register_unstructure_hook
from fixcloudutils.service import Dependencies
from fixcloudutils.logging import setup_logger


class CollectDependencies(Dependencies):
    @property
    def args(self) -> Namespace:
        return self.service("args", Namespace)

    @property
    def redis_event_url(self) -> str:
        return f"{self.args.redis_url_nodb}/{self.args.redis_event_db}"

    @property
    def redis_worker_url(self) -> str:
        return f"{self.args.redis_url_nodb}/{self.args.redis_worker_db}"


def setup_process(args: Namespace) -> None:
    level = logging.DEBUG if args.debug else logging.INFO
    setup_logger("collect.coordinator", level=level)
    logging.getLogger("arq.worker").setLevel(logging.CRITICAL)


# Register json structure/unstructure hooks
register_structure_hook(Byte, lambda v: MiB(v).best_prefix())  # type: ignore
register_unstructure_hook(Byte, lambda b: int(b.to_MiB().value))
