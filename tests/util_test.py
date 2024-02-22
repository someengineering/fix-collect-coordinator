#  Copyright (c) 2024. Some Engineering
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
import logging

from _pytest.logging import LogCaptureFixture

from collect_coordinator.util import suppress_logging


def test_suppress(caplog: LogCaptureFixture) -> None:
    name = "foo.bla.bar"
    log = logging.getLogger(name)

    with caplog.at_level(logging.INFO, logger=name):
        # log warning is printed
        log.warning("HELP! I need somebody!")
        assert len(caplog.records) == 1

        # when suppressed it is not printed
        with suppress_logging(name):
            log.warning("HELP! I need somebody!")

        # after the block it is printed again
        assert len(caplog.records) == 1
        log.warning("HELP! I need somebody!")
        assert len(caplog.records) == 2
