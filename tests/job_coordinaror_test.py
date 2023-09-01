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

from collect_coordinator.job_coordinator import JobDefinition


def test_read_job_definition() -> None:
    job_def = JobDefinition.collect_definition_json(
        {
            "job_id": "uid",
            "tenant_id": "a",
            "graphdb_server": "b",
            "graphdb_database": "c",
            "graphdb_username": "d",
            "graphdb_password": "e",
            "worker_config": {"foo": "bar"},
            "env": {"test": "test"},
            "account_len_hint": 42,
        }
    )
    assert job_def.name == "collect-single-a"
    assert job_def.args == [
        "--write",
        "resoto.worker.yaml=WORKER_CONFIG",
        "--job-id",
        "uid",
        "--tenant-id",
        "a",
        "--redis-url",
        "redis://redis-master.fix.svc.cluster.local:6379/0",
        "---",
        "--graphdb-bootstrap-do-not-secure",
        "--graphdb-server",
        "b",
        "--graphdb-database",
        "c",
        "--graphdb-username",
        "d",
        "--graphdb-password",
        "e",
        "--override-path",
        "/home/resoto/resoto.worker.yaml",
        "---",
    ]
    assert job_def.env == {"WORKER_CONFIG": '{"foo": "bar"}', "test": "test", "RESOTO_LOG_TEXT": "true"}
