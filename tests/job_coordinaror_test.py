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
    assert job_def.env == {"WORKER_CONFIG": '{"foo": "bar"}', "test": "test"}
