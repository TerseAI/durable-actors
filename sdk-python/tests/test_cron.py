import pytest
from fixtures.effects import Effects

from durable_actors import Actor, CronEvent, cron
from durable_actors.contract import describe_actor, public_contract
from durable_actors.runtime import ActorRuntime


def test_multiple_crons_publish_method_schedules():
    class Jobs(Actor):
        @cron("*/5 * * * *", retries=3)
        @cron("0 0 * * *")
        def refresh(self, event: CronEvent) -> None:
            pass

        @cron("59 23 LW * *")
        def cleanup(self, event: CronEvent) -> None:
            pass

    contract = public_contract([Jobs])["actors"][0]
    assert contract["crons"] == [
        {"method": "cleanup", "expression": "59 23 LW * *"},
        {"method": "refresh", "expression": "*/5 * * * *", "retries": 3},
        {"method": "refresh", "expression": "0 0 * * *"},
    ]


@pytest.mark.parametrize("retries", [-1, 1.5, True, "3", 2**31])
def test_cron_rejects_invalid_retry_counts(retries):
    with pytest.raises(ValueError, match="retries"):
        cron("* * * * *", retries=retries)


@pytest.mark.parametrize("expression", ["", "* * * *", "0 0 0 * * *", 123])
def test_cron_requires_a_five_field_expression(expression):
    with pytest.raises(ValueError, match="cron"):
        cron(expression)


def test_cron_requires_one_event_parameter():
    class Bad(Actor):
        @cron("* * * * *")
        def cleanup(self) -> None:
            pass

    with pytest.raises(ValueError, match="CronEvent"):
        describe_actor(Bad)


def test_duplicate_schedule_on_same_method_is_rejected():
    with pytest.raises(ValueError, match="duplicate"):

        class Bad(Actor):
            @cron("* * * * *")
            @cron("* * * * *")
            def cleanup(self, event: CronEvent) -> None:
                pass


async def test_cron_event_reaches_the_handler_and_its_state_is_persisted():
    class Jobs(Actor):
        last_time: int = 0
        last_cron: str = ""

        @cron("* * * * *")
        def refresh(self, event: CronEvent) -> None:
            self.last_time = event.scheduled_time
            self.last_cron = event.cron

    runtime = ActorRuntime(Jobs, Effects())
    reply = await runtime.handle(
        {
            "type": "invoke",
            "request_id": "cron-1",
            "actor": {"project_id": "local", "actor_name": "Jobs", "actor_id": "one"},
            "method": "refresh",
            "args": [{"cron": "* * * * *", "scheduledTime": 60_000}],
            "state": None,
        }
    )
    assert reply["type"] == "invoked"
    assert reply["state"] == {"last_time": 60_000, "last_cron": "* * * * *"}
