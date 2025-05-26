from celery import Celery
from celery.schedules import crontab

INCLUDE = [
    "app.insights_async",
]

celery = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
    include=INCLUDE,
)


celery.conf.update(
    result_extended=True,
    task_track_started=True,
    result_expires=0,
    timezone="Asia/Kolkata",
    celery_timezone="UTC",
    task_serializer="json",
    result_serializer="json",
    accept_content=["application/json", "application/x-python-serialize"],
)


celery.conf.beat_schedule = {
    "update-ad-insights": {
        "task": "app.insights_async.update_ad_insights",
        "schedule": crontab(minute="*/10"),
    },
}
