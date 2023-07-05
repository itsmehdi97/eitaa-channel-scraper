from fastapi import APIRouter, HTTPException, Depends
from starlette.responses import Response

from celery.schedules import schedule
from redbeat import RedBeatSchedulerEntry

from worker.celery import app as celery_app
from adapters import MongoChannScheduleRepository
from api.dependencies import get_repo
import schemas


router = APIRouter()


@router.post("/channels/")
async def add_channel(
    chann_schedule: schemas.ChannelSchedule,
    chann_repo: MongoChannScheduleRepository = Depends(get_repo),
) -> schemas.ChannelSchedule:
    old = chann_repo.get({"channel_id": chann_schedule.channel_id})
    if old and old.running:
        raise HTTPException(status_code=400, detail="Channel is already running")

    if not old:
        chann_repo.create(chann_schedule)
    else:
        chann_schedule = old

    chann_repo.update(chann_schedule.channel_id, running=True, error=None)

    entry = RedBeatSchedulerEntry(
        f"crawl-{chann_schedule.channel_id}",
        "worker.tasks.refresh_channel",
        schedule(chann_schedule.refresh_interval),
        app=celery_app,
        kwargs={
            "peer_channel": {
                'channel_id': chann_schedule.channel_id,
                'access_hash': chann_schedule.access_hash},
            },
    )
    entry.save()

    return chann_schedule


@router.put("/channels/{channel_id}")
async def stop_channel(
    channel_id: int,
    chann_repo: MongoChannScheduleRepository = Depends(get_repo),
) -> schemas.ChannelSchedule:
    chann_sched = chann_repo.get({"channel_id": channel_id})
    if not chann_sched:
        raise HTTPException(status_code=404, detail="Channel does not exist")

    try:
        entry = RedBeatSchedulerEntry.from_key(f"redbeat:crawl-{channel_id}", app=celery_app)
        entry.delete()
    except KeyError:
        raise HTTPException(status_code=404, detail="Channel does not exist")

    chann_repo.update(channel_id, running=False)

    return chann_sched


@router.get("/ping")
async def ping():
    return Response(status_code=204)
