from fastapi import APIRouter, HTTPException, Depends
from starlette.responses import Response

from celery.schedules import schedule
from redbeat import RedBeatSchedulerEntry

from worker import tasks
from worker.celery import app as celery_app
from adapters import MongoRepository
from api.dependencies import get_repo
import schemas




router = APIRouter()


@router.post('/channels/')
async def add_channel(
    chann_schedule: schemas.ChannelSchedule,
    repo: MongoRepository = Depends(get_repo)
) -> str:
    if repo.get_channel(chann_schedule.channel_name):
        raise HTTPException(
            status_code=400, detail="Channel already exists")

    interval = schedule(45)
    entry = RedBeatSchedulerEntry(
        f'crawl-{chann_schedule.channel_name}',
        'worker.tasks.refresh_channel',
        interval, 
        app=celery_app,
        kwargs={"channel_name": chann_schedule.channel_name,})
    entry.save()

    return "res.task_id"


@router.get("/ping")
async def ping():
    return Response(status_code=204)
