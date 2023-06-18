from fastapi import FastAPI

from api.routes import router as api_router
from core import tasks


tags_metadata = []


def get_application():
    app = FastAPI()

    app.include_router(api_router, prefix="/api")
    app.add_event_handler("startup", tasks.create_start_app_handler(app))

    return app


app = get_application()
