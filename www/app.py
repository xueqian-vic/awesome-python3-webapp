import logging; logging.basicConfig(level=logging.INFO)
from aiohttp import web
import asyncio,time,os,json


def index(request):
    return web.Response(b'<h1>index</h1>')

@asyncio.coroutine
def init(loop):
    app = web.Application(loop=loop)
    app.router.add_router('GET','/',index)
    svr = yield from loop.create_server(app.make_handler(),'127.0.0.1',8088)
    logging.info('server started at http://127.0.0.1:9000...')
    return svr

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
