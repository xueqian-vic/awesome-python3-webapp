import www.orm
import asyncio
import sys
from www.models import User, Blog, Comment

# def test(loop):
#     yield from www.orm.create_pool(user='root', password='password', database='awesome',loop=loop)
#
#     u = User(name='victoria', email='457855140@qq.com', passwd='1234567890', image='about:blank')
#
#     yield from u.save()

@asyncio.coroutine
def test(loop):
    yield from www.orm.create_pool(loop=loop, host='localhost', port=3306, user='www-data', password='www-data', db='awesome')
    u = User(name='test77',email='test77@test.com',passwd='test',image='about:blank')
    yield from u.save()

loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close()
if loop.is_closed():
    sys.exit(0)