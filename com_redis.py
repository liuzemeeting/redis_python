import datetime
import redis
import simplejson as json
from django.conf import settings
from simplejson.encoder import JSONEncoder


OUR_REDIS = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
        )

class XJSONEncoder(JSONEncoder):
    """
    JSON扩展: 支持datetime和date类型
    """
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, datetime.date):
            return o.strftime('%Y-%m-%d')
        else:
            return JSONEncoder.default(self, o)


class Struct(dict):
    """
    为字典加上点语法. 例如:
    >>> o = Struct({'a':1})
    >>> o.a
    >>> 1
    >>> o.b
    >>> None
    """

    def __init__(self, *e, **f):
        if e:
            self.update(e[0])
        if f:
            self.update(f)

    def __getattr__(self, name):
        # Pickle is trying to get state from your object, and dict doesn't implement it.
        # Your __getattr__ is being called with "__getstate__" to find that magic method,
        # and returning None instead of raising AttributeError as it should.
        if name.startswith('__'):
            raise AttributeError
        return self.get(name)

    def __setattr__(self, name, val):
        self[name] = val

    def __delattr__(self, name):
        self.pop(name, None)

    def __hash__(self):
        return id(self)


r = redis.Redis(
    host="127.0.0.1",
    port=6379,
    db=1,
    password=""
)


class CacheProxy:
    """
    主要是一些缓存模块的使用
    存储格式:
    为了通用, KEY也是字符串而且不能有空格, VALUE数据约定用JSON格式.

    内部KEY一般长这样:
    :1:user_units:897447

    内部VALUE一般长这样:
    [{"city": "410100", "name": "304\u73ed", "class_id": 4, "type": 1}]

    用法:
    from com_redis import cache

    cache.***.set(630445, 'hello')
    print ***.user.get(630445)
    """

    def __init__(self, alias, action):
        """
        :param alias: 缓存别名
        :param action: 操作缓存 get add delete update
        """
        self.r = OUR_REDIS
        self.prefix = alias
        self.timeout = 0
        self.action = action

    def get(self, key):
        # redis get 操作
        if not isinstance(key, str):
            print("TypeError: Redis get key must be str")
            return ""
        data = self.r.get(self.prefix+key)
        if not data:
            return ""
        # redis中获取出来的都是bytes类型数据，需要转换成str
        return data.decode("utf-8")

    def set(self, key, value):
        # redis set 操作
        if not isinstance(key, str):
            print("TypeError: Redis get key must be str")
            return False
        if isinstance(value, list) or isinstance(value, dict):
            value = json.dumps(value)
        data = self.r.set(self.prefix+key, value, ex=self.timeout)
        return data

    def exists(self, key):
        # redis 判断 key 是否存在
        if not isinstance(key, str):
            print("TypeError: Redis get key must be str")
            return False
        return self.r.exists(self.prefix+key)

    def delete(self, key):
        # redis 删除key
        if not isinstance(key, str):
            print("TypeError: Redis get key must be str")
            return False
        return self.r.delete(self.prefix+key)

    def mset(self, *args, **kwargs):
        # redis mset 操作（批量设置值）
        data = {}
        for d in args:
            if not isinstance(d, dict):
                print("TypeError: Redis mset key must be dict")
                return False
            data.update(d)
        data.update(kwargs)
        if not data:
            return False
        final_data = {}
        for d in data:
            final_data[self.prefix+d] = data[d]
        flag = self.r.mset(final_data)
        return flag

    def mget(self, keys):
        # redis mget 操作（批量获取值）
        data = []
        if not isinstance(keys, (list, tuple)):
            print("TypeError: Redis mget key must be list or tuple")
            return False
        for d in keys:
            data.append(self.prefix+d)
        data = self.r.mget(data)
        data = [d.decode("utf-8") for d in data]
        return data

    def incr(self, key, amount=1):
        # 自增amount个
        if not isinstance(key, str):
            print("TypeError: Redis incr key must be str")
            return False
        key = self.prefix + key
        self.r.incr(key, amount)

    def decr(self, key, amount=1):
        # 自减amount个
        if not isinstance(key, str):
            print("TypeError: Redis decr key must be str")
            return False
        key = self.prefix + key
        self.r.decr(key, amount)

    def hset(self, key, value):
        # redis hash set 操作
        if not isinstance(key, str):
            print("TypeError: Redis hset key must be str")
            return False
        return self.r.hset(self.prefix, key, value)

    def hget(self, key):
        # redis hash get 操作
        if not isinstance(key, str):
            print("TypeError: Redis hset key must be str")
            return False
        data = self.r.hget(self.prefix, key)
        return data.decode("utf-8")

    def hmset(self, *args, **kwargs):
        # redis mset 操作（批量设置值）
        data = {}
        for d in args:
            if not isinstance(d, dict):
                print("TypeError: Redis mset key must be dict")
                return False
            data.update(d)
        data.update(kwargs)
        if not data:
            return False
        flag = self.r.hmset(self.prefix, data)
        return flag

    def zadd(self, *args, **kwargs):
        # redis zadd操作(批量设置值至args有序集合中)
        if not (args or kwargs):
            return False

        self.r.zadd(self.prefix, *args, **kwargs)

    def zrem(self, name):
        # redis zrem操作(删除name有序集合中的特定元素)
        if not name:
            return False
        flag = self.r.zrem(self.prefix, name)
        return flag

    def zincrby(self, name, value, amount=1):
        # 如果在key为name的zset中已经存在元素value，则该元素的score增加amount，否则向该集合中添加该元素，其score的值为amount
        if not (name or value):
            return False
        return self.r.zincrby(self.prefix, value, amount)

    def zrevrank(self, value):
        if not value:
            return False
        return self.r.zrevrank(self.prefix, value)

    def zscore(self, member):
        if not member:
            return False
        return self.r.zscore(self.prefix, member)

    def zrange(self, start, end, withscores=False, desc=False):
        return self.decoding(self.r.zrange(self.prefix, start, end, withscores=withscores, desc=desc))

    def decoding(self, o):
        if not isinstance(o, str):
            return o
        data = json.loads(o)
        return self.decode(data)

    def decode(self, data):
        if isinstance(data, dict):
            return Struct(data)
        elif isinstance(data, (list, tuple)):
            return [self.decode(d) for d in data]
        return data

class Cache:
    def __getattr__(self, name):
        return Action(name)


class Action:
    def __init__(self, alias):
        self.alias = alias

    def __getattr__(self, item):
        instance = CacheProxy(self.alias, item)
        method = getattr(instance, item)
        return method


def check_input(func):
    def wrapper(*args, **kwargs):
        if not isinstance(args[1], int):
            raise ValueError(f"User_id must be int, and your input is {type(d)}")
        return func(*args, **kwargs)

    return wrapper


class RedisCheckIn:
    """
    主要封装用户签到的一些功能模块的使用
    """

    _private_key = "_check_in_"

    def __init__(self):
        pass

    @check_input
    def sign(self, user_id: int) -> int:
        # 用户签到
        today = str(datetime.datetime.now())[:10]
        return r.setbit(self._private_key+today, user_id, 1)

    @check_input
    def sign_status(self, user_id: int) -> int:
        # 用户今日签到状态
        today = str(datetime.datetime.now())[:10]
        return r.getbit(self._private_key+today, user_id)

    @check_input
    def week_sign_status(self, user_id: int) -> list:
        # 求出这个周的签到状况
        now = datetime.datetime.now()
        # 周一是1 周日是7 now.weekday()则是周一是0，周日是6
        weekday = now.isoweekday()
        with r.pipeline(transaction=False) as p:
            for d in range(weekday):
                check_day = str(now-datetime.timedelta(days=1)*d)[:10]
                p.getbit(self._private_key + check_day, user_id)
            data = p.execute()[::-1]
        return data

    @check_input
    def month_sing_status(self, user_id: int) -> list:
        # 求出这个月的签到状况
        now = datetime.datetime.now()
        # 周一是1 周日是7 now.weekday()则是周一是0，周日是6
        day = now.day
        with r.pipeline(transaction=False) as p:
            for d in range(day):
                check_day = str(now - datetime.timedelta(days=1) * d)[:10]
                p.getbit(self._private_key + check_day, user_id)
            data = p.execute()[::-1]
        return data

    @check_input
    def week_sign_num(self, user_id: int) -> int:
        # 求出这个周的签到次数
        return sum(self.week_sign_status(user_id))

    @check_input
    def month_sign_num(self, user_id: int) -> int:
        # 求出这个月的签到次数
        return sum(self.month_sing_status(user_id))


redis_sign_in = RedisCheckIn()


cache = Cache()


if __name__ == "__main__":
    redis_key = "rankstar"
    rank_star_redis = cache.__getattr__(redis_key)
    data = rank_star_redis.zrange(0, -1, withscores=True)
    print(redis_sign_in.week_sign_status(1))
