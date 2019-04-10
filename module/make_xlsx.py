from module.cache import RedisClient

redis = RedisClient()

cache_name = "{}_FULL_OHLCV"

cleannsci = redis.get_df(cache_name.format("045520"))
nano = redis.get_df(cache_name.format("187750"))
ogong = redis.get_df(cache_name.format("045060"))
km = redis.get_df(cache_name.format("083550"))
posco_ict = redis.get_df(cache_name.format("022100"))

cleannsci.to_csv("크린앤사이언스.csv")
nano.to_csv("나노.csv")
ogong.to_csv("오공.csv")
km.to_csv("케이엠.csv")
posco_ict.to_csv("포스코_ICT.csv")
