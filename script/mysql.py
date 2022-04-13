from sqlalchemy import create_engine

class MySQL:
  def __init__(self, config):
    self.host = config['host']
    self.port = config['port']
    self.username = config['username']
    self.password = config['password']
    self.database = config['database']

  def connect(self):
    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
    engine_conn = engine.connect()
    print("Connect Engine MySQL")
    return engine, engine_conn