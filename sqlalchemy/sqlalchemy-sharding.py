from sqlalchemy import Column, Integer, String, BigInteger, SmallInteger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class UserData(object):
    _mapper = {}

    @staticmethod
    def model(user_id):
        table_index = user_id % 100
        class_name = 'user_%d' % table_index
        ModelClass = UserData._mapper.get(class_name, None)
        if ModelClass is None:
            ModelClass = type(class_name, (Base,), dict(
                __module__=__name__,
                __name__=class_name,
                __tablename__='user_%d' % table_index,
                id=Column(BigInteger, primary_key=True),
                name=Column(String(255)),
                picture=Column(String(255)),
                valid=Column(SmallInteger),
                utime=Column(Integer),
                ctime=Column(Integer)

            ))
            UserData._mapper[class_name] = ModelClass
        cls = ModelClass
        return cls


if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' % ('root',
                                                               'root',
                                                               '127.0.0.1',
                                                               '3306',
                                                               'test'), encoding='utf8')
    Base.metadata.create_all(engine)

    session = Session(engine)

    # user_id = 100001
    user_id = 1000001
    user = UserData.model(user_id)
    result_1 = session.query(user).count()
    result_2 = session.query(user).filter(user.name == "test", user.valid == 0).all()
    result_3 = session.query(user).filter_by(name="test", valid=0).all()
    print(result_1, result_2, result_3)
