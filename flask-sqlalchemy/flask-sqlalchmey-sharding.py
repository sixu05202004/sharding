from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
app.config.from_pyfile('flask-sqlalchmey-sharding.cfg')
db = SQLAlchemy(app)


class User(object):
    '''用户分表'''
    _mapper = {}

    @staticmethod
    def model(user_id):
        table_index = user_id % 100
        # db_index = int((user_id % 100) / 25)
        class_name = 'user_%d' % table_index

        ModelClass = User._mapper.get(class_name, None)
        if ModelClass is None:
            ModelClass = type(class_name, (db.Model,), {
                '__module__': __name__,
                '__name__': class_name,
                '__tablename__': 'user_%d' % table_index,
                'id': db.Column(db.BigInteger, primary_key=True),
                'name': db.Column(db.String(100)),
                'phone': db.Column(db.String(20)),
                'status': db.Column(db.SmallInteger, default=0),
                'ctime': db.Column(db.Integer, default=0),
                'utime': db.Column(db.Integer, default=0)

            })
            User._mapper[class_name] = ModelClass

        cls = ModelClass
        return cls


@app.route('/<int:user_id>/test', methods=['GET', 'POST'])
def test(user_id):
    user = User.model(user_id)
    count = user.query.filter_by(id=user_id).count()
    item = user.query.filter(user.id == user_id, user.status == 0).first()
    # default
    name = item.name if item else "default"
    return jsonify(count=count, name=name)


if __name__ == '__main__':
    app.run()
