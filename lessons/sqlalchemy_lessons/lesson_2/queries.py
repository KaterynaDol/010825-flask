from sqlalchemy import create_engine, select, func

from lessons.sqlalchemy_lessons.lesson_2.db_connector import DBConnector
from lessons.sqlalchemy_lessons.lesson_2.social_blogs_models import *

from sqlalchemy.orm import joinedload


engine = create_engine(
    url="mysql+pymysql://ich1:ich1_password_ilovedbs@ich-edit.edu.itcareerhub.de:3306/social_blogs",
    echo=True,
    future=True
)


# Session = sessionmaker(bind=engine)
# session = Session()
# session.close()

with DBConnector(engine) as session:
    ...
    # CRUD operations
    #
    # C (Create)
    # data = {"name": "NewRole"}
    #
    # new_role = Role(**data)
    # session.add(new_role)
    # session.commit()


    # R (Read)

    # Read one
    # user = session.get(User, 11)
    #
    # print(user)
    #
    # print(user.email)
    # print(user.first_name)
    # print(user.created_at)

    # Read many
    # all_authors = (  # stmt (STATEMENT)
    #     select(User)  # SELECT * FROM `user`
    #     .where(User.role_id == 3)  # WHERE role_id = 3
    # )
    #
    # # по умолчанию вернётся [Row(User()), Row(User()), ..., Row(User())]
    # response = session.execute(all_authors).scalars()  # -> [User(), User(), ..., User()]
    #
    # data = [
    #     {
    #         "id": user.id,
    #         "name": user.first_name,
    #         "role": user.role_id
    #     }
    #     for user in response
    # ]
    #
    # print(data)

    # Получить только пользователей с рейтингом больше 5

    # v1
    # result = session.query(User).filter(User.rating > 5).all()

    # v2
    # stmt = (
    #     select(User)
    #     .where(User.rating > 5)
    # )
    #
    # res = session.execute(stmt).scalars()
    #
    # for obj in res:
    #     print(obj.email, obj.rating)

    # получить только тех юзеров, чьи фамилии начинаются на 'М'
    # stmt =(
    #     select(User)
    #     .where(User.last_name.like("M%"))
    # )
    #
    # result = session.execute(stmt).scalars()
    #
    # print(result)
    #
    # for user in result:
    #     print(user.last_name, user.role_id)

    # посмотреть пользователей с рейтингом от 2 до 5

    # stmt = (
    #     select(User)
    #     .where(User.rating.between(2, 5))
    # )
    #
    # res = session.execute(stmt).scalars()
    #
    # for row in res:
    #     print(row.rating)


    # взять только авторов с рейтингом больше 6

    # stmt = (
    #     select(User)
    #     .where(
    #         and_(User.role_id == 3, User.rating > 6)
    #     )
    # )
    #
    # res = session.execute(stmt).scalars()
    #
    # for user in res:
    #     print(user.rating, user.role_id)

    # stmt = (
    #     select(User)
    #     .where(
    #         and_(User.role_id == 3, User.rating > 6)
    #     )
    #     .order_by(desc(User.rating), User.last_name)
    # )
    #
    # res = session.execute(stmt).scalars()
    #
    # for user in res:
    #     print(user.rating, user.last_name)

    # ====================================================================

    # Aggregation && grouping

    # stmt = (
    #     select(User.role_id, func.avg(User.rating)) # SELECT AVG('user'.rating) FROM user;
    #     .group_by(User.role_id)
    # )
    #
    # result = session.execute(stmt).scalar()
    #
    # print(result)

    # for res in result:
        # print(res)


    # us = alias(selectable=User, name="us")
    # us = aliased(element=User, name="us")
    #
    # stmt = (
    #     select(
    #         us.role_id,
    #         func.count(us.id).label("count_of_users")
    #     )
    #     .group_by(us.role_id)
    # )
    #
    # result = session.execute(stmt).all()  # -> [(1, 1), (2, 4), (3, 26)]
    #
    # for group_ in result:
    #     print(f"user role: {group_.role_id}  | Count of Users: {group_.count_of_users}")

    # us = aliased(element=User, name="us")
    #
    # stmt = (
    #     select(
    #         us.role_id,
    #         func.count(us.id).label("count_of_users")
    #     )
    #     .group_by(us.role_id)
    #     .having(func.count(us.id) > 4)
    # )
    #
    # result = session.execute(stmt).all()  # -> [(3, 26)]
    #
    # for group_ in result:
    #     print(f"user role: {group_.role_id}  | Count of Users: {group_.count_of_users}")

    #
    # # Подзапрос
    # mean_rate_by_author_sbq = select(
    #     func.avg(User.rating).label("user_rating")
    # ).where(User.role_id == 3).scalar_subquery()
    #
    # # Главный запрос
    # main_query = select(User).where(User.rating > mean_rate_by_author_sbq)
    #
    # result = session.execute(main_query).scalars()
    #
    # print(result)
    #
    # for user in result:
    #     print(user.last_name, user.rating)


    # ===================================================================
    #
    # .join() -- когда нужно провести фильтрацию \ поиск данных из таблицы А на основе
    # .joinedload()
    # .subqueryload()
    # .selectinload()

    # взять пользователей в роли "author"

    # stmt =(
    #     select(User)
    #     # .join(Role)
    #     .join(Role, Role.id == User.role_id)
    #     # .join(User.role)
    #     .where(Role.name == 'author')
    # )
    #
    # results = session.execute(stmt).scalars()
    #
    # for user in results:
    #     print(user.first_name, user.role_id)


    # получить пользователей и для каждого пользователя взять его новости

    # stmt =(
    #     select(User)
    #     .outerjoin(News, Role.id == User.role_id)
    #     .options(joinedload(User.news))
    #     .where(Role.name == 'author')
    # )


    # stmt =(
    #     select(User).
    #     where(User.first_name == 'Anna')
    # )
    #
    # result = session.execute(stmt).scalars()
    #
    # print(result)
    # print(list(result))
    #
    # for i in result:
    #     print(i)
    #     print(i.first_name)

    # NOTE: Напишите запрос для вывода всех пользователей, рейтинг которых больше 6.
    # stmt =(
    #     select(User)
    #     .where(User.rating > 6)
    #     .order_by(User.rating)
    # )
    #
    # result = session.execute(stmt).scalars()
    # for user in result:
    #     print(user.rating, user.first_name)

    # stmt =(
    #     select(User).
    #     where(User.first_name == 'Anna')
    # )
    #
    # result = session.execute(stmt).scalars().first()
    # print(result)
    # if result:
    #     result.rating = 3.4
    #     session.commit()
    #     print(result.last_name, result.rating)
    #


    # stmt =(
    #     select(
    #         func.min(User.rating).label('min_rating'),
    #         func.max(User.rating).label('max_rating')
    #     )
    # )
    #
    # result = session.execute(stmt).all()
    # print(result)
    # # print(list(result))
    # print(result[0].min_rating)
    # print(result[0].max_rating)


stmt = (
    select(User.rating,
           func.count(User.id).label('user_count')
           )
    .group_by(User.rating)
)

result  = session.execute(stmt).all()
# print(result)
for user in result:
    print(user.rating, user.user_count)


