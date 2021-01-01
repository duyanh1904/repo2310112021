import datetime
from functools import partial
from peewee import *

db = MySQLDatabase('TodoList', user='root', passwd='test')

class BaseModel(Model):
    class Meta:
        database = db

class Member(BaseModel):
    memid = AutoField()  # Auto-incrementing primary key.
    surname = CharField()
    firstname = CharField()
    address = CharField(max_length=300)
    zipcode = IntegerField()
    telephone = CharField()
    recommendedby = ForeignKeyField('self', backref='recommended',
                                    column_name='recommendedby', null=True)
    joindate = DateTimeField()

    class Meta:
        table_name = 'members'

class Post(BaseModel):
    content = TextField()
    flags = BitField()

    is_favorite = flags.flag(1)
    is_sticky = flags.flag(2)
    is_minimized = flags.flag(4)
    is_deleted = flags.flag(8)

    class Meta:
        table_name = 'post_table'



# Conveniently declare decimal fields suitable for storing currency.
MoneyField = partial(DecimalField, decimal_places=2)


class Facility(BaseModel):
    facid = AutoField()
    name = CharField()
    membercost = MoneyField()
    guestcost = MoneyField()
    initialoutlay = MoneyField()
    monthlymaintenance = MoneyField()

    class Meta:
        table_name = 'facilities'


class Booking(BaseModel):
    bookid = AutoField()
    facility = ForeignKeyField(Facility, column_name='facid')
    member = ForeignKeyField(Member, column_name='memid')
    starttime = DateTimeField()
    slots = IntegerField()

    class Meta:
        table_name = 'bookings'

db.create_tables([Member, Facility, Booking, Post])

# query = Facility.select()
# print(query)

# query = Facility.select(Facility.name, Facility.membercost)

# To iterate:


# query = Facility.select().where(Facility.facid.in_([1, 10]))
# for facility in query:
#     print(facility.membercost)

# res = (Facility
#        .insert(facid=19, name='Spa', membercost=20, guestcost=30,
#                initialoutlay=100000, monthlymaintenance=800)
#        .execute())

# query = (Facility
#          .select(Facility.facid, Facility.name, Facility.membercost,
#                  Facility.monthlymaintenance)
#          .where(
#              (Facility.membercost > 0) &
#              (Facility.membercost < (Facility.monthlymaintenance / 50))))

# query = Facility.select().where(Facility.name ** '%duynghia%')
# for score in query:
#     print(score.initialoutlay)
# res = Member.insert({
#     Member.surname: 'basic'}).execute()

information = input(">>")
information = information.split(",")
# res = Member.insert({
#     Member.surname: information[0],
#     Member.firstname: information[1],
#     Member.address: information[2],
#     Member.zipcode: information[3],
#     Member.telephone: information[4],
#     Member.joindate: information[5],
#     Member.recommendedby: information[6]}).execute()

# res = Booking.insert({
#         Booking.starttime: information[0],
#         Booking.slots: information[1]}).execute()

# maxq = Facility.select(fn.MAX(Facility.facid) + 1)
# subq = Select(columns=(maxq, 'Spa', 30, 50, 100000, 800))
# res = Facility.insert_from(subq, Facility._meta.sorted_fields).execute()
#
# res = Facility.insert({
#     Facility.name: information[0],
#     Facility.membercost: information[1],
#     Facility.guestcost: information[2],
#     Facility.initialoutlay: information[3],
#     Facility.monthlymaintenance: information[4]}).execute()

rank = fn.rank().over(order_by=[fn.SUM(Booking.slots).desc()])

subq = (Booking
        .select(Booking.facility, fn.SUM(Booking.slots).alias('total'),
                rank.alias('rank'))
        .group_by(Booking.facility))

# Here we use a plain Select() to create our query.
query = (Select(columns=[subq.c.facid, subq.c.total])
         .from_(subq)
         .where(subq.c.rank == 1)
         .bind(db).tuples()) # We must bind() it to the database.

# To iterate over the query results:
for facid, total in query.tuples():
    print(facid, total)



