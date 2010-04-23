from arx import *

ctx = ArxCtx()
ctx.sync()


import api
p = api.Arx()


b = p.admin()
b.id = "Ivar"
b.name = "Ivar"
b.title = "MySelf"
b.insert()

exit

a = p.admin()

a.select()
while a.next():
    print(a)

